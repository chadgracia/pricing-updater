"""
pricing-updater Lambda  (Function URL, "Market Price Updater" paste form)
=======================================================================

Serves a single URL:
    GET  /  -> HTML form with a textarea
    POST /  -> parse paste, match to CRM, update Pipeline, return HTML report

Input formats — both can appear in the same paste:

    1. Access code (REQUIRED, first non-empty line):
           1623

    2. Hiive dashboard dump (markdown-link names from Hiive copy-paste):
           [Ripple Labs](https://app.hiive.com/companies/<uuid>)
           Highest Bid
           $120.00
           Lowest Ask
           $112.50
           ...

    3. One-line overrides (any format-matching line):
           zipline bid $100
           Anthropic ask $1400

       One-liners override Hiive values for the same company in the same
       paste. They only touch the named side; the unspecified side is left
       alone (not cleared).

Environment variables:
    DRY_RUN     'true' (default) | 'false'   when true, no Pipeline writes

Pipeline fields written (company custom fields):
    custom_label_3997297  Hiive Ask          currency
    custom_label_3997298  Hiive Bid          currency
    custom_label_3997299  Hiive Ask Date     YYYY-MM-DD
    custom_label_3997300  Hiive Bid Date     YYYY-MM-DD
    custom_label_3999575  Hiive Price        currency
    custom_label_3999576  Hiive Price Date   YYYY-MM-DD

Field-touch semantics (per record):
    set_bid / set_ask in {float, "CLEAR", None}
        float  -> write that value + stamp date
        "CLEAR"-> Hiive showed —. Skip writing — keep CRM's prior value AND prior date.
                  (We don't overwrite known-good data with 0 just because Hiive has no
                  current bid/ask on that side; the CRM's old value is still the most
                  recent data point we have.)
        None   -> don't touch this side at all (one-liner didn't specify it).
"""

import os
import re
import json
import html
import logging
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timezone

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Config ----------------------------------------------------------------

DRY_RUN   = os.environ.get("DRY_RUN", "true").lower() == "true"
BUCKET    = "full-pipeline-cache"
AUTH_CODE = "1623"

PIPELINE_BASE = "https://api.pipelinecrm.com/api/v3"

FIELD_HIIVE_ASK      = "custom_label_3997297"
FIELD_HIIVE_BID      = "custom_label_3997298"
FIELD_HIIVE_ASK_DATE = "custom_label_3997299"
FIELD_HIIVE_BID_DATE = "custom_label_3997300"
FIELD_HIIVE_PRICE      = "custom_label_3999575"
FIELD_HIIVE_PRICE_DATE = "custom_label_3999576"

# High Priority multi_select (company). This tool flags a company when Hiive shows
# a hot book, and never touches a record a human has marked Hold.
FIELD_HIGH_PRIORITY = "custom_label_4002734"
HP_SPV            = 7190470   # "Source SPV Seller"
HP_DIRECT         = 7190471   # "Source Direct Seller"
HP_HOLD           = 7190472   # "Hold" — when present, this tool skips the record entirely
HP_BIDS_THRESHOLD = 10        # Hiive bid count at/above which a company is flagged hot

ISSUER_ORG_TYPE_IDS = {5103523, 6677589}  # Unicorn, Private Company
MIN_UNMATCHED_BIDS = 3  # show unmatched only when Hiive shows real demand

NAME_SUFFIXES = (
    " Systems", " Technologies", ...
)
NAME_SUFFIXES = (
    " Systems", " Technologies", " Technology", " Labs", " Lab",
    " Inc.", " Inc", " Corp.", " Corp", " Corporation",
    " Software", " AI", " Network", " Networks",
)

# --- Request/response helpers ----------------------------------------------

def _decode_body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        import base64
        raw = base64.b64decode(raw).decode("utf-8", errors="replace")
    return urllib.parse.parse_qs(raw, keep_blank_values=True)

MOVER_THRESHOLD_PCT = 2.5

def _max_move_pct(rec, crm):
    """Largest absolute % move across bid and ask vs prior CRM values.
    None when no % move is computable (side untouched, CLEAR, or no prior)."""
    moves = []
    for action, prior in ((rec["set_bid"], crm["prior_bid"]),
                          (rec["set_ask"], crm["prior_ask"])):
        if isinstance(action, (int, float)) and isinstance(prior, (int, float)) and prior > 0:
            moves.append(abs(action - prior) / prior * 100.0)
    return max(moves) if moves else None

def html_response(status, body):
    return {
        "statusCode": status,
        "headers":    {"Content-Type": "text/html; charset=utf-8"},
        "body":       body,
    }

# --- Auth ------------------------------------------------------------------

def check_auth_and_strip(text):
    """
    First non-empty line of the paste must equal AUTH_CODE.
    Returns (text_without_auth_line, ok).
    """
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    lines = text.split("\n")
    for i, line in enumerate(lines):
        s = line.strip()
        if not s:
            continue
        if s == AUTH_CODE:
            return "\n".join(lines[i + 1:]), True
        return text, False
    return "", False

# --- Pipeline JWT ----------------------------------------------------------

def get_jwt(s3):
    obj = s3.get_object(Bucket="pipeline-token", Key="pipeline-jwt.json")
    return json.loads(obj["Body"].read())["jwt"]

# --- CRM snapshot ----------------------------------------------------------

def _safe_float(v):
    try:
        return float(v) if v not in (None, "", 0, "0") else None
    except (TypeError, ValueError):
        return None

def load_crm_issuers(s3):
    """
    Read companies.json from S3, keep companies whose Org. Type contains
    a Unicorn or Private Company dropdown ID.

    Snapshot shape (verified against live API):
        company['custom_fields']['custom_label_625142']  ->  list[int]   e.g. [5103523]
        company['custom_fields']['custom_label_3997298'] ->  str | None  e.g. "7.0"
        company['custom_fields']['custom_label_3997297'] ->  str | None
    """
    obj = s3.get_object(Bucket=BUCKET, Key="companies.json")
    snap = json.loads(obj["Body"].read())
    companies = snap.get("companies", [])
    out = []
    for c in companies:
        custom = c.get("custom_fields") or {}
        org_ids = custom.get("custom_label_625142") or []
        if not any(oid in ISSUER_ORG_TYPE_IDS for oid in org_ids):
            continue
        out.append({
            "id":        c["id"],
            "name":      c["name"],
            "org_ids":   list(org_ids),
            "prior_bid": _safe_float(custom.get(FIELD_HIIVE_BID)),
            "prior_ask": _safe_float(custom.get(FIELD_HIIVE_ASK)),
            "prior_price": _safe_float(custom.get(FIELD_HIIVE_PRICE)),
        })
    logger.info(f"companies.json has {len(companies)} records; {len(out)} match issuer filter")
    return out

# --- Hiive dump parser -----------------------------------------------------

MD_LINK = re.compile(r"\[([^\]]+)\]\(([^)]+)\)")

def _empty_rec(name, uuid=None, source="hiive_dump"):
    return {
        "name":       name,
        "hiive_uuid": uuid,
        "set_bid":    None,
        "set_ask":    None,
        "set_price":  None,
        "listings":   0,
        "bids":       0,
        "source":     source,
    }

def parse_hiive_blocks(raw):
    """Returns list of records from the markdown-link Hiive dashboard format."""
    uuid_by_name = {}
    def grab_uuid(m):
        name = m.group(1).strip()
        u = re.search(r"/companies/([a-f0-9-]{16,})", m.group(2))
        if u and name not in uuid_by_name:
            uuid_by_name[name] = u.group(1)
        return name

    text = MD_LINK.sub(grab_uuid, raw)
    lines = [l.strip() for l in text.split("\n")]

    def to_price(s):
        if not s:
            return None
        if s == "—":
            return "CLEAR"
        m = re.search(r"\$?([\d,]+\.?\d*)", s)
        return float(m.group(1).replace(",", "")) if m else None

    def to_int(s):
        m = re.search(r"(\d+)", s or "")
        return int(m.group(1)) if m else 0

    out = []
    i = 0
    while i < len(lines):
        if lines[i] != "Highest Bid":
            i += 1
            continue
        n = i - 1
        while n >= 0 and not lines[n]:
            n -= 1
        if n < 0:
            i += 1
            continue
        rec = _empty_rec(lines[n], uuid_by_name.get(lines[n]))
        j = i
        while j < len(lines):
            line = lines[j]
            if j > i and (line == "Highest Bid" or line == "Browse Companies | Hiive"):
                break
            if line == "Highest Bid":
                rec["set_bid"] = to_price(lines[j + 1] if j + 1 < len(lines) else "")
            elif line == "Lowest Ask":
                rec["set_ask"] = to_price(lines[j + 1] if j + 1 < len(lines) else "")
            elif line == "Hiive Price":
                rec["set_price"] = to_price(lines[j + 1] if j + 1 < len(lines) else "")
            elif line == "Market Activity":
                # Hiive paste inserts blank lines between stat rows; skip them,
                # but stop on the first non-blank line that isn't a Listings/Bids row.
                k = j + 1
                while k < len(lines):
                    cell = lines[k]
                    if not cell:
                        k += 1
                        continue
                    if "Listing" in cell:
                        rec["listings"] = to_int(cell)
                    elif "Bid" in cell:
                        rec["bids"] = to_int(cell)
                    else:
                        break
                    k += 1
            j += 1
        out.append(rec)
        i = j
    logger.info(f"PARSER_V2 parsed {len(out)} blocks: " + ", ".join(f"{r['name']}={r['bids']}" for r in out))
    return out
# --- One-liner parser ------------------------------------------------------

ONELINER = re.compile(
    r"^[ \t]*([A-Za-z0-9][\w .()&/'\-]{0,80}?)[ \t]+(bid|ask)[ \t]+\$?([\d,]+\.?\d*)[ \t]*$",
    re.IGNORECASE | re.MULTILINE,
)

_HIIVE_LABEL_FRAGMENTS = {"highest", "lowest"}

def parse_one_liners(text):
    out = []
    for m in ONELINER.finditer(text):
        name = re.sub(r"\s+", " ", m.group(1).strip())
        if name.lower() in _HIIVE_LABEL_FRAGMENTS:
            continue
        side = m.group(2).lower()
        price = float(m.group(3).replace(",", ""))
        rec = _empty_rec(name, source="oneliner")
        if side == "bid":
            rec["set_bid"] = price
        else:
            rec["set_ask"] = price
        out.append(rec)
    logger.info(f"Parsed {len(out)} one-liners")
    return out

# --- Matching --------------------------------------------------------------

def _strip_dollar(s): return s.rstrip("$").strip()
def _strip_parens(s): return re.sub(r"\s*\([^)]*\)\s*", " ", s).strip()
def _strip_suffix(s):
    for _ in range(5):
        for suf in NAME_SUFFIXES:
            if s.lower().endswith(suf.lower()) and len(s) > len(suf) + 1:
                s = s[: -len(suf)].strip()
                break
        else:
            break
    return s

def _name_keys(name):
    n = name.strip()
    L0 = n.lower()
    L1 = _strip_dollar(n).lower()
    L2 = _strip_parens(_strip_dollar(n)).lower()
    L3 = _strip_suffix(_strip_parens(_strip_dollar(n))).lower()
    L4 = re.sub(r"[^a-z0-9]", "", L3)
    return [L0, L1, L2, L3, L4]

def build_match_index(crm_companies):
    indices = [{} for _ in range(5)]
    for c in crm_companies:
        for i, k in enumerate(_name_keys(c["name"])):
            indices[i].setdefault(k, []).append(c)
    return indices

def match_recs_to_crm(recs, indices):
    matches, unmatched, ambiguous = [], [], []
    for h in recs:
        picked = None
        for lvl, key in enumerate(_name_keys(h["name"])):
            cands = indices[lvl].get(key)
            if not cands:
                continue
            if len(cands) == 1:
                picked = (cands[0], lvl)
                break
            exact = [c for c in cands if c["name"].lower() == h["name"].lower()]
            if exact:
                picked = (exact[0], lvl)
                break
            ambiguous.append((h, cands, lvl))
            picked = (min(cands, key=lambda c: len(c["name"])), lvl)
            break
        if picked:
            matches.append((h, picked[0], picked[1]))
        else:
            unmatched.append(h)
    return matches, unmatched, ambiguous

# --- Merge: dedupe by CRM ID, one-liners override Hiive --------------------

def merge_by_crm(matches):
    """
    Collapse matches sharing a CRM ID into a single update.
    One-liner records override Hiive records for the side they touch.
    Hiive records fill in any side a one-liner didn't set.
    """
    by_id = {}
    for h_rec, crm_co, lvl in matches:
        cid = crm_co["id"]
        if cid not in by_id:
            by_id[cid] = {"rec": dict(h_rec), "crm": crm_co, "level": lvl}
            continue
        merged = by_id[cid]["rec"]
        if h_rec["source"] == "oneliner":
            if h_rec["set_bid"] is not None:
                merged["set_bid"] = h_rec["set_bid"]
            if h_rec["set_ask"] is not None:
                merged["set_ask"] = h_rec["set_ask"]
        else:
            if merged["set_bid"] is None:
                merged["set_bid"] = h_rec["set_bid"]
            if merged["set_ask"] is None:
                merged["set_ask"] = h_rec["set_ask"]
            if merged.get("set_price") is None:
                merged["set_price"] = h_rec.get("set_price")
            if h_rec["listings"] > merged.get("listings", 0):
                merged["listings"] = h_rec["listings"]
            if h_rec["bids"] > merged.get("bids", 0):
                merged["bids"] = h_rec["bids"]
            if h_rec["hiive_uuid"] and not merged.get("hiive_uuid"):
                merged["hiive_uuid"] = h_rec["hiive_uuid"]
    return [(d["rec"], d["crm"], d["level"]) for d in by_id.values()]

# --- Pipeline writes -------------------------------------------------------

def get_high_priority(jwt, company_id):
    """Live read of a company's High Priority multi_select entry IDs. Returns a list."""
    req = urllib.request.Request(
        f"{PIPELINE_BASE}/companies/{company_id}.json",
        method="GET",
        headers={"Authorization": f"Bearer {jwt}"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        data = json.loads(r.read())
    custom = data.get("custom_fields") or {}
    return list(custom.get(FIELD_HIGH_PRIORITY) or [])


def update_company(jwt, company_id, rec, date_str, dry_run):
    """
    Only write a side when set_bid/set_ask is an actual number.
    "CLEAR" (Hiive showed —) and None (one-liner skipped) both result in no write
    for that side — the existing CRM value and date are preserved.
    """
    payload = {}
    if isinstance(rec["set_bid"], (int, float)):
        payload[FIELD_HIIVE_BID]      = rec["set_bid"]
        payload[FIELD_HIIVE_BID_DATE] = date_str
    if isinstance(rec["set_ask"], (int, float)):
        payload[FIELD_HIIVE_ASK]      = rec["set_ask"]
        payload[FIELD_HIIVE_ASK_DATE] = date_str
    if isinstance(rec.get("set_price"), (int, float)):
        payload[FIELD_HIIVE_PRICE]      = rec["set_price"]
        payload[FIELD_HIIVE_PRICE_DATE] = date_str

    flag_hot = (rec.get("bids", 0) or 0) >= HP_BIDS_THRESHOLD

    # Nothing to write and nothing to flag -> no read needed.
    if not payload and not flag_hot:
        return True, "nothing to write"

    # Read current High Priority live so we can honor Hold and merge the
    # Source checkboxes without clobbering an existing selection.
    try:
        current_hp = get_high_priority(jwt, company_id)
    except Exception as e:
        return False, f"high-priority read failed: {type(e).__name__}: {e}"
    if HP_HOLD in current_hp:
        return True, "held — not touched"

    status_note = None
    if flag_hot:
        merged_hp = sorted(set(current_hp) | {HP_SPV, HP_DIRECT})
        if merged_hp != sorted(current_hp):
            payload[FIELD_HIGH_PRIORITY] = merged_hp
            status_note = "flagged SPV+Direct"

    if not payload:
        return True, "nothing to write"
    if dry_run:
        return True, status_note or "dry-run ok"
    body = json.dumps({"company": {"custom_fields": payload}}).encode("utf-8")
    req = urllib.request.Request(
        f"{PIPELINE_BASE}/companies/{company_id}.json",
        data=body, method="PUT",
        headers={"Authorization": f"Bearer {jwt}",
                 "Content-Type":  "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            r.read()
        return True, None
    except urllib.error.HTTPError as e:
        return False, f"HTTP {e.code}: {e.read().decode('utf-8', errors='replace')[:200]}"
    except Exception as e:
        return False, f"{type(e).__name__}: {e}"

# --- HTML rendering --------------------------------------------------------

CSS = """
* { box-sizing: border-box; }
body { font: 14px/1.45 -apple-system, system-ui, sans-serif; max-width: 1100px;
       margin: 24px auto; padding: 0 16px; color: #1c1c1c; }
h1 { margin: 0 0 8px; font-size: 22px; }
h2 { margin: 28px 0 8px; font-size: 16px; border-bottom: 1px solid #ddd; padding-bottom: 4px; }
.muted { color: #666; font-size: 13px; }
.banner { padding: 8px 12px; border-radius: 6px; margin: 12px 0; font-weight: 600; }
.banner.dry { background: #fff3cd; color: #5a4a00; }
.banner.live { background: #d1f7d6; color: #035c1a; }
.banner.err { background: #fde2e2; color: #6b0c0c; }
textarea { width: 100%; height: 480px; font: 12px/1.4 ui-monospace, "SF Mono", Menlo, monospace;
           padding: 10px; border: 1px solid #ccc; border-radius: 6px; }
button { background: #1a1a1a; color: white; border: 0; padding: 10px 24px;
         border-radius: 6px; cursor: pointer; font-size: 14px; margin-top: 8px; }
button:hover { background: #333; }
a.run-btn { display: inline-flex; align-items: center; background: #1a1a1a; color: white;
            text-decoration: none; padding: 0 20px; border-radius: 6px; font-size: 14px;
            font-weight: 600; white-space: nowrap; }
a.run-btn:hover { background: #333; }
.stats { display: flex; gap: 12px; margin: 12px 0 20px; flex-wrap: wrap; align-items: stretch; }
.stat { background: #f5f5f5; padding: 8px 14px; border-radius: 6px; min-width: 100px; }
.stat .n { font-size: 22px; font-weight: 700; display: block; }
.stat .l { font-size: 12px; color: #666; text-transform: uppercase; letter-spacing: 0.04em; }
table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 8px; }
th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #eee; }
th { background: #f9f9f9; font-weight: 600; }
td.num { text-align: right; font-variant-numeric: tabular-nums; }
.up { color: #035c1a; }
.dn { color: #b00020; }
.eq { color: #999; }
.untouched { color: #bbb; font-style: italic; }
.ok { color: #035c1a; }
.fail { color: #b00020; }
a.back { display: inline-block; margin-top: 24px; }
code { background: #f1f1f1; padding: 1px 5px; border-radius: 3px; font-size: 12px; }
"""

def render_form():
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Market Price Updater</title><style>{CSS}</style></head>
<body>
  <h1>Market Price Updater</h1>
  <p class="muted">First line must be your access code. Below that, paste a data dump
     OR one-liners like <code>zipline bid $100</code> (not both in the same paste).</p>
  <form method="POST" action="">
    <textarea name="hiive_text" autofocus required></textarea>
    <br><button type="submit">Update CRM</button>
  </form>
</body></html>"""

def _fmt_price(v):
    return f"${v:,.2f}" if v is not None else "—"

def _delta_cell(prior, action):
    if action is None:
        # One-liner didn't touch this side
        if prior is None:
            return '<td class="num untouched">—</td>'
        return f'<td class="num untouched">{_fmt_price(prior)} (untouched)</td>'
    if action == "CLEAR":
        # Hiive showed — ; policy is to keep prior CRM value + prior date (no write)
        if prior is None or prior == 0:
            return '<td class="num untouched">— (no Hiive data)</td>'
        return f'<td class="num untouched">{_fmt_price(prior)} (kept; — on Hiive)</td>'
    if prior is None:
        return f'<td class="num up">+ {_fmt_price(action)}</td>'
    if abs(prior - action) < 0.005:
        return f'<td class="num eq">{_fmt_price(action)}</td>'
    cls = "up" if action > prior else "dn"
    arrow = "↑" if action > prior else "↓"
    return f'<td class="num {cls}">{_fmt_price(prior)} → {_fmt_price(action)} {arrow}</td>'

def render_results(updates, results, unmatched, ambiguous, skipped_empty,
                    parsed_hiive, parsed_oneliners, dry_run):
    ok_n   = sum(1 for ok, _ in results if ok)
    fail_n = sum(1 for ok, _ in results if not ok)

    banner = ('<div class="banner dry">DRY RUN — no Pipeline writes were made. Showing what WOULD have changed.</div>'
              if dry_run else
              '<div class="banner live">LIVE — Pipeline records updated.</div>')

    # Sort: big bid/ask movers (>= MOVER_THRESHOLD_PCT %) first, largest move
    # first. Hiive Price is deliberately ignored -- the composite drifts a few
    # cents daily, which made the old changed-first sort useless.
    pairs = list(zip(updates, results))
    def _sort_key(p):
        rec, crm = p[0][0], p[0][1]
        pct = _max_move_pct(rec, crm)
        if pct is not None and pct >= MOVER_THRESHOLD_PCT:
            return (0, -pct, crm["name"].lower())
        return (1, -(rec.get("bids", 0) or 0), crm["name"].lower())
    pairs.sort(key=_sort_key)

    rows = []
    for (rec, crm, _lvl), (ok, err) in pairs:
        rows.append(
            "<tr>"
            f'<td>{html.escape(crm["name"])}</td>'
            f"{_delta_cell(crm['prior_bid'], rec['set_bid'])}"
            f"{_delta_cell(crm['prior_ask'], rec['set_ask'])}"
            f"{_delta_cell(crm.get('prior_price'), rec.get('set_price'))}"
            f'<td class="num">{rec.get("bids", 0)}</td>'
            f'<td class="num">{crm["id"]}</td>'
            f'<td class="{"ok" if ok else "fail"}">'
            f'{(html.escape(err) if err else "ok") if ok else html.escape(err or "fail")}</td>'
            "</tr>"
        )
    updates_table = (
        "<table><thead><tr><th>Company</th><th>Bid</th><th>Ask</th><th>Hiive Price</th>"
        "<th># Bids</th><th>CRM ID</th><th>Status</th></tr></thead><tbody>"
        + "".join(rows) + "</tbody></table>"
    ) if updates else '<p class="muted">No matches.</p>'

    skipped_block = ""
    if skipped_empty:
        skipped_block = (
            "<h2>Skipped (no data parsed — likely truncated paste)</h2><ul>" +
            "".join(f"<li>{html.escape(s)}</li>" for s in skipped_empty) +
            "</ul>"
        )

    unm_filtered = [h for h in unmatched if h.get("bids", 0) >= MIN_UNMATCHED_BIDS]
    unm_sorted = sorted(unm_filtered, key=lambda h: -(h["listings"] + h["bids"]))
    unm_block = ""
    if unm_sorted:
        rows_u = []
        for h in unm_sorted[:25]:
            bid_disp = _fmt_price(h['set_bid']) if isinstance(h['set_bid'], (int, float)) else "—"
            ask_disp = _fmt_price(h['set_ask']) if isinstance(h['set_ask'], (int, float)) else "—"
            rows_u.append(
                "<tr>"
                f"<td>{html.escape(h['name'])}</td>"
                f"<td class='num'>{bid_disp}</td>"
                f"<td class='num'>{ask_disp}</td>"
                f"<td class='num'>{h['listings']}</td>"
                f"<td class='num'>{h['bids']}</td>"
                f"<td class='muted'>{h['source']}</td>"
                "</tr>"
            )
        unm_block = (
            f"<h2>Top unmatched with {MIN_UNMATCHED_BIDS}+ bids (candidates for new CRM records)</h2>"
            "<table><thead><tr><th>Name</th><th>Bid</th><th>Ask</th>"
            "<th>Listings</th><th>Bids</th><th>Source</th></tr></thead><tbody>"
            + "".join(rows_u) + "</tbody></table>"
        )

    amb_block = ""
    if ambiguous:
        amb_block = "<h2>Ambiguous matches (review)</h2><ul>"
        for h, cands, lvl in ambiguous:
            cand_str = ", ".join(html.escape(c["name"]) for c in cands)
            amb_block += f"<li><b>{html.escape(h['name'])}</b> (level {lvl}) → picked first of: {cand_str}</li>"
        amb_block += "</ul>"

    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Market Price Updater · Results</title><style>{CSS}</style></head>
<body>
  <h1>Market Price Updater — Results</h1>
  {banner}
  <div class="stats">
    <div class="stat"><span class="n">{parsed_hiive}</span><span class="l">Hiive blocks</span></div>
    <div class="stat"><span class="n">{parsed_oneliners}</span><span class="l">one-liners</span></div>
    <div class="stat"><span class="n">{len(updates)}</span><span class="l">to update</span></div>
    <div class="stat"><span class="n">{ok_n}</span><span class="l">{'would-ok' if dry_run else 'updated ok'}</span></div>
    <div class="stat"><span class="n">{fail_n}</span><span class="l">failed</span></div>
    <div class="stat"><span class="n">{len(unmatched)}</span><span class="l">unmatched</span></div>
    <div class="stat"><span class="n">{len(skipped_empty)}</span><span class="l">skipped</span></div>
    <a class="run-btn" href="">Run another</a>
  </div>

  <h2>Updates</h2>
  {updates_table}

  {skipped_block}
  {amb_block}
  {unm_block}
</body></html>"""

def render_error(msg):
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><style>{CSS}</style></head>
<body>
  <h1>Market Price Updater</h1>
  <div class="banner err">{html.escape(msg)}</div>
  <a class="back" href="">← Back</a>
</body></html>"""

# --- Pitchbook (investor-list) paste mode ----------------------------------
# A third paste format: a markdown pipe table of investors/firms whose header
# contains Name, Company, Investment. This phase parses + classifies rows,
# matches firms against the full companies.json snapshot, and resolves the
# security's holding entry ID live in both the person and company holding
# fields. It performs NO writes and does not load people.json. The Hiive and
# one-liner paths are untouched.

FIELD_PERSON_HOLDING     = "custom_label_3740611"   # person HOLDING (write key)
FIELD_COMPANY_HOLDING    = "custom_label_3746654"   # company HOLDING (write key)
PERSON_HOLDING_LABEL_ID  = 3740611                  # numeric id for /admin labels lookup
COMPANY_HOLDING_LABEL_ID = 3746654

PITCHBOOK_BLANKS = {"", "-", "—", "–", "n/a", "na", "none"}

LEGAL_SUFFIX_RE = re.compile(
    r"[,\s]+(inc|incorporated|corp|corporation|co|llc|l\.l\.c|ltd|limited|"
    r"lp|l\.p|llp|plc|sa|s\.a|sas|s\.a\.s|gmbh|ag|nv|bv|pte|pty)\.?\s*$",
    re.IGNORECASE)


def strip_legal_suffix(name):
    prev, out = None, (name or "").strip()
    while out != prev:
        prev = out
        out = LEGAL_SUFFIX_RE.sub("", out).strip().rstrip(",").strip()
    return out


def _norm_security(name):
    """Normalize a security/entry name: drop trailing $/# marker + whitespace."""
    n = re.sub(r"[\s$#]+$", "", (name or "").strip())
    return n.strip().lower()


def is_pitchbook_table(text):
    for ln in text.splitlines():
        if ln.count("|") >= 3:
            low = ln.lower()
            if "name" in low and "company" in low and "investment" in low:
                return True
    return False


def _pb_clean(cell):
    c = (cell or "").strip()
    return "" if c.lower() in PITCHBOOK_BLANKS else c


def parse_pitchbook_table(text):
    """Return (rows, security_raw). Rows carry name/title/company/email/kind."""
    lines = [ln for ln in text.splitlines() if ln.count("|") >= 2]
    header_idx = None
    for i, ln in enumerate(lines):
        low = ln.lower()
        if "name" in low and "company" in low and "investment" in low:
            header_idx = i
            break
    if header_idx is None:
        return [], ""

    def cells(ln):
        parts = [p.strip() for p in ln.split("|")]
        if parts and parts[0] == "":
            parts = parts[1:]
        if parts and parts[-1] == "":
            parts = parts[:-1]
        return parts

    header = [h.lower() for h in cells(lines[header_idx])]
    col = {k: (header.index(k) if k in header else None)
           for k in ("name", "title", "company", "investment", "email")}

    rows, security_raw = [], ""
    for ln in lines[header_idx + 1:]:
        c = cells(ln)
        if c and all(set(x) <= set("-: ") for x in c):
            continue  # markdown separator row
        def get(k):
            j = col[k]
            return _pb_clean(c[j]) if (j is not None and j < len(c)) else ""
        name, title, company, investment, email = (
            get("name"), get("title"), get("company"), get("investment"), get("email"))
        if investment and not security_raw:
            security_raw = investment
        if not name and not company:
            continue
        if not name and company:
            kind = "firm_only"
        elif name and (not company or company.lower() == "self"
                       or company.lower() == name.lower()):
            kind = "angel"
        else:
            kind = "named"
        rows.append({"name": name, "title": title, "company": company,
                     "email": email, "kind": kind})
    return rows, security_raw


def _fetch_label_entries(jwt, endpoint, label_id):
    """GET a Pipeline custom-field labels endpoint and return {norm_name: entry_id}
    for the given label_id. Tolerant of both nested-under-label and flat-entry shapes."""
    req = urllib.request.Request(
        f"{PIPELINE_BASE}{endpoint}",
        method="GET",
        headers={"Authorization": f"Bearer {jwt}"},
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        body = json.loads(r.read())
    if isinstance(body, dict):
        items = (body.get("custom_field_labels") or body.get("entries")
                 or body.get("data") or [])
    elif isinstance(body, list):
        items = body
    else:
        items = []
    out = {}
    def _add(nm, eid):
        out[_norm_security(nm)] = int(eid)
        ps = _norm_security(_strip_paren(nm))
        if ps:
            out.setdefault(ps, int(eid))
    for it in items:
        if not isinstance(it, dict):
            continue
        # Case A: label object with nested dropdown entries.
        if it.get("id") == label_id and "custom_field_label_dropdown_entries" in it:
            for e in it.get("custom_field_label_dropdown_entries") or []:
                if isinstance(e, dict) and e.get("name") and e.get("id"):
                    _add(e["name"], e["id"])
        # Case B: item is itself a dropdown entry carrying its parent label id.
        elif it.get("custom_field_label_id") == label_id and it.get("name") and it.get("id"):
            _add(it["name"], it["id"])
    return out


def _strip_paren(s):
    return re.sub(r"\s*\([^)]*\)", "", s or "").strip()


def resolve_security_holdings(jwt, security):
    """Return (person_entry_id|None, company_entry_id|None) for a security name.
    Matches on both the full name and the parenthetical-stripped name, so
    'Physical Intelligence (Pi)' resolves against a 'Physical Intelligence' entry
    and vice versa."""
    cands = []
    for form in (security, _strip_paren(security)):
        k = _norm_security(form)
        if k and k not in cands:
            cands.append(k)
    person_entries = _fetch_label_entries(
        jwt, "/admin/custom_field_labels.json?conditions[entity_type]=person",
        PERSON_HOLDING_LABEL_ID)
    company_entries = _fetch_label_entries(
        jwt, "/admin/company_custom_field_labels.json?per_page=200",
        COMPANY_HOLDING_LABEL_ID)
    def pick(entries):
        for k in cands:
            if k in entries:
                return entries[k]
        return None
    return pick(person_entries), pick(company_entries)


def load_all_companies(s3):
    """All companies from companies.json (no issuer/org filter). Returns [{id,name}]."""
    obj = s3.get_object(Bucket=BUCKET, Key="companies.json")
    snap = json.loads(obj["Body"].read())
    return [{"id": c["id"], "name": c["name"]}
            for c in snap.get("companies", []) if c.get("name")]


def plan_pitchbook_firms(rows, companies_idx):
    """Match each distinct firm (from named + firm_only rows) to companies.json.
    A firm-only row whose Company equals an angel's own name (an un-deduped
    self-investor, e.g. 'Jeffrey Bezos') is dropped — it's the angel, not a firm."""
    angel_names = {r["name"].strip().lower() for r in rows if r["kind"] == "angel"}
    firms = {}
    for r in rows:
        if r["kind"] == "angel":
            continue
        fname = r["company"].strip()
        if fname and fname.lower() not in angel_names:
            firms.setdefault(fname.lower(), fname)
    firm_recs = [{"name": n} for n in firms.values()]
    matches, unmatched, ambiguous = match_recs_to_crm(firm_recs, companies_idx)
    matched   = [(h["name"], co["name"], co["id"]) for h, co, _lvl in matches]
    to_create = [h["name"] for h in unmatched]
    ambig     = [(h["name"], [c["name"] for c in cands]) for h, cands, _lvl in ambiguous]
    return matched, to_create, ambig


def render_pitchbook(security_raw, security, person_hid, company_hid,
                     sec_company_id, sec_company_name,
                     named, angels, firmonly, matched, to_create, ambig):
    def esc(x):
        return html.escape(str(x if x is not None else ""))

    def rows_table(items):
        if not items:
            return "<p class='muted'>none</p>"
        trs = "".join(
            "<tr>"
            f"<td>{esc(r['name'])}</td>"
            f"<td>{esc(r['title'])}</td>"
            f"<td>{esc(r['company'])}</td>"
            f"<td class='muted'>{esc(r['email'])}</td>"
            "</tr>" for r in items)
        return ("<table><thead><tr><th>Name</th><th>Title</th>"
                "<th>Company</th><th>Email</th></tr></thead><tbody>"
                + trs + "</tbody></table>")

    matched_block = ("<p class='muted'>none</p>" if not matched else
        "<table><thead><tr><th>Firm (paste)</th><th>Matched CRM company</th>"
        "<th>CRM id</th></tr></thead><tbody>"
        + "".join(f"<tr><td>{esc(a)}</td><td>{esc(b)}</td>"
                  f"<td class='muted'>{esc(cid)}</td></tr>"
                  for a, b, cid in matched) + "</tbody></table>")

    create_block = ("<p class='muted'>none</p>" if not to_create else
        "<ul>" + "".join(f"<li>{esc(n)}</li>" for n in to_create) + "</ul>")

    ambig_block = "" if not ambig else (
        "<h2>Ambiguous firm matches (review)</h2><ul>"
        + "".join(f"<li><b>{esc(a)}</b> → {esc(', '.join(cs))}</li>"
                  for a, cs in ambig) + "</ul>")

    p_line = (f"<span class='ok'>{person_hid}</span>" if person_hid
              else "<span class='fail'>NOT FOUND — person HOLDING will be skipped</span>")
    c_line = (f"<span class='ok'>{company_hid}</span>" if company_hid
              else "<span class='fail'>NOT FOUND — company HOLDING will be skipped</span>")
    s_line = (f"<span class='ok'>{esc(sec_company_name)} (id {esc(sec_company_id)})</span>"
              if sec_company_id
              else "<span class='fail'>not found in companies.json — check the security name</span>")

    total = len(named) + len(angels) + len(firmonly)
    return f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Market Price Updater · Pitchbook</title><style>{CSS}</style></head>
<body>
  <h1>Pitchbook Import — Preview</h1>
  <div class="banner dry">PARSE + RESOLVE ONLY — no records created, no writes. Live reads were used only to resolve holding IDs.</div>
  <div class="stats">
    <div class="stat"><span class="n">{total}</span><span class="l">rows</span></div>
    <div class="stat"><span class="n">{len(named)}</span><span class="l">named</span></div>
    <div class="stat"><span class="n">{len(angels)}</span><span class="l">angels</span></div>
    <div class="stat"><span class="n">{len(firmonly)}</span><span class="l">firm-only</span></div>
    <div class="stat"><span class="n">{len(matched)}</span><span class="l">firms matched</span></div>
    <div class="stat"><span class="n">{len(to_create)}</span><span class="l">firms to create</span></div>
    <a class="run-btn" href="">Run another</a>
  </div>

  <h2>Security</h2>
  <p>Investment column: <code>{esc(security_raw)}</code> → match name <code>{esc(security)}</code></p>
  <p>Security CRM company record: {s_line}</p>
  <p>person HOLDING (custom_label_3740611): {p_line}</p>
  <p>company HOLDING (custom_label_3746654): {c_line}</p>

  <h2>Named contacts</h2>
  {rows_table(named)}
  <h2>Angels</h2>
  {rows_table(angels)}
  <h2>Firm-only rows</h2>
  {rows_table(firmonly)}

  <h2>Firms matched to existing CRM companies</h2>
  {matched_block}
  <h2>Firms that would be created</h2>
  {create_block}
  {ambig_block}
</body></html>"""


def run_pitchbook(s3, text, dry_run):
    rows, security_raw = parse_pitchbook_table(text)
    if not rows:
        return render_error("Detected a pitchbook table but parsed no usable rows.")
    security = strip_legal_suffix(security_raw)

    jwt = get_jwt(s3)
    try:
        person_hid, company_hid = resolve_security_holdings(jwt, security)
    except Exception as e:
        return render_error(f"Holding resolution failed: {type(e).__name__}: {e}")

    all_companies = load_all_companies(s3)
    idx = build_match_index(all_companies)
    matched, to_create, ambig = plan_pitchbook_firms(rows, idx)

    # Cross-check: resolve the security itself to its CRM company record so the
    # preview confirms the holding entry maps to the real traded-issuer record.
    sec_matches, _su, _sa = match_recs_to_crm([{"name": security}], idx)
    if sec_matches:
        sec_co = sec_matches[0][1]
        sec_company_id, sec_company_name = sec_co["id"], sec_co["name"]
    else:
        sec_company_id, sec_company_name = None, None

    named    = [r for r in rows if r["kind"] == "named"]
    angels   = [r for r in rows if r["kind"] == "angel"]
    firmonly = [r for r in rows if r["kind"] == "firm_only"]

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    run_log = {
        "ts":              datetime.now(timezone.utc).isoformat(),
        "dry_run":         dry_run,
        "security_raw":    security_raw,
        "security":        security,
        "person_holding_id":  person_hid,
        "company_holding_id": company_hid,
        "security_company_id":   sec_company_id,
        "security_company_name": sec_company_name,
        "rows":            len(rows),
        "named":           len(named),
        "angels":          len(angels),
        "firm_only":       len(firmonly),
        "firms_matched":   matched,
        "firms_to_create": to_create,
        "firms_ambiguous": ambig,
    }
    try:
        s3.put_object(Bucket=BUCKET, Key=f"pitchbook-output/{ts}.json",
                      ContentType="application/json",
                      Body=json.dumps(run_log, indent=2, default=str).encode("utf-8"))
    except Exception as e:
        logger.warning(f"Couldn't write pitchbook run log: {e}")

    return render_pitchbook(security_raw, security, person_hid, company_hid,
                            sec_company_id, sec_company_name,
                            named, angels, firmonly, matched, to_create, ambig)


# --- Entry point -----------------------------------------------------------

def lambda_handler(event, context):
    method = event.get("requestContext", {}).get("http", {}).get("method", "GET")

    if method == "GET":
        return html_response(200, render_form())
    if method != "POST":
        return html_response(405, render_error(f"Method {method} not allowed"))

    body = _decode_body(event)
    raw = (body.get("hiive_text") or [""])[0]
    if not raw.strip():
        return html_response(400, render_error("No text submitted."))

    text, auth_ok = check_auth_and_strip(raw)
    if not auth_ok:
        return html_response(403, render_error("Unauthorized"))

    s3 = boto3.client("s3")
    try:
        # Mode detection: the two formats are never mixed in one paste.
        # Hiive dashboard copies always contain the word "hiive" (markdown links
        # to app.hiive.com, "Hiive Price" label, etc.). One-liners don't.
        is_hiive_mode = "hiive" in text.lower()
        if is_hiive_mode:
            hiive_recs    = parse_hiive_blocks(text)
            oneliner_recs = []
        elif is_pitchbook_table(text):
            return html_response(200, run_pitchbook(s3, text, DRY_RUN))
        else:
            hiive_recs    = []
            oneliner_recs = parse_one_liners(text)

        # Skip records with no data (truncated Hiive blocks)
        all_recs = hiive_recs + oneliner_recs
        skipped_empty = [r["name"] for r in all_recs
                         if r["set_bid"] is None and r["set_ask"] is None]
        all_recs = [r for r in all_recs
                    if r["set_bid"] is not None or r["set_ask"] is not None]

        if not all_recs:
            return html_response(200, render_error(
                "No usable data parsed from the paste (after the access code)."))

        crm = load_crm_issuers(s3)
        idx = build_match_index(crm)
        matches, unmatched, ambiguous = match_recs_to_crm(all_recs, idx)

        # Merge by CRM ID — one-liners override Hiive
        updates = merge_by_crm(matches)
        logger.info(f"Hiive={len(hiive_recs)} oneliners={len(oneliner_recs)} "
                    f"matched={len(matches)} merged_updates={len(updates)} "
                    f"unmatched={len(unmatched)} skipped={len(skipped_empty)}")

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        jwt = get_jwt(s3)
        results = []
        for rec, crm_co, _lvl in updates:
            ok, err = update_company(jwt, crm_co["id"], rec, today, DRY_RUN)
            results.append((ok, err))
            if not ok:
                logger.warning(f"Update failed for {crm_co['name']} ({crm_co['id']}): {err}")

        # Persist run log to S3 (non-fatal if it fails)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
        run_log = {
            "ts":              datetime.now(timezone.utc).isoformat(),
            "dry_run":         DRY_RUN,
            "parsed_hiive":    len(hiive_recs),
            "parsed_oneliner": len(oneliner_recs),
            "updates":         len(updates),
            "updated_ok":      sum(1 for ok, _ in results if ok),
            "failed":          sum(1 for ok, _ in results if not ok),
            "unmatched":       len(unmatched),
            "skipped_empty":   skipped_empty,
            "ambiguous":       [(h["name"], [c["name"] for c in cands], lvl)
                                for h, cands, lvl in ambiguous],
            "errors":          [(crm_co["name"], err)
                                for (_r, crm_co, _l), (ok, err) in zip(updates, results)
                                if not ok],
            "applied":         [{"id": crm_co["id"], "name": crm_co["name"],
                                 "set_bid": rec["set_bid"], "set_ask": rec["set_ask"],
                                 "set_price": rec["set_price"]}
                                for rec, crm_co, _l in updates],
        }
        try:
            s3.put_object(Bucket=BUCKET, Key=f"hiive-output/{ts}.json",
                          ContentType="application/json",
                          Body=json.dumps(run_log, indent=2, default=str).encode("utf-8"))
        except Exception as e:
            logger.warning(f"Couldn't write run log: {e}")

        return html_response(200, render_results(
            updates, results, unmatched, ambiguous, skipped_empty,
            parsed_hiive=len(hiive_recs),
            parsed_oneliners=len(oneliner_recs),
            dry_run=DRY_RUN,
        ))

    except Exception as e:
        logger.exception("Handler crashed")
        return html_response(500, render_error(f"{type(e).__name__}: {e}"))
