"""
Sova Health · Dynamic Campaign Data Extractor
Run: python extract.py
"""

import requests, csv, json, io, sys, re
from datetime import datetime
from urllib.parse import quote

# ══════════════════════════════════════════════════════
# CONFIG — only edit these 3 things
# ══════════════════════════════════════════════════════
SHEET_ID = "16FAQeJ50cSY6GFjC_FXANv0CoqV0dbzD90rSPxLQuj8"

# ✅ ADD YOUR EXACT TAB NAMES HERE (copy-paste from Google Sheets)
MANUAL_SHEETS = [
    "Jan 2026",
    "Feb 2026",
    "March 2026",
]

OUTPUT_FILE = "data.json"
# ══════════════════════════════════════════════════════

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

SKIP_SHEETS = {"Sheet1", "Sheet2", "Sheet3", "Sheet4", "Sheet5", "Sheet6"}


# ── SHEET DISCOVERY (tries auto first, falls back to MANUAL_SHEETS) ──

def discover_via_feeds(sheet_id):
    """Google Sheets public JSON feed."""
    url = f"https://spreadsheets.google.com/feeds/worksheets/{sheet_id}/public/full?alt=json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entries = data.get("feed", {}).get("entry", [])
        names = [e["title"]["$t"] for e in entries if e.get("title", {}).get("$t")]
        names = [n for n in names if n not in SKIP_SHEETS]
        if names:
            print(f"  ✓ Auto-discovered: {', '.join(names)}")
            return names
    except Exception as e:
        print(f"  (feeds API failed: {e})")
    return None


def get_sheet_names(sheet_id):
    print("  → Trying auto-discovery...")
    names = discover_via_feeds(sheet_id)
    if names:
        return names

    # Fallback to manual list
    if MANUAL_SHEETS:
        print(f"  → Using manual list: {', '.join(MANUAL_SHEETS)}")
        return MANUAL_SHEETS

    print("  ✗ No sheet names available. Add names to MANUAL_SHEETS in extract.py")
    sys.exit(1)


# ── FETCH CSV ──

def fetch_csv(sheet_id, sheet_name):
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


# ── COLUMN DETECTION ──

FIELD_PATTERNS = {
    "name":       ["campaign name", "campaign", "name", "title"],
    "team":       ["team", "group", "category"],
    "date":       ["date", "campaign date", "sent date"],
    "cohort":     ["cohort", "audience", "list", "segment", "batch"],
    "sent":       ["sent", "total sent", "messages sent"],
    "delivered":  ["delivered", "delivery"],
    "read":       ["read count", "reads", "opened", "opens"],
    "read_rate":  ["read rate", "read %", "open rate", "read%"],
    "revenue":    ["revenue", "rev", "income", "sales", "amount", "gmv"],
    "clicks":     ["clicked count", "clicks", "link click"],
    "click_rate": ["clicked rate", "click rate", "ctr"],
    "orders":     ["order count", "orders", "conversions", "purchases"],
}

def detect_columns(headers):
    h = [x.lower().strip() for x in headers]
    col_map = {}
    for field, patterns in FIELD_PATTERNS.items():
        for pat in patterns:
            for i, header in enumerate(h):
                if pat in header:
                    col_map[field] = i
                    break
            if field in col_map:
                break
    return col_map

def safe_float(val):
    if not val: return 0.0
    val = str(val).strip()
    if val.lower() in ("#ref!", "#value!", "#error!", "#n/a", "n/a", "-", "—", ""):
        return 0.0
    try:
        return float(val.replace(",", "").replace("₹", "").strip())
    except ValueError:
        return 0.0


# ── PARSE SHEET ──

def parse_sheet(csv_text):
    reader = csv.reader(io.StringIO(csv_text))
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows:
        return [], {}

    # Find header row
    header_idx = 0
    for i, row in enumerate(rows[:6]):
        joined = " ".join(row).lower()
        if "campaign name" in joined or ("team" in joined and "sent" in joined):
            header_idx = i
            break

    headers = rows[header_idx]
    col_map = detect_columns(headers)
    detected = {f: headers[i] for f, i in col_map.items()}

    def get(row, key, default=""):
        idx = col_map.get(key, -1)
        if idx == -1 or idx >= len(row): return default
        return row[idx].strip()

    campaigns = []
    for row in rows[header_idx + 1:]:
        name = get(row, "name")
        if not name or name.lower() in ("campaign name", "name", ""):
            continue
        campaigns.append({
            "name":       name,
            "team":       get(row, "team") or "Unknown",
            "date":       get(row, "date"),
            "cohort":     get(row, "cohort") or "Untagged",
            "sent":       int(safe_float(get(row, "sent"))),
            "delivered":  int(safe_float(get(row, "delivered"))),
            "read":       int(safe_float(get(row, "read"))),
            "read_rate":  round(safe_float(get(row, "read_rate")), 2),
            "revenue":    round(safe_float(get(row, "revenue")), 2),
            "clicks":     int(safe_float(get(row, "clicks"))),
            "click_rate": round(safe_float(get(row, "click_rate")), 2),
            "orders":     int(safe_float(get(row, "orders"))),
        })

    schema = {
        "original_headers": headers,
        "detected_columns": detected,
        "unmapped_headers": [h for i, h in enumerate(headers) if i not in col_map.values()]
    }
    return campaigns, schema


# ── MAIN ──

def main():
    print("━" * 52)
    print("  Sova Health · Campaign Data Extractor")
    print("━" * 52)

    print("\n  [1/3] Getting sheet names...")
    sheet_names = get_sheet_names(SHEET_ID)

    print(f"\n  [2/3] Fetching {len(sheet_names)} sheets...\n")
    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "sheet_id": SHEET_ID,
        "sheets": {}
    }

    success = 0
    for name in sheet_names:
        print(f"  → {name}")
        try:
            csv_text = fetch_csv(SHEET_ID, name)
            campaigns, schema = parse_sheet(csv_text)

            if not campaigns:
                print(f"     ⚠ No rows found — check tab name matches exactly")
                continue

            key = re.sub(r'[^a-z0-9_]', '', name.lower().replace(' ', '_'))
            output["sheets"][key] = {
                "name": name,
                "campaigns": campaigns,
                "schema": schema,
            }

            total_rev = sum(c["revenue"] for c in campaigns)
            print(f"     ✓ {len(campaigns)} campaigns | ₹{total_rev:,.0f} revenue")
            print(f"     ✓ Columns mapped: {', '.join(schema['detected_columns'].keys())}")
            success += 1

        except requests.HTTPError as e:
            status = e.response.status_code if e.response else '?'
            print(f"     ✗ HTTP {status} — sheet may not be public")
        except Exception as e:
            print(f"     ✗ Error: {e}")

    print(f"\n  [3/3] Writing {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total = sum(len(s["campaigns"]) for s in output["sheets"].values())
    print(f"\n{'━'*52}")
    print(f"  ✅ {total} campaigns across {success}/{len(sheet_names)} sheets")
    print(f"  📅 {output['last_updated']}")
    print("━" * 52)

    if success == 0:
        print("\n  ✗ Nothing written. Two things to check:")
        print("  1. Google Sheet → File → Share → Anyone with link → Viewer")
        print("  2. MANUAL_SHEETS names must match tabs EXACTLY (case-sensitive)")
        sys.exit(1)


if __name__ == "__main__":
    main()
