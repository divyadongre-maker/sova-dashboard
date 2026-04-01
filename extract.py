"""
Sova Health · Dynamic Campaign Data Extractor
Auto-discovers ALL sheet tabs — no hardcoded names.
Add a new sheet tab → run this → dashboard updates automatically.

Run: python extract.py
"""

import requests, csv, json, io, sys, re
from datetime import datetime
from urllib.parse import quote

# ── ONLY THIS NEEDS TO CHANGE ────────────────────────
SHEET_ID = "16FAQeJ50cSY6GFjC_FXANv0CoqV0dbzD90rSPxLQuj8"
OUTPUT_FILE = "data.json"
# Skip these generic default tab names
SKIP_SHEETS = {"Sheet1", "Sheet2", "Sheet3", "Sheet4", "Sheet5", "Sheet6"}
# ─────────────────────────────────────────────────────

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# ═══════════════════════════════════════════════════
# STEP 1: AUTO-DISCOVER ALL SHEET NAMES
# ═══════════════════════════════════════════════════

def discover_sheets_via_feeds(sheet_id: str) -> list[str] | None:
    """Use Google Sheets Worksheets Feed API to list all tabs."""
    url = f"https://spreadsheets.google.com/feeds/worksheets/{sheet_id}/public/full?alt=json"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        entries = data.get("feed", {}).get("entry", [])
        names = [e["title"]["$t"] for e in entries if e.get("title", {}).get("$t")]
        return [n for n in names if n not in SKIP_SHEETS]
    except Exception as e:
        print(f"  (feeds API: {e})")
        return None


def discover_sheets_via_html(sheet_id: str) -> list[str] | None:
    """Fallback: parse the spreadsheet HTML to find tab names."""
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/edit"
    try:
        resp = requests.get(url, headers=HEADERS, timeout=12)
        html = resp.text
        # Try multiple patterns the HTML might use for sheet names
        patterns = [
            r'"label":"([^"]{2,60})"',
            r'data-sheet-name="([^"]+)"',
            r'class="docs-sheet-tab[^"]*"[^>]*title="([^"]+)"',
        ]
        found = []
        for pat in patterns:
            matches = re.findall(pat, html)
            found.extend(matches)
        
        unique = list(dict.fromkeys(n for n in found if n not in SKIP_SHEETS and len(n) <= 50))
        return unique if unique else None
    except Exception as e:
        print(f"  (HTML parse: {e})")
        return None


def discover_all_sheets(sheet_id: str) -> list[str]:
    """Try all discovery methods; return sheet names."""
    print("  → Method 1: Google Feeds API...")
    names = discover_sheets_via_feeds(sheet_id)
    if names:
        return names
    
    print("  → Method 2: HTML parsing...")
    names = discover_sheets_via_html(sheet_id)
    if names:
        return names
    
    return []


# ═══════════════════════════════════════════════════
# STEP 2: FETCH EACH SHEET AS CSV
# ═══════════════════════════════════════════════════

def fetch_csv(sheet_id: str, sheet_name: str) -> str:
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}"
        f"/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"
    )
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text


# ═══════════════════════════════════════════════════
# STEP 3: DYNAMIC COLUMN DETECTION
# Maps ANY column name → standard field
# ═══════════════════════════════════════════════════

FIELD_PATTERNS = {
    "name":       ["campaign name", "campaign", "name", "title", "subject"],
    "team":       ["team", "group", "category", "type"],
    "date":       ["date", "campaign date", "sent date", "day"],
    "cohort":     ["cohort", "audience", "list", "batch", "segment", "target"],
    "sent":       ["sent", "total sent", "messages sent"],
    "delivered":  ["delivered", "delivery"],
    "read":       ["read count", "reads", "opened", "opens"],
    "read_rate":  ["read rate", "read %", "open rate", "open %", "read%"],
    "revenue":    ["revenue", "rev", "income", "sales", "amount", "gmv"],
    "clicks":     ["clicked count", "clicks", "link click"],
    "click_rate": ["clicked rate", "click rate", "ctr"],
    "orders":     ["order count", "orders", "conversions", "purchases"],
}

def detect_columns(headers: list[str]) -> dict[str, int]:
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


def safe_float(val: str) -> float:
    if not val: return 0.0
    val = str(val).strip()
    if val.lower() in ("#ref!", "#value!", "#error!", "#n/a", "n/a", "-", "—", ""):
        return 0.0
    try:
        return float(val.replace(",", "").replace("₹", "").strip())
    except ValueError:
        return 0.0


# ═══════════════════════════════════════════════════
# STEP 4: PARSE SHEET → CAMPAIGNS LIST
# ═══════════════════════════════════════════════════

def parse_sheet(csv_text: str) -> tuple[list[dict], dict]:
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

    # Report detected columns
    detected = {f: headers[i] for f, i in col_map.items()}

    def get(row: list, key: str, default="") -> str:
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


# ═══════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════

def main():
    print("━" * 52)
    print("  Sova Health · Dynamic Campaign Extractor")
    print("━" * 52)
    print(f"\n  Sheet ID: {SHEET_ID[:20]}...\n")

    # ── Discover sheets
    print("  [1/3] Discovering sheet tabs...")
    sheet_names = discover_all_sheets(SHEET_ID)

    if not sheet_names:
        print("\n  ✗ Could not auto-discover sheets.")
        print("  → Make sheet public: File → Share → Anyone with link can view")
        print("  → Or enter sheet names manually (see MANUAL_SHEETS in config)")
        sys.exit(1)

    print(f"\n  ✓ Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")

    # ── Fetch & parse each sheet
    print(f"\n  [2/3] Fetching {len(sheet_names)} sheets...\n")
    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "sheet_id": SHEET_ID,
        "discovered_sheets": sheet_names,
        "sheets": {}
    }

    success = 0
    for name in sheet_names:
        print(f"  → {name}")
        try:
            csv_text = fetch_csv(SHEET_ID, name)
            campaigns, schema = parse_sheet(csv_text)

            if not campaigns:
                print(f"     ⚠ No campaign rows found (empty or unrecognized format)")
                continue

            key = re.sub(r'[^a-z0-9_]', '', name.lower().replace(' ', '_'))
            output["sheets"][key] = {
                "name": name,
                "campaigns": campaigns,
                "schema": schema,
            }

            total_rev = sum(c["revenue"] for c in campaigns)
            print(f"     ✓ {len(campaigns)} campaigns | Revenue: ₹{total_rev:,.0f}")
            print(f"     ✓ Columns detected: {', '.join(schema['detected_columns'].keys())}")
            if schema["unmapped_headers"]:
                print(f"     ℹ Unmapped headers: {schema['unmapped_headers']}")
            success += 1

        except requests.HTTPError as e:
            print(f"     ✗ HTTP {e.response.status_code} — check sheet is public")
        except Exception as e:
            print(f"     ✗ Error: {e}")

    # ── Write output
    print(f"\n  [3/3] Writing {OUTPUT_FILE}...")
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total_camps = sum(len(s["campaigns"]) for s in output["sheets"].values())
    print(f"\n{'━'*52}")
    print(f"  ✅ Done — {total_camps} campaigns across {success} sheets")
    print(f"  📄 Output: {OUTPUT_FILE}")
    print(f"  📅 Timestamp: {output['last_updated']}")
    print("━" * 52)

    if success == 0:
        print("\n  ⚠  All sheets failed. Fix:")
        print("  1. Open Google Sheet → File → Share → Anyone with link can view")
        print("  2. Run: python extract.py")
        sys.exit(1)


if __name__ == "__main__":
    main()
