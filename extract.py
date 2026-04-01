"""
Sova Health · Dynamic Campaign Data Extractor
Handles any number of months, any years. Sorted chronologically.
Just add a new tab name below → run this → dashboard auto-updates.

Run: python extract.py
"""

import requests, csv, json, io, sys, re
from datetime import datetime
from urllib.parse import quote

# ══════════════════════════════════════════════════
# CONFIG — only edit this section
# ══════════════════════════════════════════════════
SHEET_ID = "16FAQeJ50cSY6GFjC_FXANv0CoqV0dbzD90rSPxLQuj8"

# Add every tab name exactly as it appears in Google Sheets.
# Uncomment months as you add them. Year-on-year just works.
SHEET_TABS = [
    "Jan 2026",
    "Feb 2026",
    "March 2026",
    # "April 2026",
    # "May 2026",
    # "June 2026",
    # "July 2026",
    # "August 2026",
    # "September 2026",
    # "October 2026",
    # "November 2026",
    # "December 2026",
    # "Jan 2027",   # ← next year: just add here, dashboard shows YoY tab automatically
    # "Feb 2027",
]

OUTPUT_FILE = "data.json"
# ══════════════════════════════════════════════════

HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}

MONTH_ORDER = {
    "jan":1,"january":1,"feb":2,"february":2,"mar":3,"march":3,
    "apr":4,"april":4,"may":5,"jun":6,"june":6,"jul":7,"july":7,
    "aug":8,"august":8,"sep":9,"sept":9,"september":9,
    "oct":10,"october":10,"nov":11,"november":11,"dec":12,"december":12
}

def get_sort_key(name):
    n = name.lower()
    m = re.search(r'(20\d{2})', n)
    year = int(m.group(1)) if m else 2026
    month = next((num for word, num in MONTH_ORDER.items() if word in n), 0)
    return (year, month)

def fetch_csv(sheet_id, sheet_name):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/gviz/tq?tqx=out:csv&sheet={quote(sheet_name)}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    return resp.text

FIELD_PATTERNS = {
    "name":       ["campaign name","campaign","name","title"],
    "team":       ["team","group","category"],
    "date":       ["date","campaign date","sent date"],
    "cohort":     ["cohort","audience","list","segment","batch"],
    "sent":       ["sent","total sent","messages sent"],
    "delivered":  ["delivered","delivery"],
    "read":       ["read count","reads","opened","opens"],
    "read_rate":  ["read rate","read %","open rate","read%"],
    "revenue":    ["revenue","rev","income","sales","amount","gmv"],
    "clicks":     ["clicked count","clicks","link click"],
    "click_rate": ["clicked rate","click rate","ctr"],
    "orders":     ["order count","orders","conversions","purchases"],
}

def detect_columns(headers):
    h = [x.lower().strip() for x in headers]
    col_map = {}
    for field, patterns in FIELD_PATTERNS.items():
        for pat in patterns:
            for i, hdr in enumerate(h):
                if pat in hdr:
                    col_map[field] = i
                    break
            if field in col_map:
                break
    return col_map

def safe_float(val):
    if not val: return 0.0
    val = str(val).strip()
    if val.lower() in ("#ref!","#value!","#error!","#n/a","n/a","-","—",""): return 0.0
    try: return float(val.replace(",","").replace("₹","").strip())
    except ValueError: return 0.0

def parse_sheet(csv_text):
    reader = csv.reader(io.StringIO(csv_text))
    rows = [r for r in reader if any(c.strip() for c in r)]
    if not rows: return [], {}
    header_idx = 0
    for i, row in enumerate(rows[:6]):
        joined = " ".join(row).lower()
        if "campaign name" in joined or ("team" in joined and "sent" in joined):
            header_idx = i; break
    headers = rows[header_idx]
    col_map = detect_columns(headers)
    def get(row, key, default=""):
        idx = col_map.get(key, -1)
        if idx == -1 or idx >= len(row): return default
        return row[idx].strip()
    campaigns = []
    for row in rows[header_idx + 1:]:
        name = get(row, "name")
        if not name or name.lower() in ("campaign name","name",""): continue
        campaigns.append({
            "name":       name,
            "team":       get(row,"team") or "Unknown",
            "date":       get(row,"date"),
            "cohort":     get(row,"cohort") or "Untagged",
            "sent":       int(safe_float(get(row,"sent"))),
            "delivered":  int(safe_float(get(row,"delivered"))),
            "read":       int(safe_float(get(row,"read"))),
            "read_rate":  round(safe_float(get(row,"read_rate")), 2),
            "revenue":    round(safe_float(get(row,"revenue")), 2),
            "clicks":     int(safe_float(get(row,"clicks"))),
            "click_rate": round(safe_float(get(row,"click_rate")), 2),
            "orders":     int(safe_float(get(row,"orders"))),
        })
    return campaigns, {"original_headers": headers, "detected_columns": {f: headers[i] for f, i in col_map.items()}}

def main():
    print("━" * 54)
    print("  Sova Health · Campaign Data Extractor")
    print("━" * 54)

    sorted_tabs = sorted(SHEET_TABS, key=get_sort_key)
    print(f"\n  Sheets ({len(sorted_tabs)}): {', '.join(sorted_tabs)}\n")

    output = {
        "last_updated": datetime.utcnow().isoformat() + "Z",
        "sheet_id": SHEET_ID,
        "sheet_order": [],
        "sheets": {}
    }

    success = 0
    for name in sorted_tabs:
        print(f"  → {name} ...", end=" ", flush=True)
        try:
            csv_text = fetch_csv(SHEET_ID, name)
            campaigns, schema = parse_sheet(csv_text)
            if not campaigns:
                print("⚠  no data rows found"); continue

            key = re.sub(r'[^a-z0-9_]', '', name.lower().replace(' ', '_'))
            n = name.lower()
            m = re.search(r'(20\d{2})', n)
            year = int(m.group(1)) if m else 2026
            month_num = next((num for word, num in MONTH_ORDER.items() if word in n), 0)
            rrs = [c["read_rate"] for c in campaigns if c["read_rate"] > 0]

            output["sheets"][key] = {
                "name": name, "key": key,
                "year": year, "month_num": month_num,
                "campaigns": campaigns, "schema": schema,
                "summary": {
                    "total_revenue":   round(sum(c["revenue"] for c in campaigns), 2),
                    "total_sent":      sum(c["sent"] for c in campaigns),
                    "total_delivered": sum(c["delivered"] for c in campaigns),
                    "total_campaigns": len(campaigns),
                    "avg_read_rate":   round(sum(rrs)/len(rrs), 2) if rrs else 0,
                }
            }
            output["sheet_order"].append(key)
            rev = output["sheets"][key]["summary"]["total_revenue"]
            print(f"✓  {len(campaigns)} campaigns | ₹{rev:,.0f}")
            success += 1

        except requests.HTTPError as e:
            print(f"✗  HTTP {e.response.status_code if e.response else '?'}")
        except Exception as e:
            print(f"✗  {e}")

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2, ensure_ascii=False)

    total = sum(len(s["campaigns"]) for s in output["sheets"].values())
    print(f"\n{'━'*54}")
    print(f"  ✅  {total} campaigns | {success}/{len(sorted_tabs)} sheets written")
    print(f"  📄  {OUTPUT_FILE} | {output['last_updated']}")
    print("━" * 54)

    if success == 0:
        print("\n  Fix: Google Sheet → File → Share → Anyone with link → Viewer")
        sys.exit(1)

if __name__ == "__main__":
    main()
