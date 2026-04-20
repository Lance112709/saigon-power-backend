#!/usr/bin/env python3
"""
Update crm_deals ESIIDs using service address matching from hubspot CSV.
Run from backend/ directory: python3 update_esiids.py
"""
import sys, os, csv, re
sys.path.insert(0, os.path.dirname(__file__))

from app.db.client import get_client

CSV_PATH = "/Users/misa/Downloads/hubspot-crm-exports-all-deals-2026-04-05.csv"

def normalize(addr: str) -> str:
    """Lowercase, strip punctuation, collapse whitespace for fuzzy matching."""
    if not addr:
        return ""
    addr = addr.lower()
    addr = re.sub(r"[,#.]", " ", addr)
    addr = re.sub(r"\s+", " ", addr).strip()
    return addr

def main():
    db = get_client()

    # Load CSV into a dict: normalized_address -> full_esiid
    csv_map: dict[str, str] = {}
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = (row.get("SERVICE ADDRESS") or "").strip()
            esiid = (row.get("ESI ID") or "").strip()
            if addr and esiid:
                csv_map[normalize(addr)] = esiid

    print(f"CSV loaded: {len(csv_map)} address → ESIID mappings")

    # Load all deals that have a service address
    offset = 0
    limit = 1000
    all_deals = []
    while True:
        res = db.table("crm_deals").select("id, esiid, service_address").range(offset, offset + limit - 1).execute()
        if not res.data:
            break
        all_deals.extend(res.data)
        if len(res.data) < limit:
            break
        offset += limit

    print(f"Deals in DB: {len(all_deals)}")

    updated = 0
    no_match = 0
    already_full = 0

    for deal in all_deals:
        svc = deal.get("service_address") or ""
        if not svc:
            no_match += 1
            continue

        key = normalize(svc)
        full_esiid = csv_map.get(key)

        if not full_esiid:
            # Try partial match: check if deal address is a substring of any CSV key
            for csv_key, eid in csv_map.items():
                if key and (key in csv_key or csv_key in key):
                    full_esiid = eid
                    break

        if not full_esiid:
            no_match += 1
            continue

        # Skip if already set to the same value
        if deal.get("esiid") == full_esiid:
            already_full += 1
            continue

        db.table("crm_deals").update({"esiid": full_esiid}).eq("id", deal["id"]).execute()
        updated += 1
        if updated % 100 == 0:
            print(f"  Updated {updated} deals...")

    print(f"\n✓ Done!")
    print(f"  ESIIDs updated:      {updated}")
    print(f"  Already correct:     {already_full}")
    print(f"  No address match:    {no_match}")

if __name__ == "__main__":
    main()
