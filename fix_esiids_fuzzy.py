#!/usr/bin/env python3
"""
Fix the remaining ~43 deals with bad ESIIDs (scientific notation) by fuzzy-matching
on street number + street name only (ignore city/state/zip).
Run from backend/: python3 fix_esiids_fuzzy.py
"""
import sys, os, csv, re
sys.path.insert(0, os.path.dirname(__file__))
from app.db.client import get_client

CSV_PATH = "/Users/misa/Downloads/hubspot-crm-exports-all-deals-2026-04-05.csv"


def extract_street_key(addr: str) -> str:
    """Extract 'NUMBER STREETNAME' as a normalized key, stripping unit/apt/suite."""
    if not addr:
        return ""
    addr = addr.strip().upper()
    # Take only the part before the first comma (street portion)
    street = addr.split(",")[0].strip()
    # Normalize abbreviations
    for old, new in [("STREET", "ST"), ("AVENUE", "AVE"), ("DRIVE", "DR"),
                     ("BOULEVARD", "BLVD"), ("ROAD", "RD"), ("LANE", "LN"),
                     ("COURT", "CT"), ("CIRCLE", "CIR"), ("TRAIL", "TRL"),
                     ("PLACE", "PL"), ("WAY", "WAY"), ("HIGHWAY", "HWY")]:
        street = re.sub(r'\b' + old + r'\b', new, street)
    # Remove unit/suite/apt suffixes (#123, STE 101, APT 2, UNIT 5, etc.)
    street = re.sub(r'\s+(#|STE|SUITE|APT|UNIT|UNT|RM|ROOM)\s*\S+.*$', '', street)
    # Collapse whitespace
    street = re.sub(r'\s+', ' ', street).strip()
    return street


def main():
    db = get_client()

    # Load CSV
    csv_map: dict[str, str] = {}  # street_key -> esiid
    csv_raw: dict[str, str] = {}  # street_key -> original address (for logging)
    with open(CSV_PATH, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            addr = (row.get("SERVICE ADDRESS") or "").strip()
            esiid = (row.get("ESI ID") or "").strip()
            if addr and esiid:
                key = extract_street_key(addr)
                if key:
                    csv_map[key] = esiid
                    csv_raw[key] = addr

    print(f"CSV loaded: {len(csv_map)} street-key → ESIID mappings")

    # Fetch all bad deals (scientific notation or null ESIID)
    all_deals = []
    offset = 0
    while True:
        res = db.table("crm_deals").select("id, esiid, service_address, customer_id").range(offset, offset + 999).execute()
        if not res.data:
            break
        all_deals.extend(res.data)
        if len(res.data) < 1000:
            break
        offset += 1000

    bad_deals = [
        d for d in all_deals
        if (not d.get("esiid")) or ("E+" in str(d.get("esiid", "")).upper())
    ]
    print(f"\nDeals with bad/missing ESIIDs: {len(bad_deals)}")

    updated = 0
    no_match = []

    for deal in bad_deals:
        svc = deal.get("service_address") or ""
        if not svc:
            print(f"  [NO ADDR]  id={deal['id']}")
            no_match.append(deal)
            continue

        key = extract_street_key(svc)
        esiid = csv_map.get(key)
        matched_via = "exact"

        if not esiid:
            # Fuzzy: try matching just the first two tokens (number + first word of street name)
            tokens = key.split()
            if len(tokens) >= 2:
                prefix = tokens[0] + " " + tokens[1]
                candidates = [(k, v) for k, v in csv_map.items() if k.startswith(prefix)]
                if len(candidates) == 1:
                    esiid = candidates[0][1]
                    matched_via = f"prefix '{prefix}'"
                elif len(candidates) > 1:
                    # Pick the closest: most matching tokens
                    best = max(candidates, key=lambda c: sum(1 for t in key.split() if t in c[0].split()))
                    esiid = best[1]
                    matched_via = f"best-prefix '{prefix}' → '{best[0]}'"

        if esiid:
            db.table("crm_deals").update({"esiid": esiid}).eq("id", deal["id"]).execute()
            print(f"  [FIXED]    '{svc}' → {esiid}  (via {matched_via})")
            updated += 1
        else:
            print(f"  [NO MATCH] '{svc}' (key='{key}')")
            no_match.append(deal)

    print(f"\n✓ Done!")
    print(f"  Fixed:       {updated}")
    print(f"  Still stuck: {len(no_match)}")

    if no_match:
        print("\nUnmatched deals (manual lookup needed):")
        for d in no_match:
            print(f"  id={d['id']} addr={d.get('service_address', 'N/A')}")


if __name__ == "__main__":
    main()
