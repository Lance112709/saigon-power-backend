#!/usr/bin/env python3
"""
One-time CRM import script.
Run from backend/ directory: python3 import_crm.py
"""
import sys, os, re
sys.path.insert(0, os.path.dirname(__file__))

from app.db.client import get_client

FILE_PATH = "/Users/misa/Downloads/MERGED_DEALS FINAL.xlsx"

PROVIDER_TO_CODE = {
    "BUDGET POWER": "BUDGET",
    "IRON HORSE": "IRONHORSE",
    "HERITAGE POWER": "HERITAGE",
    "NRG ENERGY": "NRG_COMM",
    "DISCOUNT POWER": "NRG",
    "CHARIOT ENERGY": "CHARIOT",
    "CLEANSKY ENERGY": "CLEANSKY",
    "HUDSON ENERGY": "HUDSON",
}

def _extract_email(contact_str):
    if not contact_str:
        return None
    m = re.search(r"\(([^)@]+@[^)]+)\)", str(contact_str))
    return m.group(1).strip().lower() if m else None

def _to_date_str(val):
    if not val or str(val).strip() in ("", "nan", "None", "NaT"):
        return None
    try:
        from datetime import datetime as dt
        if hasattr(val, "date"):
            return val.date().isoformat()
        s = str(val).strip()
        for fmt in ("%m/%d/%Y %H:%M", "%m/%d/%Y", "%Y-%m-%d"):
            try:
                return dt.strptime(s[:16] if ":" in s else s[:10], fmt).date().isoformat()
            except Exception:
                continue
    except Exception:
        pass
    return None

def _to_float(val, max_val=9999.999999):
    try:
        f = float(str(val).replace(",", "").strip()) if val not in (None, "", "nan") else None
        if f is not None and abs(f) > max_val:
            return None
        return f
    except Exception:
        return None

def main():
    import openpyxl

    db = get_client()

    # 1. Add missing suppliers
    print("Checking suppliers...")
    new_suppliers = [
        {"name": "Chariot Energy", "code": "CHARIOT"},
        {"name": "CleanSky Energy", "code": "CLEANSKY"},
        {"name": "Hudson Energy", "code": "HUDSON"},
    ]
    for s in new_suppliers:
        existing = db.table("suppliers").select("id").eq("code", s["code"]).execute()
        if not existing.data:
            db.table("suppliers").insert(s).execute()
            print(f"  Added: {s['name']}")
        else:
            print(f"  Already exists: {s['name']}")

    # 2. Load supplier map
    sup_res = db.table("suppliers").select("id, code").execute()
    supplier_map = {s["code"]: s["id"] for s in sup_res.data}

    # 3. Load existing customers (idempotency)
    existing_res = db.table("crm_customers").select("id, email").execute()
    customer_by_key = {c["email"]: c["id"] for c in existing_res.data if c.get("email")}
    print(f"\nExisting customers in DB: {len(customer_by_key)}")

    # 4. Open Excel
    print(f"Opening: {FILE_PATH}")
    wb = openpyxl.load_workbook(FILE_PATH, read_only=True, data_only=True)
    ws = wb.active

    rows_iter = ws.iter_rows(values_only=True)
    next(rows_iter)  # skip header

    customers_created = 0
    deals_created = 0
    deals_skipped = 0

    for i, row in enumerate(rows_iter):
        if len(row) < 20:
            continue

        adder        = _to_float(row[0])
        signed_date  = _to_date_str(row[1])
        end_date     = _to_date_str(row[2])
        rate         = _to_float(row[3])
        start_date   = _to_date_str(row[4])
        term         = str(row[5] or "").strip() or None
        deal_name    = str(row[6] or "").strip() or None
        biz_name     = str(row[7] or "").strip() or None
        deal_owner   = str(row[8] or "").strip() or None
        deal_status  = str(row[9] or "ACTIVE").strip().upper() or "ACTIVE"
        meter_type   = str(row[10] or "").strip() or None
        deal_type    = str(row[11] or "").strip() or None
        esiid        = str(row[12] or "").strip() or None
        product_type = str(row[13] or "").strip() or None
        sales_agent  = str(row[14] or "").strip() or None
        svc_address  = str(row[15] or "").strip() or None
        provider     = str(row[16] or "").strip().upper() or None
        contact      = str(row[17] or "").strip()
        first_name   = str(row[18] or "").strip() or None
        last_name    = str(row[19] or "").strip() or None
        anxh         = str(row[20] or "").strip() or None
        dob          = str(row[21] or "").strip() or None
        email        = str(row[22] or "").strip().lower() or None
        mail_addr    = str(row[23] or "").strip() or None
        city         = str(row[24] or "").strip() or None
        postal       = str(row[25] or "").strip() or None
        state        = str(row[26] or "TX").strip() or "TX"
        phone        = str(row[28] or "").strip() if len(row) > 28 else None

        if not email:
            email = _extract_email(contact)

        # Build customer key
        if email:
            cust_key = email
        elif first_name or last_name:
            cust_key = f"{first_name or ''} {last_name or ''}".strip().lower()
        else:
            deals_skipped += 1
            continue

        # Get or create customer
        if cust_key not in customer_by_key:
            full_name = f"{first_name or ''} {last_name or ''}".strip()
            if not full_name:
                m = re.match(r"^([^(]+)\s*\(", contact)
                full_name = m.group(1).strip() if m else cust_key

            new_cust = {
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "email": email if "@" in (email or "") else None,
                "phone": phone,
                "dob": dob,
                "mailing_address": mail_addr,
                "city": city,
                "state": state[:2] if state else "TX",
                "postal_code": postal,
            }
            cres = db.table("crm_customers").insert(new_cust).execute()
            customer_id = cres.data[0]["id"]
            customer_by_key[cust_key] = customer_id
            customers_created += 1
        else:
            customer_id = customer_by_key[cust_key]

        # Map provider → supplier_id
        supplier_code = PROVIDER_TO_CODE.get(provider or "")
        supplier_id = supplier_map.get(supplier_code) if supplier_code else None

        deal = {
            "customer_id": customer_id,
            "deal_name": deal_name,
            "business_name": biz_name,
            "esiid": esiid,
            "provider": provider,
            "supplier_id": supplier_id,
            "meter_type": meter_type,
            "deal_type": deal_type,
            "deal_status": deal_status if deal_status in ("ACTIVE", "INACTIVE") else "ACTIVE",
            "adder": adder,
            "energy_rate": rate,
            "product_type": product_type,
            "contract_term": term,
            "contract_signed_date": signed_date,
            "contract_start_date": start_date,
            "contract_end_date": end_date,
            "service_address": svc_address,
            "deal_owner": deal_owner,
            "sales_agent": sales_agent,
            "anxh": anxh,
        }
        db.table("crm_deals").insert(deal).execute()
        deals_created += 1

        if deals_created % 100 == 0:
            print(f"  Progress: {deals_created} deals imported...")

    wb.close()
    print(f"\n✓ Import complete!")
    print(f"  Customers created: {customers_created}")
    print(f"  Deals created:     {deals_created}")
    print(f"  Deals skipped:     {deals_skipped}")

if __name__ == "__main__":
    main()
