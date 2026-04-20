from decimal import Decimal, ROUND_HALF_UP
from app.db.client import get_client

def calculate_commission(model: str, rate: float, usage_kwh: float = 0, bill_amount: float = 0) -> Decimal:
    r = Decimal(str(rate))
    if model == "per_kwh":
        return (Decimal(str(usage_kwh)) * r).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    elif model == "percentage_of_bill":
        return (Decimal(str(bill_amount)) * r).quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
    return Decimal("0")

def generate_expected_for_month(contract: dict, billing_month: str, usage_kwh: float = 0, bill_amount: float = 0):
    db = get_client()
    amount = calculate_commission(
        model=contract["commission_model"],
        rate=contract["commission_rate"],
        usage_kwh=usage_kwh,
        bill_amount=bill_amount
    )
    record = {
        "contract_id": contract["id"],
        "service_point_id": contract["service_point_id"],
        "supplier_id": contract["supplier_id"],
        "billing_month": billing_month,
        "usage_kwh": usage_kwh or None,
        "bill_amount": bill_amount or None,
        "commission_model": contract["commission_model"],
        "commission_rate": contract["commission_rate"],
        "expected_amount": float(amount),
    }
    res = db.table("expected_commissions").upsert(record, on_conflict="contract_id,service_point_id,billing_month").execute()
    return res.data[0]

def bulk_generate_expected(billing_month: str, supplier_id: str = None):
    db = get_client()
    q = db.table("contracts").select("*").eq("status", "active")
    if supplier_id:
        q = q.eq("supplier_id", supplier_id)
    contracts = q.execute().data
    results = []
    for contract in contracts:
        try:
            result = generate_expected_for_month(contract, billing_month)
            results.append({"contract_id": contract["id"], "status": "ok", "amount": result["expected_amount"]})
        except Exception as e:
            results.append({"contract_id": contract["id"], "status": "error", "error": str(e)})
    return {"generated": len(results), "results": results}
