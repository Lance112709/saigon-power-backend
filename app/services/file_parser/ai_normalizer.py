import json
from app.config import settings

SYSTEM_PROMPT = """You are a data extraction assistant for an energy broker commission system.
Given column headers and sample rows from a supplier commission statement,
identify which columns correspond to these fields:
- esiid: The Electric Service Identifier (long numeric ID, usually 17-22 digits)
- customer_name: The customer or account name
- billing_month: The billing period or invoice date
- amount: The commission or payment amount (dollars)
- kwh: Electricity usage/consumption in kWh
- rate: The commission rate (per kWh or percentage)

Return a JSON object with this exact format:
{
  "mapping": {
    "esiid": "exact_column_name_or_null",
    "customer_name": "exact_column_name_or_null",
    "billing_month": "exact_column_name_or_null",
    "amount": "exact_column_name_or_null",
    "kwh": "exact_column_name_or_null",
    "rate": "exact_column_name_or_null"
  },
  "confidence": 0.95,
  "notes": "any observations about the data format"
}
Only return the JSON object, nothing else."""

def normalize_columns(headers: list, sample_rows: list) -> dict:
    if not settings.anthropic_api_key or settings.anthropic_api_key == "your_anthropic_api_key_here":
        return _rule_based_mapping(headers)

    import anthropic
    client = anthropic.Anthropic(api_key=settings.anthropic_api_key)

    user_msg = f"Headers: {json.dumps(headers)}\n\nSample rows:\n{json.dumps(sample_rows, indent=2)}"

    response = client.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=500,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}]
    )

    try:
        return json.loads(response.content[0].text)
    except Exception:
        return _rule_based_mapping(headers)

def _rule_based_mapping(headers: list) -> dict:
    """Fallback: match by common column name patterns."""
    mapping = {
        "esiid": None,
        "customer_name": None,
        "billing_month": None,
        "amount": None,
        "kwh": None,
        "rate": None
    }

    for h in headers:
        hl = h.lower().replace(" ", "_").replace("-", "_")
        if any(k in hl for k in ["esiid", "esi_id", "meter_id", "service_id", "premise_id", "premise"]):
            mapping["esiid"] = h
        elif any(k in hl for k in ["customer_name", "customer", "account_name"]) and "id" not in hl and "number" not in hl:
            mapping["customer_name"] = h
        elif any(k in hl for k in ["invoice_from", "invoice_date", "billing_month", "period_start"]):
            mapping["billing_month"] = h
        elif any(k in hl for k in ["invoice", "billing", "period", "month"]) and "stop" not in hl and "end" not in hl:
            mapping["billing_month"] = h
        elif any(k in hl for k in ["commission", "residual", "amount", "payment", "paid"]):
            mapping["amount"] = h
        elif any(k in hl for k in ["kwh", "consumption", "usage", "billed_kwh"]):
            mapping["kwh"] = h
        elif any(k in hl for k in ["broker_rate", "mils", "mil"]):
            mapping["rate"] = h
        elif "rate" in hl and "billed" not in hl:
            mapping["rate"] = h

    return {"mapping": mapping, "confidence": 0.7, "notes": "Rule-based mapping (no AI key set)"}
