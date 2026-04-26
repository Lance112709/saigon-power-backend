import json
from app.config import settings

SYSTEM_PROMPT = """You are a data extraction assistant for an energy broker commission system.
Given column headers and sample rows from a supplier commission statement,
identify which columns correspond to these fields:
- esiid: The Electric Service Identifier (long numeric ID, usually 17-22 digits). Also called Premise ID, Meter ID, Service ID.
- customer_name: The customer full name in one column. If only split columns exist, leave null.
- customer_first_name: Customer first name column (if name is split).
- customer_last_name: Customer last name column (if name is split).
- customer_status: Account/customer status (e.g. "Going Final", "Active", "Cancelled").
- service_address: The service or premise address.
- rate: The affinity/commission rate (per kWh or as a dollar amount).
- amount: The affinity/commission payment amount (dollars).
- kwh: Electricity usage/consumption in kWh. Also called Usage, Billed kWh.
- bill_start_date: The billing period start date.
- bill_end_date: The billing period end date.

Return a JSON object with this exact format:
{
  "mapping": {
    "esiid": "exact_column_name_or_null",
    "customer_name": "exact_column_name_or_null",
    "customer_first_name": "exact_column_name_or_null",
    "customer_last_name": "exact_column_name_or_null",
    "customer_status": "exact_column_name_or_null",
    "service_address": "exact_column_name_or_null",
    "rate": "exact_column_name_or_null",
    "amount": "exact_column_name_or_null",
    "kwh": "exact_column_name_or_null",
    "bill_start_date": "exact_column_name_or_null",
    "bill_end_date": "exact_column_name_or_null"
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
    mapping = {
        "esiid":               None,
        "customer_name":       None,
        "customer_first_name": None,
        "customer_last_name":  None,
        "customer_status":     None,
        "service_address":     None,
        "rate":                None,
        "amount":              None,
        "kwh":                 None,
        "bill_start_date":     None,
        "bill_end_date":       None,
    }

    for h in headers:
        hl = h.lower().replace(" ", "_").replace("-", "_")

        if any(k in hl for k in ["esiid", "esi_id", "meter_id", "service_id", "premise_id", "premise"]):
            mapping["esiid"] = h

        elif any(k in hl for k in ["first_name", "firstname"]):
            mapping["customer_first_name"] = h

        elif any(k in hl for k in ["last_name", "lastname"]):
            mapping["customer_last_name"] = h

        elif any(k in hl for k in ["customer_name", "full_name", "account_name"]) and "id" not in hl and "number" not in hl:
            mapping["customer_name"] = h
        elif hl == "customer":
            mapping["customer_name"] = h

        elif any(k in hl for k in ["customer_status", "acct_status", "account_status", "contract_status",
                                    "service_status", "enrollment_status", "cust_status", "status"]):
            mapping["customer_status"] = h

        elif any(k in hl for k in ["service_address", "premise_address", "cust_premise", "service_addr", "address"]):
            mapping["service_address"] = h

        elif any(k in hl for k in ["affinity_rate", "broker_rate", "commission_rate", "mils", "mil"]):
            mapping["rate"] = h
        elif "rate" in hl and "billed" not in hl and "start" not in hl and "end" not in hl:
            mapping["rate"] = h

        elif any(k in hl for k in ["affinity_amount", "commission", "residual", "amount", "payment"]):
            mapping["amount"] = h

        elif any(k in hl for k in ["kwh", "consumption", "usage", "billed_kwh"]):
            mapping["kwh"] = h

        elif any(k in hl for k in ["bill_start", "start_date", "period_start", "invoice_from", "from_date", "service_start"]):
            mapping["bill_start_date"] = h
        elif any(k in hl for k in ["bill_end", "end_date", "period_end", "invoice_to", "to_date", "service_end", "thru"]):
            mapping["bill_end_date"] = h
        elif any(k in hl for k in ["billing_month", "invoice_date", "bill_date", "period"]):
            mapping["bill_start_date"] = h  # fallback: treat single date as start

    return {"mapping": mapping, "confidence": 0.7, "notes": "Rule-based mapping (no AI key set)"}
