"""Parser tests: fingerprint detection, ESIID integrity, amount math."""
import io
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.file_parser.provider_parsers import (
    detect_and_parse, normalize_esiid, is_valid_esiid, label_from_filename,
)


def xlsx(sheets: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        for name, rows in sheets.items():
            pd.DataFrame(rows).to_excel(w, sheet_name=name, index=False)
    return buf.getvalue()


# ── ESIID normalization ───────────────────────────────────────────────────────

def test_esiid_centerpoint_22_digits_kept():
    assert normalize_esiid("1008901023816857340105") == "1008901023816857340105"

def test_esiid_meter_suffix_trimmed_centerpoint():
    # Iron Horse appends meter suffixes: 22-digit CenterPoint id + "001"
    assert normalize_esiid("1008901020782984255100001") == "1008901020782984255100"

def test_esiid_meter_suffix_trimmed_oncor():
    assert normalize_esiid("10443720008297350001") == "10443720008297350"

def test_esiid_formatting_stripped():
    assert normalize_esiid("#:10032789407800704") == "10032789407800704"
    assert normalize_esiid(" 10443720008297350 ") == "10443720008297350"

def test_esiid_never_scientific_notation():
    # the exact failure mode of float64 corruption
    assert not is_valid_esiid(normalize_esiid("1.0089010238159e+21"))

def test_valid_esiid_lengths():
    assert is_valid_esiid("10443720008297350")            # 17 Oncor
    assert is_valid_esiid("1008901023816857340105")       # 22 CenterPoint
    assert not is_valid_esiid("23668")
    assert not is_valid_esiid("")


# ── Statement month from filename ────────────────────────────────────────────

@pytest.mark.parametrize("name,expect", [
    ("Mar 2026 Saigon Power Statement (1).xlsx", "2026-03"),
    ("December 2024 Saigon Power Statement.xlsx", "2024-12"),
    ("SAIGON POWER (LANCE NGUYEN) 03-2026.xlsx", "2026-03"),
    ("Affinity Report_Saigon_Power_LLC_MAR 2026.xlsx", "2026-03"),
    ("Affinity Report_Saigon_Power_LLC_Nov 2025 (1).xlsx", "2025-11"),
    ("Saigon Power LLC-CE Commissions Statement-Jan-26-02172026.xlsx", "2026-01"),
    ("broker-report-Saigon Power (Lance-Nguyen).xlsx", ""),
])
def test_label_from_filename(name, expect):
    assert label_from_filename(name) == expect


# ── Format detection + parsing ───────────────────────────────────────────────

def test_discount_power_detected():
    data = xlsx({"Summary": [{"TOTAL": 10}], "Residuals": [
        {"CUSTOMER_NAME": "A", "ESIID": "10443720000135519", "BROKER_RATE": "0.008",
         "CONSUMPTION": "938", "RESIDUAL_COMMISSION": "7.504",
         "INVOICE_FROM_DATE": "2026-02-07", "INVOICE_TO_DATE": "2026-03-10",
         "TRANSACTION_TYPE": "Switch Back"},
    ]})
    res = detect_and_parse(data, "Mar 2026 Saigon Power Statement.xlsx")
    assert res and res["provider_group"] == "Discount Power/Cirro"
    assert res["row_count"] == 1
    r = res["rows"][0]
    assert r["esiid"] == "10443720000135519"
    assert r["amount"] == 7.504
    assert r["rate"] == 0.008
    assert r["statement_label"] == "2026-03"


def test_iron_horse_legacy_result_format():
    data = xlsx({"Result": [
        {"Payment Type": "Residual", "Customer": "Duong, Hiep",
         "Utility Account Number": "1008901022900612170112",
         "Invoice Start Date": "2026-01-08", "Invoice End Date": "2026-01-12",
         "Billed Usage": "36", "Commission Rate": "0.007", "Commission Paid": "0.252"},
    ]})
    res = detect_and_parse(data, "SAIGON POWER (Lance Nguyen) JANUARY 2026.xlsx")
    assert res and res["provider_group"] == "Iron Horse"
    assert res["rows"][0]["amount"] == 0.252
    assert res["rows"][0]["rate"] == 0.007


def test_budget_power_detected_with_status_and_going_final():
    def bp_row(esiid, status):
        return {"Premise ID": esiid, "Usage": "661",
                "Affinity Rate in ($)": "0.008", "Affinity Amount": "5.29",
                "Premise Address": "3532 OMEARA DR", "Premise City": "HOUSTON", "Premise Zip": "77025",
                "Cust First Name": "Hoang", "Cust Last Name": "Nguyen",
                "Cust Status": status, "Start Date": "2026-02-18", "End Date": "2026-03-19"}
    data = xlsx({"Sheet1": [
        bp_row("1008901023900175571", "Inactive"),
        bp_row("1008901023900175572", "Active"),
        bp_row("1008901023900175573", "Active"),
    ]})
    res = detect_and_parse(data, "Affinity Report_Saigon_Power_LLC_MAY 2026.xlsx")
    assert res and res["provider_group"] == "Budget Power"
    assert len(res["going_final"]) == 1  # the Inactive minority flagged for follow-up


def test_mass_churn_statement_suppresses_going_final():
    def bp_row(esiid):
        return {"Premise ID": esiid, "Usage": "661",
                "Affinity Rate in ($)": "0.008", "Affinity Amount": "5.29",
                "Premise Address": "X", "Premise City": "HOUSTON", "Premise Zip": "77025",
                "Cust First Name": "A", "Cust Last Name": "B",
                "Cust Status": "Inactive", "Start Date": "2026-02-18", "End Date": "2026-03-19"}
    data = xlsx({"Sheet1": [bp_row("1008901023900175571"), bp_row("1008901023900175572")]})
    res = detect_and_parse(data, "Affinity Report_Saigon_Power_LLC_APR 2026.xlsx")
    assert res["going_final"] == []
    assert any("unreliable" in w for w in res["warnings"])


def test_chariot_clawback_rows_separated():
    data = xlsx({
        "Commissions": [
            {"Premise ID": "10089010097630001122", "Affinity Rate in ($)": "0.007",
             "Affinity Amount": "19.02", "Metered Points": "2660",
             "Premise Address": "4601 AVENUE H", "Premise City": "ROSENBERG",
             "Premise State": "TX", "Premise Zip": "77471",
             "Cust First Name": "Lyly", "Cust Last Name": "Duyen",
             "Start Date": "2026-01-07", "End Date": "2026-02-06"},
        ],
        "Clawback": [
            {"Enrollment ID:": "X", "ESID:": "10089010097630001122", "Name:": "Lyly",
             "Start Date:": "2026-01-01", "End Date:": "2026-02-01"},
        ],
    })
    res = detect_and_parse(data, "Saigon Power LLC-CE Commissions Statement-Feb-26-03162026.xlsx")
    assert res and res["provider_group"] == "Chariot"
    types = sorted(r["row_type"] for r in res["rows"])
    assert types == ["clawback", "commission"]
    assert res["total_amount"] == 19.02  # clawback rows excluded from total


def test_nrg_business_statement_parsed():
    data = xlsx({
        "Summary": [{"NRG Commission Statement": "Broker", "Unnamed: 1": "Saigon Power LLC"},
                    {"NRG Commission Statement": "Total Paid", "Unnamed: 1": "88.51"}],
        "Info": [{"Source Legend": "x"}],
        "Commissions": [
            {"Commission ID": "48519719", "Customer Name": "1 GOLDEN DRAGON LLC",
             "Commission Type": "Plan", "Calculation Type": "OnFlow",
             "Current LDC Account #": "1008901022900661710113", "LDC Status": "Enrolled",
             "Period Start": "4/16/2026", "Period End": "5/17/2026",
             "Commission Usage": "9073", "UOM": "kWh", "Adder": "0.006997",
             "Amount": "63.48", "Total": "63.48",
             "Contract Start Date": "5/1/2024", "Contract End Date": "4/30/2027"},
            {"Commission ID": "48519720", "Customer Name": "",
             "Commission Type": "Manual", "Calculation Type": "Other",
             "Current LDC Account #": "", "Notes": "NRG Winter Incentive",
             "Amount": "25.03", "Total": "25.03"},
        ],
        "Delinquents": [],
    })
    res = detect_and_parse(data, "1-3UVANNQ_Saigon Power LLC_US_Monthly_1123_May312026_NRG.xlsx")
    assert res and res["provider_group"] == "NRG Commercial"
    assert res["supplier"]["code"] == "NRGBIZ"
    assert res["statement_label"] == "2026-05"
    comm = [r for r in res["rows"] if r["row_type"] == "commission"]
    bonus = [r for r in res["rows"] if r["row_type"] == "bonus"]
    assert len(comm) == 1 and len(bonus) == 1
    assert comm[0]["esiid"] == "1008901022900661710113"
    assert comm[0]["rate"] == 0.006997 and comm[0]["amount"] == 63.48
    assert bonus[0]["amount"] == 25.03
    assert not res["warnings"]  # 63.48 + 25.03 == Total Paid


def test_cleansky_month_sheets_get_own_labels():
    row = {"MARKETER_NAME": "TITAN", "CUSTOMER_NAME": "NGUYEN",
           "LDC_ACCOUNT_NUM": "10032789407800704", "USAGE_NUM": "67",
           "RATE_UNIT_NUM": "0.005", "COMMISSION_AMT": "0.34",
           "BEGIN_READ_DATE": "2026-01-27", "END_READ_DATE": "2026-02-25"}
    data = xlsx({"Mar_2026": [row], "Feb_2026": [dict(row, COMMISSION_AMT="0.5")]})
    res = detect_and_parse(data, "Lastest CleanSky Commission Report .xlsx")
    assert res and res["provider_group"] == "CleanSky"
    assert sorted(res["labels"]) == ["2026-02", "2026-03"]


def test_unknown_format_returns_none_for_ai_fallback():
    data = xlsx({"Sheet1": [{"Meter": "123", "Money": "9.99"}]})
    assert detect_and_parse(data, "mystery.xlsx") is None


def test_excel_numeric_esiid_not_corrupted():
    """ESIIDs stored as NUMBER cells must survive parsing intact.

    Note: Excel numbers are float64, so a 22-digit ESIID cannot be stored
    numerically by anyone — providers store those as text. The realistic
    numeric case is a 17-digit Oncor ID, which must come through without
    a '.0' suffix or scientific notation."""
    buf = io.BytesIO()
    df = pd.DataFrame([{
        "CUSTOMER_NAME": "B", "ESIID": 10443720008297350,  # int cell, 17 digits
        "BROKER_RATE": 0.008, "CONSUMPTION": 100, "RESIDUAL_COMMISSION": 0.8,
        "INVOICE_FROM_DATE": "2026-01-01", "INVOICE_TO_DATE": "2026-02-01",
        "TRANSACTION_TYPE": "",
    }])
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, sheet_name="Residuals", index=False)
    res = detect_and_parse(buf.getvalue(), "Feb 2026 Saigon Power Statement.xlsx")
    assert res["rows"][0]["esiid"] == "10443720008297350"


def _sheet_rows(rows):
    return pd.DataFrame(rows)


def test_tara_xls_statement_parsed():
    buf = io.BytesIO()
    rows = [
        ["Agent Commission"] + [None] * 15,
        ["Accounts paid 5/1/2025 through 5/31/2025"] + [None] * 15,
        [None] * 16,
        ["Rec Type", "Pmt Flag", "Cust ID", "ESI ID", "Cust Status", "Name", "Address",
         "Bill Date", "Bill No", "Start Date", "End Date", "Due Date", "Posted Date",
         "KWH", "Comm Rate", "Comm Due"],
        ["Paid", "+", "2205200361", "1008901001157325183100", "A", "CHAN'S TAILOR",
         "7818 LOUETTA RD, SPRING, TX 77379", "2025-05-03", "9250516564",
         "2025-04-01", "2025-05-01", "2025-05-19", "2025-05-03", "566", "0.007", "3.962"],
        ["Unpaid", "-", "2205200361", "1008901001157325183100", "A", "CHAN'S TAILOR",
         "7818 LOUETTA RD, SPRING, TX 77379", "2025-06-04", "9250611728",
         "2025-05-01", "2025-06-02", "2025-06-20", None, "1069", "0.007", "0"],
    ]
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="Agent Commission for Export", index=False, header=False)
    res = detect_and_parse(buf.getvalue(), "Tara_Saigon Power LLC _20250501-20250531.xlsx")
    assert res and res["provider_group"] == "Tara Energy"
    assert res["row_count"] == 1  # Unpaid placeholder rows dropped
    r = res["rows"][0]
    assert r["esiid"] == "1008901001157325183100"
    assert r["amount"] == 3.962 and r["rate"] == 0.007
    assert r["statement_label"] == "2025-05"


def test_reliant_usage_report_parsed():
    data = xlsx({
        "Summary": [{"Broker": "Saigon Power LLC", "Payment Month": "2026-05-01",
                     "Commission Total": "6.33"}],
        "Sites List": [{"ESID": "1008901020901313930117"}],
        "Usage Report": [
            {"Broker": "Saigon Power LLC", "BP: Org. Name 1": "K & K NAILS",
             "ESID": "1008901020901313930117", "Invoice number": "322001742337",
             "Strt Bill Per (Cons)": "2026-03-06", "End Bill Per (Cons)": "2026-04-06",
             "Posting date": "2026-04-08", "Energy Price (Char)": "0.095",
             "Quant. Energy": "1055", "Broker Fee": "0.006", "Payment": "6.33",
             "Customer Name": "K & K NAILS", "Start Date": "2022-06-01", "End Date": "2027-05-31"},
        ],
    })
    res = detect_and_parse(data, "Saigon Power LLC_CombMthlyPmts_2026.05.xlsx")
    assert res and res["provider_group"] == "Reliant Energy"
    r = res["rows"][0]
    assert r["amount"] == 6.33 and r["rate"] == 0.006
    assert r["statement_label"] == "2026-05"


def test_apge_residual_parsed():
    buf = io.BytesIO()
    rows = [[None] * 13 for _ in range(9)]
    rows[2][6] = "APG&E Commissions Report"
    rows[2][10] = "Report Date: May 15, 2026"
    rows.append(["LDC Account #", "Service Address", "Customer Name", "Period Start",
                 "Period End", "Payable Date", "State", "Agent Code",
                 "Commission Rate \n$/MWh", "Energy Usage\nMWh", "Payment", None, None])
    rows.append(["1008901010187496905100", "11633 Katy Fwy Houston, TX", "OMNLIFE USA",
                 "2026-02-13", "2026-03-16", "2026-04-09", "TX", "00758",
                 "6.97", "1.513", "10.55", None, None])
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name="RESIDUAL", index=False, header=False)
        pd.DataFrame([[None]]).to_excel(w, sheet_name="SUMMARY", index=False, header=False)
    res = detect_and_parse(buf.getvalue(), "Saigon Power LLC_2026_05_15.xlsx")
    assert res and res["provider_group"] == "APG&E"
    r = res["rows"][0]
    assert r["amount"] == 10.55
    assert r["rate"] == 0.00697       # $/MWh converted to $/kWh
    assert r["usage_kwh"] == 1513.0   # MWh converted to kWh
    assert r["statement_label"] == "2026-05"


def test_hudson_premise_detail_parsed():
    buf = io.BytesIO()
    letterhead = [[None] * 25 for _ in range(9)]
    letterhead[0][2] = "Statement To:"
    letterhead[1][2] = "SAIGON POWER LLC"
    letterhead[4][2] = "6/25/2026"
    letterhead[5][2] = "Statement #:"
    letterhead[6][2] = "223514"
    premise_header = ["Division", "Customer Name", "Document #", "Premise", "Term",
                      "Term Start Date", "Term End Date", "Drop?", "Payment Plan",
                      "Plan Type", "Utility", "Commodity Type", "Fee",
                      "Forecasted Term Usage", "Payment To Date", "Forecasted Usage",
                      "Actuals Usage", "Statement Usage", "Forecasted Payment",
                      "Actuals Payment", "Statement Payment", "Transaction Type",
                      "Document Payment Total", "Customer Subtotal", "Total"]
    premise_rows = [
        ["HES_ERCOT_TX", "Adore Nail Studio", "H24051648482171", "1008901007185252692100",
         "24", "2024-05-29", "2026-05-16", None, "Monthly Actuals Payment", "Actuals",
         "CNTP", "POWER", "0.009", "24839", "443.95", "0", "2820", "2820", "0",
         "25.38", "25.38", "Payment", None, None, None],
        ["HES_ERCOT_TX", "AVA NAILS", "H25030650968058", "1008901006901086310116",
         "60", "2025-03-17", "2030-03-17", "Y", "Monthly Actuals Payment", "Actuals",
         "CNTP", "POWER", "0.01", "134027", "248.53", "0", "1644", "1644", "0",
         "16.44", "16.44", "Payment", None, None, None],
    ]
    txn_header = ["Division", "Customer Name", "Document #", "Premise", "Term",
                  "Term Start Date", "Term End Date", "Drop?", "Payment Plan",
                  "Plan Type", "Utility", "Commodity Type", "Fee", "Forecasted Usage",
                  "Actual Usage", "Statement Usage", "Forecasted Payment",
                  "Actuals Payment", "Statement Payment", "Period Start Date",
                  "Period End Date", "Transaction Type", "Document Payment Total",
                  "Customer Subtotal", "Total"]
    txn_rows = [
        ["HES_ERCOT_TX", "Adore Nail Studio", "H24051648482171", "1008901007185252692100",
         "24", "2024-05-29", "2026-05-16", None, "Monthly Actuals Payment", "Actuals",
         "CNTP", "POWER", "0.009", "0", "1400", "1400", "0", "12.69", "12.69",
         "2026-04-08", "2026-04-30", "Actuals", None, None, None],
        ["HES_ERCOT_TX", "Adore Nail Studio", "H24051648482171", "1008901007185252692100",
         "24", "2024-05-29", "2026-05-16", None, "Monthly Actuals Payment", "Actuals",
         "CNTP", "POWER", "0.009", "0", "1420", "1420", "0", "12.69", "12.69",
         "2026-05-01", "2026-05-16", "Actuals", None, None, None],
    ]
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame([[None]]).to_excel(w, sheet_name="Summary", index=False, header=False)
        pd.DataFrame(letterhead + [premise_header] + premise_rows).to_excel(
            w, sheet_name="Premise Detail", index=False, header=False)
        pd.DataFrame(letterhead + [txn_header] + txn_rows).to_excel(
            w, sheet_name="Transaction Detail", index=False, header=False)
    res = detect_and_parse(buf.getvalue(), "SAIGON POWER LLC_223514_06-25-26.xlsx")
    assert res and res["provider_group"] == "Hudson Energy"
    assert res["supplier"]["code"] == "HUDSON"
    assert res["row_count"] == 2
    r = res["rows"][0]
    assert r["esiid"] == "1008901007185252692100"
    assert r["amount"] == 25.38 and r["rate"] == 0.009 and r["usage_kwh"] == 2820.0
    assert r["statement_label"] == "2026-06"          # letterhead date, not filename
    assert r["service_start"] == "2026-04-08"         # merged from Transaction Detail
    assert r["service_end"] == "2026-05-16"
    assert res["rows"][1]["provider_status"].startswith("drop")
    assert [g["esiid"] for g in res["going_final"]] == ["1008901006901086310116"]
