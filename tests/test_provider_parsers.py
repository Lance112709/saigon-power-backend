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
    data = xlsx({"Sheet1": [
        {"Premise ID": "1008901023900175571", "Usage": "661",
         "Affinity Rate in ($)": "0.008", "Affinity Amount": "5.29",
         "Premise Address": "3532 OMEARA DR", "Premise City": "HOUSTON", "Premise Zip": "77025",
         "Cust First Name": "Hoang", "Cust Last Name": "Nguyen",
         "Cust Status": "Inactive", "Start Date": "2026-02-18", "End Date": "2026-03-19"},
    ]})
    res = detect_and_parse(data, "Affinity Report_Saigon_Power_LLC_MAY 2026.xlsx")
    assert res and res["provider_group"] == "Budget Power"
    assert len(res["going_final"]) == 1  # Inactive flagged for follow-up


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
