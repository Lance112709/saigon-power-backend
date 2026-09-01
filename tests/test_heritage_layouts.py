"""Heritage Power: Abel renames the Affinity export columns most months.
Every layout seen Nov 2025 – Jul 2026 must fingerprint and yield the same
normalized row (rate = SGP margin in $/kWh)."""
import io
import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.file_parser.provider_parsers import detect_and_parse

ESI = "1008901023812452220102"
BASE = {"Premise ID": ESI, "Premise Address": "1 Main St", "Premise City": "Houston", "Premise Zip": "77036",
        "Cust First Name": "Yenni", "Cust Last Name": "Nguyen", "Cust Company Name": "",
        "Cust Status": "Active", "Start Date": "2026-05-01", "End Date": "2026-05-31",
        "Bill Paid Date": "2026-06-12"}

LAYOUTS = {
    # classic (Jun 2026 and Nov 2025–Apr 2026): gross rate + Abel's mils, SGP money = Commissions Amount
    "classic": {**BASE, "kWh": "1000", "Affinity Rate in ($)": "0.009", "Abel's Mils": "0.002",
                "Commissions Amount": "7.00", "Abel's Commission Amount": "2.00"},
    # Mar 2026: explicit Saigon Mils + Abel's Mils, SGP money = Saigon Commissions Amount
    "mar26": {**BASE, "kWh": "1000", "Affinity Rate in ($)": "0.009", "Saigon Mils": "0.007", "Abel's Mils": "0.002",
              "Abel's Commission": "2.00", "Saigon Commissions Amount": "7.00"},
    # May 2026: rates typed in mils, SGP money = Saigon Power Commissions Amount
    "may26": {**BASE, "kWh": "1000", "SG Affinity Rate in ($) Mils": "7", "Saigon Power Commissions Amount": "7.00",
              "Abel's Affinity Rate in ($) Mils": "2", "Abel's Commission Amount": "2.00"},
    # Jul 2026 (.xlsx): 'Abe's Mils', no kWh column (Metered Points), SGP money = Saigon's Commissions Amount
    "jul26": {**BASE, "Metered Points": "1000", "Affinity Rate in ($)": "0.009", "Abe's Mils": "0.002",
              "Abel's Commission Amount": "2.00", "Saigon's Commissions Amount": "7.00"},
}


def _xlsx(row: dict) -> bytes:
    buf = io.BytesIO()
    pd.DataFrame([row]).to_excel(buf, index=False, sheet_name="Saigon Power's Commissions")
    return buf.getvalue()


@pytest.mark.parametrize("name", list(LAYOUTS))
def test_every_heritage_layout_fingerprints(name):
    parsed = detect_and_parse(_xlsx(LAYOUTS[name]), f"SGP Residual Commissions - {name}.xlsx")
    assert parsed is not None, f"{name} layout not recognized"
    assert parsed["provider_group"] == "Heritage Power"
    (r,) = parsed["rows"]
    assert r["esiid"] == ESI
    assert r["amount"] == 7.0
    assert r["usage_kwh"] == 1000.0
    assert r["rate"] == pytest.approx(0.007)
    assert r["statement_label"] == "2026-06"
    assert r["provider_status"] == "Active"


def test_budget_affinity_export_is_not_heritage():
    row = {**BASE, "kWh": "1000", "Affinity Rate in ($)": "0.007", "Affinity Amount": "7.00"}
    parsed = detect_and_parse(_xlsx(row), "Affinity Report June.xlsx")
    assert parsed is None or parsed["provider_group"] != "Heritage Power"
