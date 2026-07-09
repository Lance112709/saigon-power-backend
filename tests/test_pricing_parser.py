"""Pricing parser tests: header resilience, term detection, rate bounds."""
import io
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.pricing_parser import _term_from_header, parse_pricing_file


def _book(rows, sheet="matrix prices_all"):
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        pd.DataFrame(rows).to_excel(w, sheet_name=sheet, index=False, header=False)
    return buf.getvalue()


def test_term_header_variants():
    assert _term_from_header(12) == 12
    assert _term_from_header(12.0) == 12
    assert _term_from_header("Term - 24") == 24
    assert _term_from_header("36 mo") == 36
    assert _term_from_header("START_DATE") is None
    assert _term_from_header(float("nan")) is None
    assert _term_from_header(2026) is None  # a year is not a term


def test_nrg_matrix_parsed_and_melted():
    data = _book([
        ["START_DATE", "PRODUCTNAME", "DC", "LOAD_PROFILE", "CONGESTIONZONE", "USAGEGROUPKWH", 6, 12],
        ["2026-07-01", "SMALL FIXED PRICE", "CNP", "BUSLOLF", "HOUSTON", "0-300,000", 0.0543, 0.05325],
        ["2026-07-01", "SMALL FIXED PRICE", "ONC", "BUSLOLF", "NORTH", "0-300,000", None, 0.0601],
    ])
    r = parse_pricing_file("NRG", data, "NRG_Matrix_Price_62.xlsm")
    assert r["row_count"] == 3
    first = r["rows"][0]
    assert first["utility"] == "CNP" and first["zone"] == "HOUSTON"
    assert first["term"] == 6 and first["rate"] == 0.0543
    assert first["start_month"] == "2026-07"
    assert r["dims"]["utilities"] == ["CNP", "ONC"]
    assert r["dims"]["terms"] == [6, 12]


def test_layout_shift_tolerated():
    # renamed sheet, shuffled columns, 'Term - N' headers, junk row above header
    data = _book([
        [None, "daily pricing", None, None, None, None],
        ["CONGESTIONZONE", "Term - 12", "DC", "PRODUCTNAME", "Term - 24", "START_DATE"],
        ["WEST", 0.061, "ONC", "FIXED PRICE BUNDLED", 0.0655, "2026-08-01"],
    ], sheet="new prices tab")
    r = parse_pricing_file("NRG", data, "renamed.xlsx")
    assert r["row_count"] == 2
    assert {x["term"] for x in r["rows"]} == {12, 24}


def test_unparseable_file_raises():
    data = _book([["hello", "world"], [1, 2]])
    try:
        parse_pricing_file("NRG", data, "junk.xlsx")
        assert False, "should raise"
    except ValueError as e:
        assert "No pricing table" in str(e)
