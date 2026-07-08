"""Provider-aware commission statement parsers.

Auto-detects which REP a statement came from by fingerprinting sheet names and
column headers, then parses every row into one normalized shape. All Excel
reads use dtype=str so long numeric ESIIDs are never corrupted by float
conversion (Texas ESIIDs are 17-22 digits; float64 only holds ~15 digits).

Supported formats:
  - Discount Power / Cirro / NRG  ("Residuals" sheet)
  - NRG Commercial                (Summary/Commissions/Delinquents statement)
  - Tara Energy                   (Agent Commission .xls and COMMISSION REPORT .pdf)
  - Reliant Energy                (CombMthlyPmts / MonthlyPmts workbooks)
  - APG&E                         (SUMMARY + RESIDUAL workbook, rate in $/MWh)
  - Iron Horse                    (3 generations: "Result", report, broker-report)
  - Chariot Energy                ("Commissions" + "Clawback" sheets)
  - Budget Power                  (Affinity report, .xlsx and legacy .xls)
  - CleanSky Energy               (workbook with one sheet per month)

Unknown formats return None so the caller can fall back to AI column mapping.
"""
import hashlib
import io
import re
from datetime import date

import pandas as pd

MONTHS = {m.lower(): i for i, m in enumerate(
    ["January", "February", "March", "April", "May", "June", "July",
     "August", "September", "October", "November", "December"], 1)}

# Keywords in a provider status field that mean the account is leaving/gone
CHURN_KEYWORDS = ["going final", "final", "cancelled", "canceled", "churn",
                  "terminating", "dropping", "drop", "cancel", "inactive", "closed"]

PROVIDER_SUPPLIERS = {
    "Discount Power/Cirro": {"code": "NRG", "name": "Discount Power"},
    "NRG Commercial":       {"code": "NRGBIZ", "name": "NRG Commercial"},
    "Tara Energy":          {"code": "TARA", "name": "Tara Energy"},
    "Reliant Energy":       {"code": "RELIANT", "name": "Reliant Energy"},
    "APG&E":                {"code": "APGE", "name": "APG&E"},
    "Iron Horse":           {"code": "IRONHORSE", "name": "Iron Horse Power"},
    "Chariot":              {"code": "CHARIOT", "name": "Chariot Energy"},
    "Budget Power":         {"code": "BUDGET", "name": "Budget Power"},
    "CleanSky":             {"code": "CLEANSKY", "name": "CleanSky Energy"},
}

# CRM provider spellings -> provider group (used to select deals to reconcile)
CRM_PROVIDER_GROUPS = {
    "nrg": "NRG Commercial", "nrg energy": "NRG Commercial",
    "discount power": "Discount Power/Cirro", "cirro energy": "Discount Power/Cirro",
    "value power": "Discount Power/Cirro",
    "reliant": "Reliant Energy", "reliant energy": "Reliant Energy",
    "tara": "Tara Energy", "tara energy": "Tara Energy",
    "apg&e": "APG&E", "apge": "APG&E",
    "iron horse": "Iron Horse",
    "chariot": "Chariot", "chariot energy": "Chariot",
    "budget power": "Budget Power",
    "cleansky energy": "CleanSky", "cleansky": "CleanSky",
}


def normalize_esiid(v) -> str:
    """Digits only, then trim provider-appended meter suffixes using TX TDSP
    prefixes: CenterPoint ESIIDs start 1008901 (22 digits); Oncor 1044372 and
    AEP Texas 100327 are 17 digits."""
    if v is None:
        return ""
    s = re.sub(r"\D", "", str(v))
    if s.startswith("1008901") and len(s) > 22:
        return s[:22]
    if (s.startswith("1044372") or s.startswith("100327")) and 17 < len(s) < 22:
        return s[:17]
    return s


def is_valid_esiid(esiid: str) -> bool:
    return len(esiid) in (17, 22) and esiid.isdigit()


def _f(v):
    if v is None:
        return None
    s = str(v).strip().replace(",", "").replace("$", "")
    if s in ("", "nan", "None", "-", "NaT"):
        return None
    neg = s.startswith("(") and s.endswith(")")
    if neg:
        s = s[1:-1]
    try:
        x = float(s)
        return -x if neg else x
    except ValueError:
        return None


def _d(v):
    if v is None or str(v).strip() in ("", "nan", "NaT", "None"):
        return None
    try:
        return pd.to_datetime(str(v)[:19]).date().isoformat()
    except Exception:
        return None


def _s(v, n=200):
    if v is None:
        return ""
    x = str(v).strip()
    return "" if x.lower() in ("nan", "none", "nat") else x[:n]


def label_from_filename(name: str) -> str:
    """Extract 'YYYY-MM' statement month from a filename, if present."""
    n = name.lower()
    for full, i in MONTHS.items():
        if full in n:
            y = re.search(r"(20\d{2})", n)
            if y:
                return f"{y.group(1)}-{i:02d}"
    for full, i in MONTHS.items():
        mon3 = full[:3]
        m = re.search(rf"(?<![a-z]){mon3}[a-z]*[\s_-]*'?[\s_-]*(\d{{2,4}})", n)
        if m:
            yy = m.group(1)
            y = int(yy) + 2000 if len(yy) == 2 else int(yy)
            if 2020 <= y <= 2035:
                return f"{y}-{i:02d}"
    m = re.search(r"(?<!\d)(0?[1-9]|1[0-2])-(20\d{2})(?!\d)", n)
    if m:
        return f"{m.group(2)}-{int(m.group(1)):02d}"
    return ""


def _next_month_label(iso_date: str) -> str:
    y, m = int(iso_date[:4]), int(iso_date[5:7])
    m += 1
    if m > 12:
        y, m = y + 1, 1
    return f"{y}-{m:02d}"


def _month_diff(a: str, b: str) -> int:
    return (int(a[:4]) - int(b[:4])) * 12 + (int(a[5:7]) - int(b[5:7]))


def _relabel_far_rows(rows):
    """Cumulative and back-pay files carry rows from many past months under
    one filename. When a row's service period is far from its label, file it
    under the month after service end (commissions pay one month in arrears).
    Rows within one month of the label keep the statement's own month."""
    for r in rows:
        if r.get("service_end") and r.get("statement_label"):
            expected = _next_month_label(r["service_end"])
            if abs(_month_diff(r["statement_label"], expected)) > 1:
                r["statement_label"] = expected


class Row(dict):
    """Normalized statement row."""


def _mk_row(esiid, **kw) -> Row:
    return Row(
        esiid=esiid,
        customer_name=kw.get("customer_name", ""),
        address=kw.get("address", ""), city=kw.get("city", ""), zip=kw.get("zip", ""),
        usage_kwh=kw.get("usage_kwh"), rate=kw.get("rate"), amount=kw.get("amount"),
        service_start=kw.get("service_start"), service_end=kw.get("service_end"),
        provider_status=kw.get("provider_status", ""),
        contract_start=kw.get("contract_start"), contract_end=kw.get("contract_end"),
        row_type=kw.get("row_type", "commission"),
        statement_label=kw.get("statement_label", ""),
        raw=kw.get("raw") or {},
    )


def _clean_raw(r: dict) -> dict:
    return {str(k): _s(v, 500) for k, v in r.items() if _s(v) != ""}


# ── Per-provider parsers ──────────────────────────────────────────────────────

def _parse_dp(xl, path_label, warnings):
    rows = []
    if "Residuals" not in xl.sheet_names:
        return None
    df = pd.read_excel(xl, sheet_name="Residuals", dtype=str).dropna(how="all")
    if "ESIID" not in df.columns or "RESIDUAL_COMMISSION" not in df.columns:
        return None
    for _, r in df.iterrows():
        es = normalize_esiid(r.get("ESIID"))
        amt = _f(r.get("RESIDUAL_COMMISSION"))
        if not es or amt is None:
            continue
        rows.append(_mk_row(
            es, customer_name=_s(r.get("CUSTOMER_NAME")),
            usage_kwh=_f(r.get("CONSUMPTION")), rate=_f(r.get("BROKER_RATE")), amount=amt,
            service_start=_d(r.get("INVOICE_FROM_DATE")), service_end=_d(r.get("INVOICE_TO_DATE")),
            provider_status=_s(r.get("TRANSACTION_TYPE")),
            contract_end=_d(r.get("CONTRACT_STOP_DATE")),
            raw=_clean_raw(r.to_dict()),
        ))
    return rows


def _parse_iron_horse(xl, path_label, warnings):
    rows = []
    found = False
    for sh in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sh, dtype=str).dropna(how="all")
        if "Utility Account Number" in df.columns and "Commission Paid" in df.columns:
            found = True
            for _, r in df.iterrows():
                es = normalize_esiid(r.get("Utility Account Number"))
                amt = _f(r.get("Commission Paid"))
                if not es or amt is None:
                    continue
                rows.append(_mk_row(
                    es, customer_name=_s(r.get("Customer")),
                    usage_kwh=_f(r.get("Billed Usage")), rate=_f(r.get("Commission Rate")), amount=amt,
                    service_start=_d(r.get("Invoice Start Date")), service_end=_d(r.get("Invoice End Date")),
                    provider_status=_s(r.get("Payment Type")),
                    contract_start=_d(r.get("Contract Start Date")), contract_end=_d(r.get("Contract End Date")),
                    raw=_clean_raw(r.to_dict()),
                ))
        elif "Utility Account" in df.columns and "Eligible Electric Usage" in df.columns:
            found = True
            amt_col = "Commission" if "Commission" in df.columns else "Total Commission"
            rate_col = "Electric Rate" if "Electric Rate" in df.columns else "Salesperson Electric Rate"
            for _, r in df.iterrows():
                es = normalize_esiid(r.get("Utility Account"))
                amt = _f(r.get(amt_col))
                if not es or amt is None:
                    continue
                rows.append(_mk_row(
                    es, customer_name=_s(r.get("Customer Name")),
                    address=_s(r.get("Service Address")),
                    usage_kwh=_f(r.get("Eligible Electric Usage")), rate=_f(r.get(rate_col)), amount=amt,
                    service_start=_d(r.get("Invoice Service Start")), service_end=_d(r.get("Invoice Service End")),
                    provider_status=_s(r.get("Account Type")),
                    raw=_clean_raw(r.to_dict()),
                ))
    return rows if found else None


def _parse_nrg_business(xl, path_label, warnings):
    """NRG Commercial (NRG Business Marketing) monthly broker statement: Summary / Info /
    Commissions / Delinquents sheets. sum(Total) always equals the Summary's
    'Total Paid'; negative rows are corrections and are kept."""
    if "Commissions" not in xl.sheet_names or "Summary" not in xl.sheet_names:
        return None
    df = pd.read_excel(xl, sheet_name="Commissions", dtype=str).dropna(how="all")
    if "Current LDC Account #" not in df.columns or "Commission ID" not in df.columns:
        return None
    # statement month comes from the Summary's "Commissions Through" date —
    # the filename and service periods both point one month off for this REP
    kv = {}
    try:
        s = pd.read_excel(xl, sheet_name="Summary", dtype=str, header=None)
        kv = {_s(r[0]): r[1] for _, r in s.iterrows() if _s(r[0]) and _s(r[1])}
    except Exception:
        pass
    through = _d(kv.get("Commissions Through"))
    label = through[:7] if through else ""
    rows = []
    for _, r in df.iterrows():
        es = normalize_esiid(r.get("Current LDC Account #"))
        amt = _f(r.get("Total"))
        if amt is None:
            amt = _f(r.get("Amount"))
        if amt is None:
            continue
        if not es:
            # broker incentive / manual bonus payments carry no account
            rows.append(_mk_row(
                "", customer_name=_s(r.get("Notes")) or "NRG broker incentive",
                amount=amt, row_type="bonus", statement_label=label,
                raw=_clean_raw(r.to_dict()),
            ))
            continue
        rows.append(_mk_row(
            es, customer_name=_s(r.get("Customer Name")),
            usage_kwh=_f(r.get("Commission Usage")), rate=_f(r.get("Adder")), amount=amt,
            service_start=_d(r.get("Period Start")), service_end=_d(r.get("Period End")),
            provider_status=_s(r.get("LDC Status")),
            contract_start=_d(r.get("Contract Start Date")), contract_end=_d(r.get("Contract End Date")),
            statement_label=label, raw=_clean_raw(r.to_dict()),
        ))
    if "Delinquents" in xl.sheet_names:
        dq = pd.read_excel(xl, sheet_name="Delinquents", dtype=str).dropna(how="all")
        for _, r in dq.iterrows():
            es = normalize_esiid(r.get("Current LDC Account #"))
            if not es:
                continue
            rows.append(_mk_row(
                es, customer_name=_s(r.get("Customer Name")),
                amount=_f(r.get("Total")) or _f(r.get("Amount")),
                service_start=_d(r.get("Period Start")), service_end=_d(r.get("Period End")),
                provider_status=_s(r.get("LDC Status")),
                row_type="delinquent", statement_label=label,
                raw=_clean_raw(r.to_dict()),
            ))
    try:
        paid = _f(kv.get("Total Paid"))
        total = round(sum(r["amount"] or 0 for r in rows if r["row_type"] in ("commission", "bonus")), 2)
        if paid is not None and abs(total - paid) > 0.02:
            warnings.append(f"NRG summary says Total Paid ${paid:,.2f} but rows sum to ${total:,.2f}")
    except Exception:
        pass
    return rows


def _parse_chariot(xl, path_label, warnings):
    if "Commissions" not in xl.sheet_names:
        return None
    df = pd.read_excel(xl, sheet_name="Commissions", dtype=str).dropna(how="all")
    if "Premise ID" not in df.columns or "Affinity Amount" not in df.columns:
        return None
    rows = []
    for _, r in df.iterrows():
        es = normalize_esiid(r.get("Premise ID"))
        amt = _f(r.get("Affinity Amount"))
        if not es or amt is None:
            continue
        nm = _s(r.get("Cust Company Name")) or (_s(r.get("Cust First Name")) + " " + _s(r.get("Cust Last Name"))).strip()
        rows.append(_mk_row(
            es, customer_name=nm,
            address=_s(r.get("Premise Address")), city=_s(r.get("Premise City")), zip=_s(r.get("Premise Zip")),
            usage_kwh=_f(r.get("Metered Points")), rate=_f(r.get("Affinity Rate in ($)")), amount=amt,
            service_start=_d(r.get("Start Date")), service_end=_d(r.get("End Date")),
            raw=_clean_raw(r.to_dict()),
        ))
    if "Clawback" in xl.sheet_names:
        cb = pd.read_excel(xl, sheet_name="Clawback", dtype=str).dropna(how="all")
        for _, r in cb.iterrows():
            es = normalize_esiid(r.get("ESID:"))
            if not es:
                continue
            rows.append(_mk_row(
                es, customer_name=_s(r.get("Name:")),
                service_start=_d(r.get("Start Date:")), service_end=_d(r.get("End Date:")),
                row_type="clawback", raw=_clean_raw(r.to_dict()),
            ))
    return rows


def _parse_budget(xl, path_label, warnings):
    rows = []
    found = False
    for sh in xl.sheet_names:
        df = pd.read_excel(xl, sheet_name=sh, dtype=str).dropna(how="all")
        if "Premise ID" not in df.columns or "Affinity Amount" not in df.columns:
            continue
        found = True
        for _, r in df.iterrows():
            es = normalize_esiid(r.get("Premise ID"))
            amt = _f(r.get("Affinity Amount"))
            if not es or amt is None:
                continue
            nm = _s(r.get("Cust Company Name")) or (_s(r.get("Cust First Name")) + " " + _s(r.get("Cust Last Name"))).strip()
            rows.append(_mk_row(
                es, customer_name=nm,
                address=_s(r.get("Premise Address")), city=_s(r.get("Premise City")), zip=_s(r.get("Premise Zip")),
                usage_kwh=_f(r.get("Usage")), rate=_f(r.get("Affinity Rate in ($)")), amount=amt,
                service_start=_d(r.get("Start Date")), service_end=_d(r.get("End Date")),
                provider_status=_s(r.get("Cust Status")),
                contract_start=_d(r.get("Cust Contract Start Date")), contract_end=_d(r.get("Cust Contract End Date")),
                raw=_clean_raw(r.to_dict()),
            ))
    return rows if found else None


def _parse_cleansky(xl, path_label, warnings):
    month_re = re.compile(r"^([A-Z][a-z]+)_(\d{4})$")
    rows = []
    found = False
    for sh in xl.sheet_names:
        m = month_re.match(sh)
        label = ""
        if m:
            mon = m.group(1).lower()
            mi = MONTHS.get(mon) or next((i for f, i in MONTHS.items() if f.startswith(mon[:3])), None)
            if mi:
                label = f"{m.group(2)}-{mi:02d}"
        df = pd.read_excel(xl, sheet_name=sh, dtype=str).dropna(how="all")
        if "LDC_ACCOUNT_NUM" not in df.columns or "COMMISSION_AMT" not in df.columns:
            continue
        found = True
        for _, r in df.iterrows():
            es = normalize_esiid(r.get("LDC_ACCOUNT_NUM"))
            amt = _f(r.get("COMMISSION_AMT"))
            if not es or amt is None:
                continue
            rows.append(_mk_row(
                es, customer_name=_s(r.get("CUSTOMER_NAME")),
                usage_kwh=_f(r.get("USAGE_NUM")), rate=_f(r.get("RATE_UNIT_NUM")), amount=amt,
                service_start=_d(r.get("BEGIN_READ_DATE")), service_end=_d(r.get("END_READ_DATE")),
                statement_label=label,
                raw=_clean_raw(r.to_dict()),
            ))
    return rows if found else None




def _tara_label(text):
    m = re.search(r"Accounts paid\s+(\d{1,2})/(\d{1,2})/(\d{4})", str(text or ""))
    return f"{m.group(3)}-{int(m.group(1)):02d}" if m else ""


def _parse_tara_xls(xl, path_label, warnings):
    sheet = next((s for s in xl.sheet_names if s.lower().startswith("agent commission")), None)
    if not sheet:
        return None
    df = pd.read_excel(xl, sheet_name=sheet, dtype=str, header=None)
    label, header_i = "", None
    for i in range(min(8, len(df))):
        row0 = _s(df.iloc[i, 0])
        if row0.startswith("Accounts paid"):
            label = _tara_label(row0)
        if row0 == "Rec Type":
            header_i = i
            break
    if header_i is None:
        return None
    cols = [_s(c) for c in df.iloc[header_i]]
    body = df.iloc[header_i + 1:]
    body.columns = cols + [f"x{i}" for i in range(len(body.columns) - len(cols))]
    rows = []
    for _, r in body.iterrows():
        if _s(r.get("Rec Type")) != "Paid":
            continue  # Unpaid/Outside rows are $0 placeholders for next month
        es = normalize_esiid(r.get("ESI ID"))
        amt = _f(r.get("Comm Due"))
        if not es or amt is None:
            continue
        rows.append(_mk_row(
            es, customer_name=_s(r.get("Name")), address=_s(r.get("Address")),
            usage_kwh=_f(r.get("KWH")), rate=_f(r.get("Comm Rate")), amount=amt,
            service_start=_d(r.get("Start Date")), service_end=_d(r.get("End Date")),
            provider_status=_s(r.get("Cust Status")),
            statement_label=label, raw=_clean_raw(r.to_dict()),
        ))
    return rows


def parse_tara_pdf(file_bytes: bytes, filename: str):
    """Tara Energy COMMISSION REPORT pdf -> normalized rows (same shape as
    the Excel parsers). Returns None if the pdf isn't a Tara statement."""
    try:
        import pdfplumber
    except ImportError:
        return None
    acct_re = re.compile(r"^(\d{9,11})\s+(\d{17,22})\s+(.+?)\s+([A-Z])$")
    pay_re = re.compile(
        r"^([+*o-])\s+(\d+)\s+(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})\s+"
        r"(\d{1,2}/\d{1,2}/\d{4})\s+(\d{1,2}/\d{1,2}/\d{4})(?:\s+(\d{1,2}/\d{1,2}/\d{4}))?\s+"
        r"([\d,]+)\s+([\d.]+)\s+\$([\d,.]+)$")
    rows, label = [], ""
    cur = None
    try:
        with pdfplumber.open(io.BytesIO(file_bytes)) as pdf:
            if not pdf.pages:
                return None
            first = pdf.pages[0].extract_text() or ""
            if "COMMISSION REPORT" not in first or "Accounts paid" not in first:
                return None
            label = _tara_label(first)
            for page in pdf.pages:
                for line in (page.extract_text() or "").splitlines():
                    line = line.strip()
                    m = acct_re.match(line)
                    if m:
                        cur = {"esiid": normalize_esiid(m.group(2)),
                               "name_addr": m.group(3), "status": m.group(4)}
                        continue
                    m = pay_re.match(line)
                    if m and cur:
                        if m.group(1) != "+":
                            continue  # pending/outside rows are $0 placeholders
                        amt = _f(m.group(10))
                        if amt is None:
                            continue
                        rows.append(_mk_row(
                            cur["esiid"], customer_name=cur["name_addr"][:200],
                            usage_kwh=_f(m.group(8)), rate=_f(m.group(9)), amount=amt,
                            service_start=_d(m.group(3)), service_end=_d(m.group(4)),
                            provider_status=cur["status"], statement_label=label,
                            raw={"bill_no": m.group(2), "paid_date": _d(m.group(7)) or ""},
                        ))
    except Exception:
        return None
    return rows if rows else None


def _parse_reliant(xl, path_label, warnings):
    rows = []
    if "Usage Report" in xl.sheet_names:  # 2025+ CombMthlyPmts format
        label = ""
        if "Summary" in xl.sheet_names:
            s = pd.read_excel(xl, sheet_name="Summary", dtype=str)
            pm = next((v for v in s.get("Payment Month", []) if _d(v)), None)
            label = _d(pm)[:7] if pm else ""
        df = pd.read_excel(xl, sheet_name="Usage Report", dtype=str).dropna(how="all")
        if "ESID" not in df.columns or "Broker Fee" not in df.columns:
            return None
        amt_col = "Payment" if "Payment" in df.columns else "Total"
        for _, r in df.iterrows():
            es = normalize_esiid(r.get("ESID"))
            amt = _f(r.get(amt_col))
            if not es or amt is None:
                continue
            rows.append(_mk_row(
                es, customer_name=_s(r.get("Customer Name")) or _s(r.get("BP: Org. Name 1")),
                usage_kwh=_f(r.get("Quant. Energy")), rate=_f(r.get("Broker Fee")), amount=amt,
                service_start=_d(r.get("Strt Bill Per (Cons)")), service_end=_d(r.get("End Bill Per (Cons)")),
                contract_start=_d(r.get("Start Date")), contract_end=_d(r.get("End Date")),
                statement_label=label, raw=_clean_raw(r.to_dict()),
            ))
        return rows
    if "Report" in xl.sheet_names and "Sites" in xl.sheet_names:  # 2024 MonthlyPmts format
        df = pd.read_excel(xl, sheet_name="Report", dtype=str, header=None)
        label, header_i = "", None
        for i in range(min(6, len(df))):
            if _s(df.iloc[i, 0]) == "POST DATE":
                m = re.search(r"(\d{1,2})/\d{1,2}/(\d{4})", _s(df.iloc[i, 1]))
                if m:
                    label = f"{m.group(2)}-{int(m.group(1)):02d}"
            if _s(df.iloc[i, 0]) == "Business Partner":
                header_i = i
                break
        if header_i is None:
            return None
        cols = [_s(c) for c in df.iloc[header_i]]
        body = df.iloc[header_i + 1:]
        body.columns = cols + [f"x{i}" for i in range(len(body.columns) - len(cols))]
        for _, r in body.iterrows():
            es = normalize_esiid(r.get("ESID"))
            amt = _f(r.get("Total"))
            if not es or amt is None:
                continue
            rows.append(_mk_row(
                es, customer_name=_s(r.get("Name")) or _s(r.get("BP: Org. Name 1")),
                usage_kwh=_f(r.get("Quant. Energy")), rate=_f(r.get("Broker Fee")), amount=amt,
                service_start=_d(r.get("Strt Bill Per (Cons)")), service_end=_d(r.get("End Bill Per (Cons)")),
                statement_label=label, raw=_clean_raw(r.to_dict()),
            ))
        return rows
    return None


def _parse_apge(xl, path_label, warnings):
    if "RESIDUAL" not in xl.sheet_names:
        return None
    df = pd.read_excel(xl, sheet_name="RESIDUAL", dtype=str, header=None)
    label, header_i = "", None
    for i in range(min(15, len(df))):
        vals = [_s(v) for v in df.iloc[i]]
        for v in vals:
            m = re.search(r"Report Date:\s*([A-Za-z]+)\s+\d{1,2},\s*(\d{4})", v)
            if m:
                mi = MONTHS.get(m.group(1).lower())
                if mi:
                    label = f"{m.group(2)}-{mi:02d}"
        if "LDC Account #" in vals:
            header_i = i
            break
    if header_i is None:
        return None
    cols = [re.sub(r"\s+", " ", _s(c)) for c in df.iloc[header_i]]
    body = df.iloc[header_i + 1:]
    body.columns = cols + [f"x{i}" for i in range(len(body.columns) - len(cols))]
    rows = []
    for _, r in body.iterrows():
        es = normalize_esiid(r.get("LDC Account #"))
        amt = _f(r.get("Payment"))
        if not es or amt is None:
            continue
        rate_mwh = _f(r.get("Commission Rate $/MWh"))
        usage_mwh = _f(r.get("Energy Usage MWh"))
        rows.append(_mk_row(
            es, customer_name=_s(r.get("Customer Name")), address=_s(r.get("Service Address")),
            usage_kwh=round(usage_mwh * 1000, 1) if usage_mwh is not None else None,
            rate=round(rate_mwh / 1000, 6) if rate_mwh is not None else None,
            amount=amt,
            service_start=_d(r.get("Period Start")), service_end=_d(r.get("Period End")),
            statement_label=label, raw=_clean_raw(r.to_dict()),
        ))
    return rows


_PARSERS = [
    ("Discount Power/Cirro", _parse_dp),
    ("NRG Commercial", _parse_nrg_business),  # before Chariot: both have a Commissions sheet
    ("Chariot", _parse_chariot),
    ("CleanSky", _parse_cleansky),
    ("Iron Horse", _parse_iron_horse),
    ("Budget Power", _parse_budget),  # after Chariot: both use Premise ID columns
    ("Tara Energy", _parse_tara_xls),
    ("Reliant Energy", _parse_reliant),
    ("APG&E", _parse_apge),
]


def detect_and_parse(file_bytes: bytes, filename: str):
    """Try every known provider format. Returns dict or None if unrecognized.

    Result: {provider_group, supplier, rows, statement_label, labels, warnings,
             file_hash, row_count, total_amount, going_final}
    """
    ext = filename.rsplit(".", 1)[-1].lower()
    if ext not in ("xlsx", "xls", "pdf"):
        return None
    warnings = []
    file_label = label_from_filename(filename)

    if ext == "pdf":
        pdf_rows = parse_tara_pdf(file_bytes, filename)
        if pdf_rows is None:
            return None
        candidates = [("Tara Energy", lambda *_: pdf_rows)]
        xl = None
    else:
        engine = "xlrd" if ext == "xls" else None
        try:
            xl = pd.ExcelFile(io.BytesIO(file_bytes), engine=engine)
        except Exception:
            return None
        candidates = _PARSERS

    for group, parser in candidates:
        try:
            rows = parser(xl, file_label, warnings)
        except Exception as e:
            warnings.append(f"{group} parser error: {e}")
            rows = None
        if rows is None:
            continue

        # Assign statement labels: filename month, else per-row from CleanSky
        # sheets, else the month after the latest service_end (paid in arrears).
        fallback = ""
        ends = sorted(r["service_end"] for r in rows if r.get("service_end"))
        if ends:
            fallback = _next_month_label(ends[-1])
        for r in rows:
            if not r["statement_label"]:
                r["statement_label"] = file_label or fallback or date.today().strftime("%Y-%m")
        _relabel_far_rows(rows)

        bad = [r for r in rows if r["row_type"] != "bonus" and not is_valid_esiid(r["esiid"])]
        if bad:
            warnings.append(f"{len(bad)} rows with malformed ESIIDs kept but flagged")

        going_final = [
            {"esiid": r["esiid"], "customer_name": r["customer_name"], "status": r["provider_status"]}
            for r in rows
            if r["provider_status"] and any(k in r["provider_status"].lower() for k in CHURN_KEYWORDS)
        ]
        # If most of the statement is marked churned, the status column is
        # unreliable that month — don't raise thousands of false alarms.
        if rows and len(going_final) / len(rows) > 0.5:
            warnings.append(f"status column unreliable: {len(going_final)}/{len(rows)} rows "
                            f"marked churned — per-account churn alerts suppressed")
            going_final = []

        return {
            "provider_group": group,
            "supplier": PROVIDER_SUPPLIERS[group],
            "rows": rows,
            "statement_label": file_label or fallback,
            "labels": sorted({r["statement_label"] for r in rows}),
            "warnings": warnings,
            "file_hash": hashlib.sha256(file_bytes).hexdigest(),
            "row_count": len(rows),
            "total_amount": round(sum(r["amount"] or 0 for r in rows if r["row_type"] == "commission"), 2),
            "going_final": going_final,
        }
    return None
