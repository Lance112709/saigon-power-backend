"""Commercial pricing matrix parsers.

One entry per provider in PRICING_PARSERS; each takes the raw file bytes and
returns normalized long-format rows. Parsers locate their table by header
fingerprint instead of fixed sheet names/positions, so minor daily layout
changes (renamed sheets, moved columns, added terms) don't break imports.

Phase 2 note: the email-ingest pipeline can call parse_pricing_file() on an
attachment exactly like the manual upload endpoint does — nothing here knows
where the bytes came from.
"""
import hashlib
import io
import re
from datetime import date, datetime
from typing import Optional

import pandas as pd

# header synonyms -> canonical field, per column
NRG_HEADER_MAP = {
    "start_date": "start_month", "startdate": "start_month", "start month": "start_month",
    "start date": "start_month",
    "productname": "product", "product": "product", "product name": "product",
    "dc": "utility", "utility": "utility", "tdsp": "utility",
    "load_profile": "load_profile", "loadprofile": "load_profile", "load profile": "load_profile",
    "congestionzone": "zone", "zone": "zone", "congestion zone": "zone", "load zone": "zone",
    "usagegroupkwh": "usage_tier", "usagekwh": "usage_tier", "usage group": "usage_tier",
    "usage": "usage_tier",
}
REQUIRED_FIELDS = {"product", "utility", "zone"}


def _norm_header(v) -> str:
    return re.sub(r"[^a-z ]", "", str(v or "").strip().lower().replace("_", " ")).strip()


def _term_from_header(v) -> Optional[int]:
    """Term columns appear as bare numbers (1..60) or 'Term - 12' variants."""
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        if pd.isna(v):
            return None
        t = int(v)
        return t if 1 <= t <= 120 and float(v) == t else None
    m = re.match(r"^\s*term\s*[-:]?\s*(\d{1,3})\s*(mo(nths?)?)?\s*$", str(v or ""), re.I)
    if m:
        t = int(m.group(1))
        return t if 1 <= t <= 120 else None
    m = re.match(r"^\s*(\d{1,3})\s*mo(nths?)?\s*$", str(v or ""), re.I)
    if m:
        return int(m.group(1))
    return None


def _find_header_row(df: pd.DataFrame) -> Optional[int]:
    for i in range(min(8, len(df))):
        fields = {NRG_HEADER_MAP.get(_norm_header(v)) for v in df.iloc[i]}
        if REQUIRED_FIELDS.issubset(fields - {None}):
            return i
    return None


def _valid_until(xl: pd.ExcelFile) -> Optional[datetime]:
    """NRG stamps 'Prices Valid Until 5pm  09 Jul 26' somewhere in the book."""
    pat = re.compile(r"valid\s+until\s+(\d{1,2})\s*(am|pm)?\s+(\d{1,2})\s+([A-Za-z]{3})\s+(\d{2,4})", re.I)
    for sheet in xl.sheet_names:
        try:
            head = pd.read_excel(xl, sheet_name=sheet, header=None, nrows=10)
        except Exception:
            continue
        for v in head.values.ravel():
            m = pat.search(str(v or ""))
            if m:
                hour, ampm, day, mon, year = m.groups()
                y = int(year) + (2000 if int(year) < 100 else 0)
                h = int(hour) % 12 + (12 if (ampm or "").lower() == "pm" else 0)
                try:
                    mo = datetime.strptime(mon[:3], "%b").month
                    return datetime(y, mo, int(day), h, 0)
                except ValueError:
                    continue
    return None


def _parse_nrg(file_bytes: bytes, filename: str) -> dict:
    xl = pd.ExcelFile(io.BytesIO(file_bytes))
    best = None  # (rate_cell_count, rows, warnings)
    warnings: list = []

    for sheet in xl.sheet_names:
        try:
            raw = pd.read_excel(xl, sheet_name=sheet, header=None)
        except Exception:
            continue
        if raw.empty or len(raw) < 2:
            continue
        hi = _find_header_row(raw)
        if hi is None:
            continue
        header = list(raw.iloc[hi])
        fields, terms = {}, {}
        for col, v in enumerate(header):
            canon = NRG_HEADER_MAP.get(_norm_header(v))
            if canon and canon not in fields:
                fields[canon] = col
                continue
            t = _term_from_header(v)
            if t is not None:
                terms[col] = t
        if not terms:
            continue

        body = raw.iloc[hi + 1:]
        rows, cells = [], 0
        for _, r in body.iterrows():
            base = {}
            for canon, col in fields.items():
                val = r.iloc[col]
                if canon == "start_month":
                    try:
                        base[canon] = pd.to_datetime(val).strftime("%Y-%m")
                    except Exception:
                        base[canon] = None
                else:
                    s = str(val).strip()
                    base[canon] = None if s.lower() in ("nan", "none", "") else s
            if not base.get("utility") or not base.get("zone"):
                continue
            for col, term in terms.items():
                val = r.iloc[col]
                try:
                    rate = float(val)
                except (TypeError, ValueError):
                    continue
                if pd.isna(rate) or not (0.001 <= rate <= 1.0):
                    continue
                cells += 1
                rows.append({**base, "term": term, "rate": round(rate, 6)})
        if rows and (best is None or cells > best[0]):
            best = (cells, rows, f"sheet '{sheet}': {cells} rates, {len(terms)} term columns")

    if best is None:
        raise ValueError("No pricing table found — expected columns like "
                         "START_DATE / PRODUCTNAME / DC / CONGESTIONZONE plus numbered term columns.")
    cells, rows, note = best
    warnings.append(note)

    valid_until = _valid_until(xl)
    dims = {
        "utilities": sorted({r["utility"] for r in rows if r.get("utility")}),
        "zones": sorted({r["zone"] for r in rows if r.get("zone")}),
        "products": sorted({r["product"] for r in rows if r.get("product")}),
        "terms": sorted({r["term"] for r in rows}),
        "usage_tiers": sorted({r["usage_tier"] for r in rows if r.get("usage_tier")}),
        "start_months": sorted({r["start_month"] for r in rows if r.get("start_month")}),
    }
    return {
        "rows": rows,
        "row_count": len(rows),
        "effective_date": date.today().isoformat(),
        "expiration_at": valid_until.isoformat() if valid_until else None,
        "warnings": warnings,
        "dims": dims,
        "file_hash": hashlib.sha256(file_bytes).hexdigest(),
    }


PRICING_PARSERS = {
    "NRG": _parse_nrg,
}


def parse_pricing_file(provider_code: str, file_bytes: bytes, filename: str) -> dict:
    parser = PRICING_PARSERS.get((provider_code or "").upper())
    if parser is None:
        raise ValueError(f"No pricing parser for provider '{provider_code}' yet.")
    ext = (filename or "").rsplit(".", 1)[-1].lower()
    if ext == "csv":
        # normalize CSV to a single-sheet workbook so the same parser runs
        df = pd.read_csv(io.BytesIO(file_bytes), header=None, dtype=str)
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine="openpyxl") as w:
            df.to_excel(w, index=False, header=False)
        file_bytes = buf.getvalue()
    return parser(file_bytes, filename)
