import pandas as pd
import hashlib
from typing import Optional

def parse_excel(file_bytes: bytes, sheet_name: Optional[str] = None) -> dict:
    import io
    buf = io.BytesIO(file_bytes)
    xl = pd.ExcelFile(buf)
    sheets = xl.sheet_names

    # Use first non-empty sheet or specified sheet
    target = sheet_name or sheets[0]
    if "Residuals" in sheets:
        target = "Residuals"

    df = pd.read_excel(buf, sheet_name=target)
    df = df.dropna(how="all")

    headers = list(df.columns)
    sample_rows = df.head(3).fillna("").astype(str).to_dict(orient="records")
    all_rows = df.fillna("").astype(str).to_dict(orient="records")

    file_hash = hashlib.sha256(file_bytes).hexdigest()

    return {
        "file_hash": file_hash,
        "sheet_used": target,
        "all_sheets": sheets,
        "headers": headers,
        "sample_rows": sample_rows,
        "all_rows": all_rows,
        "row_count": len(all_rows)
    }

def parse_csv(file_bytes: bytes) -> dict:
    import io
    import hashlib
    buf = io.StringIO(file_bytes.decode("utf-8", errors="replace"))
    df = pd.read_csv(buf)
    df = df.dropna(how="all")

    headers = list(df.columns)
    sample_rows = df.head(3).fillna("").astype(str).to_dict(orient="records")
    all_rows = df.fillna("").astype(str).to_dict(orient="records")
    file_hash = hashlib.sha256(file_bytes).hexdigest()

    return {
        "file_hash": file_hash,
        "headers": headers,
        "sample_rows": sample_rows,
        "all_rows": all_rows,
        "row_count": len(all_rows)
    }
