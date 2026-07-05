"""Shared deal helpers."""
import re


def is_month_to_month(*values) -> bool:
    """True when any plan/term field marks the deal month-to-month.

    Catches every spelling in the data: "Month-Month", "Month to Month",
    "Month-to-Month", "IH Month to Month", and a contract term of "0".
    Month-to-month deals have no contract end — they never belong on
    renewal or expiring lists.
    """
    for v in values:
        s = re.sub(r"[^a-z0-9]+", " ", str(v or "").lower()).strip()
        if not s:
            continue
        if s == "0" or s == "0 months":
            return True
        if "month to month" in s or "month month" in s or s in ("m2m", "mtm"):
            return True
    return False
