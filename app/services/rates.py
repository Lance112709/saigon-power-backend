"""Energy-rate normalization: every rate the CRM stores is in $/kWh.

Agents and provider spreadsheets often quote rates in cents (8.5, 13.05).
Before 2026-09-23 those landed in the DB as-is next to correct values
(0.085, 0.1305); a one-time backfill fixed the history, and this guard keeps
new writes consistent.
"""
from fastapi import HTTPException

CENTS_THRESHOLD = 1.0      # nothing legitimate is $1/kWh or more → treat as cents
MIN_RATE = 0.03            # below this it is almost certainly an adder typed in the rate box
MAX_RATE = 0.30            # Texas retail rates top out well under 30¢/kWh


def normalize_rate(value, *, strict: bool = True, field: str = "Energy rate"):
    """Return the rate in $/kWh, converting cents-style input.

        7.8    -> 0.078
        13.05  -> 0.1305
        0.078  -> 0.078   (unchanged)
        None/''-> None

    strict=True  (interactive forms): reject values that are still nonsense
                 after conversion (36 -> 0.36, -12, 0) with HTTP 400.
    strict=False (bulk imports): convert what can be converted, pass the rest
                 through untouched so a whole file is never rejected for one cell.
    """
    if value in (None, "", "null"):
        return None
    try:
        v = float(value)
    except (ValueError, TypeError):
        if strict:
            raise HTTPException(status_code=400, detail=f"{field} '{value}' is not a number")
        return None
    if v >= CENTS_THRESHOLD:
        v = v / 100.0
    if strict and not (MIN_RATE <= v < MAX_RATE):
        raise HTTPException(
            status_code=400,
            detail=f"{field} {value} is not a valid $/kWh rate (enter dollars per kWh, e.g. 0.085 for 8.5¢)",
        )
    return round(v, 6)
