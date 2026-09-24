import pytest
from fastapi import HTTPException
from app.services.rates import normalize_rate


@pytest.mark.parametrize("raw,expected", [
    (7.8, 0.078), ("13.05", 0.1305), (8.125, 0.08125), (14.9, 0.149),
    (0.078, 0.078), ("0.1305", 0.1305), (0.24, 0.24), (0.039, 0.039),
    (None, None), ("", None), ("null", None),
])
def test_normalize_rate_converts_cents_and_keeps_dollars(raw, expected):
    assert normalize_rate(raw) == expected


@pytest.mark.parametrize("raw", [36, 1.0, -12, 0, 0.0, 0.007, 0.0264, 0.36, "abc"])
def test_strict_rejects_nonsense(raw):
    with pytest.raises(HTTPException) as e:
        normalize_rate(raw)
    assert e.value.status_code == 400


def test_non_strict_converts_but_never_raises():
    assert normalize_rate(8.5, strict=False) == 0.085
    assert normalize_rate(36, strict=False) == 0.36      # passed through, flagged later by reports
    assert normalize_rate(0.007, strict=False) == 0.007
    assert normalize_rate(1.5, strict=False) == 0.015
    assert normalize_rate("abc", strict=False) is None
