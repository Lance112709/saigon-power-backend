"""Dispatch template/auth rendering tests."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.enrollment_dispatch import render_template, build_request

ENR = {"id": "abc", "first_name": "Lan", "last_name": "Vo", "phone": "8325551234",
       "esiid": "1008901000000000000001", "rate": 12.5, "term_months": 12,
       "requested_start_date": "2026-08-01", "plan_name": "Saver 12",
       "service_zip": "77001", "email": None}


def test_placeholders_substituted_recursively():
    tpl = {"customer": {"name": "{{first_name}} {{last_name}}", "zip": "{{service_zip}}"},
           "meters": [{"esiid": "{{esiid}}"}], "note": "plan {{plan_name}} for {{term_months}}mo"}
    out = render_template(tpl, ENR)
    assert out["customer"]["name"] == "Lan Vo"
    assert out["meters"][0]["esiid"] == "1008901000000000000001"
    assert out["note"] == "plan Saver 12 for 12mo"


def test_bare_placeholder_keeps_native_type():
    out = render_template({"rate": "{{rate}}", "term": "{{term_months}}"}, ENR)
    assert out["rate"] == 12.5 and isinstance(out["rate"], float)
    assert out["term"] == 12 and isinstance(out["term"], int)


def test_missing_and_none_fields():
    out = render_template({"email": "e:{{email}}", "x": "{{nope}}"}, ENR)
    assert out["email"] == "e:"       # embedded in a string -> empty string
    assert out["x"] is None           # bare placeholder -> native null


def test_static_values_pass_through():
    out = render_template({"broker_id": "319010", "channel": "WEB", "n": 5}, ENR)
    assert out == {"broker_id": "319010", "channel": "WEB", "n": 5}


def test_auth_bearer():
    req = build_request({"field_mapping": {}, "auth_type": "bearer",
                         "auth_credentials": {"token": "tok123"},
                         "endpoint_url": "https://x/y"}, ENR)
    assert req["headers"]["Authorization"] == "Bearer tok123"


def test_auth_api_key_header():
    req = build_request({"field_mapping": {}, "auth_type": "api_key_header",
                         "auth_credentials": {"header_name": "X-Api-Key", "api_key": "k"},
                         "endpoint_url": "https://x", "extra_headers": {"X-Broker": "319010"}}, ENR)
    assert req["headers"]["X-Api-Key"] == "k"
    assert req["headers"]["X-Broker"] == "319010"


def test_auth_basic():
    req = build_request({"field_mapping": {}, "auth_type": "basic",
                         "auth_credentials": {"username": "u", "password": "p"},
                         "endpoint_url": "https://x"}, ENR)
    assert req["auth"] == ("u", "p")
