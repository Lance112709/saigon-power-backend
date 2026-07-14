from app.services.merge_vars import clean_email, lead_merge_vars, crm_customer_merge_vars, fmt_date


def test_clean_email_single():
    assert clean_email("john@gmail.com") == "john@gmail.com"


def test_clean_email_multi_prefers_real_customer_over_broker_placeholder():
    # Imported records pack several addresses into one field; reach the customer.
    assert clean_email("power@saigonllc.com;acepham96@gmail.com") == "acepham96@gmail.com"
    assert clean_email("info@saigonpowertx.com, real@yahoo.com") == "real@yahoo.com"


def test_clean_email_all_broker_falls_back_to_first():
    assert clean_email("a@saigonllc.com;b@saigonllc.com") == "a@saigonllc.com"


def test_clean_email_strips_display_name_and_junk():
    assert clean_email("  Bob <bob@x.com> ") == "bob@x.com"
    assert clean_email("notanemail") == ""
    assert clean_email("") == ""
    assert clean_email(None) == ""


def test_merge_vars_email_is_cleaned():
    v = crm_customer_merge_vars(
        {"full_name": "Ace Pham", "email": "power@saigonllc.com;acepham96@gmail.com"}, [])
    assert v["email"] == "acepham96@gmail.com"
    lv = lead_merge_vars({"first_name": "A", "last_name": "B", "email": "x@saigonllc.com;y@gmail.com"}, [])
    assert lv["email"] == "y@gmail.com"


def test_fmt_date_human_readable():
    assert fmt_date("2026-09-05") == "September 5, 2026"
    assert fmt_date("") == ""
