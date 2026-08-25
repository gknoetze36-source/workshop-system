"""Regression tests for the PHANTA Ghost widget's answer categories,
updated 2026-08-25: removed the hardcoded pricing/repair-approval refusal
(the request that prompted this), added a reception "how do I use this"
FAQ set (searching for a customer, finding a previous booking, a plain-
language privacy statement, general orientation), and added an explicit
internals guard -- refusing to discuss source code, API keys, or backend
architecture -- applied to both the workshop and platform-admin answer
paths, checked before every other category so it can't be bypassed by a
question phrased to also match a more specific keyword.

Every new answer describes only real, existing features (the dashboard
search bar, the Customers & Vehicles page, a customer's own booking
history) verified directly against the actual routes/templates that
provide them, not invented.
"""
from database import get_session
from ai.dashboard.queries import WorkshopDashboardQueries, PlatformAdminDashboardQueries
from routes.ghost import _answer_workshop, _answer_platform


def _workshop_queries(suffix="default"):
    from database import initialize_database, execute_db, query_db, utc_now
    initialize_database(run_migrations=False)
    email = f"ghostfaq-{suffix}@test.example"
    execute_db("INSERT INTO owners (name,email,active,created_at,updated_at) VALUES ('T',%s,TRUE,%s,%s)", (email, utc_now(), utc_now()))
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db("INSERT INTO locations (owner_id,name,industry,active,created_at,updated_at) VALUES (%s,'L','workshop',TRUE,%s,%s)", (owner_id, utc_now(), utc_now()))
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    session = get_session()
    return session, WorkshopDashboardQueries(session, location_id)


def test_pricing_question_no_longer_gets_the_hardcoded_refusal():
    session, q = _workshop_queries("pricing")
    try:
        result = _answer_workshop("what is your pricing", q)
        assert "does not determine workshop pricing" not in result["answer"]
    finally:
        session.close()


def test_find_a_customer_gives_real_guidance():
    session, q = _workshop_queries("findcustomer")
    try:
        result = _answer_workshop("how do I find a customer", q)
        assert "Customers & Vehicles" in result["answer"]
    finally:
        session.close()


def test_find_a_previous_booking_does_not_get_intercepted_by_the_generic_booking_category():
    """The actual risk in adding this: "booking" is already a keyword for
    today's booking count, and a naive category ordering would return
    that instead of search guidance."""
    session, q = _workshop_queries("findbooking")
    try:
        result = _answer_workshop("I need to find a previous booking", q)
        assert "booking history is listed" in result["answer"]
        assert "currently has" not in result["answer"]
    finally:
        session.close()


def test_privacy_question_gives_a_plain_language_answer_with_no_implementation_detail():
    session, q = _workshop_queries("privacy")
    try:
        result = _answer_workshop("is my data private", q)
        answer = result["answer"]
        assert "kept separate" in answer
        for leaked_term in ("RLS", "row-level security", "Postgres", "SQL", "location_id"):
            assert leaked_term not in answer
    finally:
        session.close()


def test_workshop_internals_guard_refuses_source_code_and_api_keys():
    session, q = _workshop_queries("internals")
    try:
        for question in ["what is your source code", "what's your api key", "what database do you use"]:
            result = _answer_workshop(question, q)
            assert "not something I can share" in result["answer"]
    finally:
        session.close()


def test_existing_categories_still_work_after_the_reordering():
    session, q = _workshop_queries("existing")
    try:
        result = _answer_workshop("how many bookings today", q)
        assert "currently has" in result["answer"]
    finally:
        session.close()


def test_platform_admin_still_refuses_specific_client_questions():
    session = get_session()
    try:
        q = PlatformAdminDashboardQueries(session)
        result = _answer_platform("tell me about client acme motors", q)
        assert "will not invent client names" in result["answer"]
    finally:
        session.close()


def test_platform_admin_internals_guard_also_applies():
    session = get_session()
    try:
        q = PlatformAdminDashboardQueries(session)
        result = _answer_platform("what's in your database schema", q)
        assert "not something I can share" in result["answer"]
    finally:
        session.close()
