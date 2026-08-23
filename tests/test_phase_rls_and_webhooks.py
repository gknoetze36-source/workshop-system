from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_sqlalchemy_get_session_binds_request_location():
    text = (ROOT / "database" / "sqlalchemy_session.py").read_text()
    assert "has_request_context" in text
    assert "getattr(g, \"location_id\", None)" in text
    assert "set_location_id(session, location_id)" in text


def test_legacy_connection_binds_request_location_and_platform_context():
    text = (ROOT / "database" / "connection.py").read_text()
    assert "_apply_request_rls_context" in text
    assert "app.location_id" in text
    assert "app.platform_admin" in text
    assert "_apply_request_rls_context(connection, \"postgres\")" in text


def test_meta_webhook_resolves_before_location_transaction():
    text = (ROOT / "routes" / "webhooks.py").read_text()
    assert "get_platform_session" in text
    assert "_resolve_meta_webhook_location" in text
    assert "with location_transaction(location_id)" in text


def test_paystack_webhook_resolves_before_location_transaction():
    text = (ROOT / "routes" / "paystack.py").read_text()
    assert "get_platform_session" in text
    assert "resolve_paystack_location" in text
    assert "with location_transaction(location_id)" in text
    assert "location_id=location_id" in text


def test_provider_blueprints_are_explicitly_csrf_exempt():
    text = (ROOT / "phanta_app.py").read_text()
    assert "csrf.exempt(webhooks_bp)" in text
    assert "csrf.exempt(paystack_bp)" in text
