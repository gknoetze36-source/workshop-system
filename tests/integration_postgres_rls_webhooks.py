"""Real PostgreSQL integration tests for PHANTA location RLS and provider resolution.

Run only against a dedicated PostgreSQL test database whose schema is already at
Alembic head::

    PHANTA_TEST_DATABASE_URL='postgresql://...' pytest -q tests/integration_postgres_rls_webhooks.py

The tests use one transaction and roll it back, so they do not intentionally
leave test rows behind. They are skipped when no dedicated PostgreSQL URL is
provided; SQLite is never accepted for these tests.
"""
from __future__ import annotations

import hashlib
import hmac
import os
from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select, text
from sqlalchemy.orm import sessionmaker

from database.sqlalchemy_session import set_location_id
from integrations.meta.webhook.webhook_router import MetaWebhookRouter
from integrations.meta.webhook.webhook_location_resolver import resolve_meta_webhook_location
from integrations.paystack.webhooks.webhook_location_resolver import resolve_paystack_location
from integrations.paystack.webhooks.webhook_handler import WebhookHandler
from models.core import Location
from models.integration_models import MetaBusinessConnection, MetaWebhookEvent, PaymentCustomer, PaystackWebhookEvent

TEST_DATABASE_URL = os.getenv("PHANTA_TEST_DATABASE_URL", "").strip()
pytestmark = pytest.mark.skipif(
    not TEST_DATABASE_URL or not TEST_DATABASE_URL.startswith(("postgresql://", "postgres://")),
    reason="Set PHANTA_TEST_DATABASE_URL to a dedicated PostgreSQL database to run real RLS integration tests",
)


@pytest.fixture()
def db():
    engine = create_engine(TEST_DATABASE_URL, future=True, pool_pre_ping=True)
    Session = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    connection = engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    try:
        yield session
    finally:
        session.close()
        transaction.rollback()
        connection.close()
        engine.dispose()


def _assert_rls_ready(session):
    assert session.execute(
        text("SELECT COALESCE(current_setting('app.location_id', true), '')")
    ).scalar() == ""

    assert session.execute(
        text("SELECT COALESCE(current_setting('app.platform_admin', true), '')")
    ).scalar() == ""

def _location(session, name: str) -> Location:
    location = Location(name=name, legal_name=name, active=True)
    session.add(location)
    session.flush()
    return location


def test_postgres_rls_two_locations_and_platform_admin(db):
    """Location A sees A only; location B sees B only; platform admin sees both."""
    _assert_rls_ready(db)
    a = _location(db, "RLS Integration A")
    b = _location(db, "RLS Integration B")

    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    db.add(MetaBusinessConnection(location_id=a.id, waba_id=f"waba-a-{a.id}", phone_number_id=f"phone-a-{a.id}", connection_status="connected"))
    db.flush()

    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(b.id)})
    db.add(MetaBusinessConnection(location_id=b.id, waba_id=f"waba-b-{b.id}", phone_number_id=f"phone-b-{b.id}", connection_status="connected"))
    db.flush()

    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    visible_a = db.scalars(select(MetaBusinessConnection).order_by(MetaBusinessConnection.id)).all()
    assert [row.location_id for row in visible_a] == [a.id]

    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(b.id)})
    visible_b = db.scalars(select(MetaBusinessConnection).order_by(MetaBusinessConnection.id)).all()
    assert [row.location_id for row in visible_b] == [b.id]

    db.execute(text("SELECT set_config('app.location_id', '', true)"))
    db.execute(text("SELECT set_config('app.platform_admin', '1', true)"))
    visible_platform = db.scalars(select(MetaBusinessConnection).order_by(MetaBusinessConnection.id)).all()
    assert {row.location_id for row in visible_platform} >= {a.id, b.id}


def test_meta_webhook_location_resolution_and_processing_under_rls(db):
    a = _location(db, "Meta Webhook Integration")
    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    db.add(MetaBusinessConnection(location_id=a.id, waba_id=f"waba-meta-{a.id}", phone_number_id=f"phone-meta-{a.id}", connection_status="connected"))
    db.flush()

    db.execute(text("SELECT set_config('app.location_id', '', true)"))
    db.execute(text("SELECT set_config('app.platform_admin', '1', true)"))
    payload = {
        "object": "whatsapp_business_account",
        "entry": [{"id": f"waba-meta-{a.id}", "changes": [{"field": "security", "value": {"phone_number": f"phone-meta-{a.id}"}}]}],
    }
    # Platform read context is deliberately required for the pre-RLS lookup.
    assert resolve_meta_webhook_location(db, payload) == a.id

    db.execute(text("SELECT set_config('app.platform_admin', '', true)"))
    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    result = MetaWebhookRouter(db).dispatch(payload, signature_valid=True)
    assert result["accepted"] is True
    event = db.scalar(select(MetaWebhookEvent).where(MetaWebhookEvent.location_id == a.id))
    assert event is not None and event.processing_status == "processed"


def test_paystack_webhook_location_resolution_and_processing_under_rls(db, monkeypatch):
    a = _location(db, "Paystack Webhook Integration")
    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    customer_code = f"CUS-RLS-{a.id}"
    db.add(PaymentCustomer(location_id=a.id, paystack_customer_code=customer_code, email=f"rls-{a.id}@example.test"))
    db.flush()

    db.execute(text("SELECT set_config('app.location_id', '', true)"))
    db.execute(text("SELECT set_config('app.platform_admin', '1', true)"))
    data = {"customer": {"customer_code": customer_code}, "id": f"evt-{a.id}"}
    assert resolve_paystack_location(db, data) == a.id

    raw = b'{"event":"test.rls","data":{"customer":{"customer_code":"' + customer_code.encode() + b'"},"id":"evt-' + str(a.id).encode() + b'"}}'
    secret = "integration-test-secret"
    signature = hmac.new(secret.encode(), raw, hashlib.sha512).hexdigest()
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", secret)

    db.execute(text("SELECT set_config('app.platform_admin', '', true)"))
    db.execute(text("SELECT set_config('app.location_id', :id, true)"), {"id": str(a.id)})
    import json
    payload = json.loads(raw)
    event, created = WebhookHandler().handle(db, raw, signature, payload, location_id=a.id)
    assert created is True
    assert event.location_id == a.id
    stored = db.scalar(select(PaystackWebhookEvent).where(PaystackWebhookEvent.location_id == a.id))
    assert stored is not None and stored.processing_status == "processed"
