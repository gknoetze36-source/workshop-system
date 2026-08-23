import hashlib
import hmac
import json
import os
from decimal import Decimal
from types import SimpleNamespace

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from models.core import Base, Location, Customer, Owner
from models.integration_models import Payment, PaymentCustomer, Subscription, Invoice, Refund, PaystackWebhookEvent
from integrations.paystack.payments.transaction_service import TransactionService
from integrations.paystack.services.paystack_client import PaystackClient
from integrations.paystack.webhooks.webhook_handler import WebhookHandler, PaystackWebhookRejected


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.text = json.dumps(payload)
    def json(self):
        return self._payload


def make_session():
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    return Session(engine)


def sign(body, secret="sk_test_secret"):
    return hmac.new(secret.encode(), body, hashlib.sha512).hexdigest()


def test_initialize_persists_pending_payment_and_reference_is_stable(monkeypatch):
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    calls = []

    def fake_request(method, url, **kwargs):
        calls.append(kwargs["json"])
        return FakeResponse({"status": True, "data": {"reference": kwargs["json"]["reference"], "authorization_url": "https://checkout", "id": 99, "currency": "ZAR"}})

    client = PaystackClient("sk_test_secret", http_request=fake_request)
    service = TransactionService(client)
    payment, data = service.initialize_and_persist(session, email="owner@example.com", amount=Decimal("199.99"), location_id=location.id, reference="phanta_1_fixed")
    session.commit()

    assert payment.status == "initialized"
    assert payment.amount == Decimal("199.99")
    assert calls[0]["amount"] == 19999
    assert calls[0]["reference"] == "phanta_1_fixed"
    assert calls[0]["metadata"]["phanta_location_id"] == location.id


def test_verify_rechecks_status_amount_currency_and_updates_payment():
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    payment = Payment(location_id=location.id, reference="ref-1", amount=Decimal("100.00"), currency="ZAR", status="initialized")
    session.add(payment); session.commit()

    def fake_request(method, url, **kwargs):
        return FakeResponse({"status": True, "data": {"status": "success", "amount": 10000, "currency": "ZAR", "id": 123, "channel": "card", "gateway_response": "Successful"}})

    service = TransactionService(PaystackClient("sk_test_secret", http_request=fake_request))
    updated, success = service.verify_and_reconcile(session, reference="ref-1")
    session.commit()
    assert success is True
    assert updated.status == "success"
    assert updated.paystack_transaction_id == "123"


def test_verify_rejects_tampered_amount():
    def fake_request(method, url, **kwargs):
        return FakeResponse({"status": True, "data": {"status": "success", "amount": 9999, "currency": "ZAR"}})
    service = TransactionService(PaystackClient("sk_test_secret", http_request=fake_request))
    with pytest.raises(ValueError, match="amount"):
        service.verify(reference="ref", expected_amount=Decimal("100.00"))


def test_webhook_success_is_idempotent_and_updates_payment(monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_secret")
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    payment = Payment(location_id=location.id, reference="ref-1", amount=Decimal("100.00"), currency="ZAR", status="initialized")
    session.add(payment); session.commit()

    payload = {"event": "charge.success", "data": {"reference": "ref-1", "amount": 10000, "currency": "ZAR", "id": 7, "metadata": {"phanta_location_id": location.id}, "gateway_response": "Successful", "channel": "card"}}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    handler = WebhookHandler()
    event, created = handler.handle(session, raw, sign(raw), payload)
    event2, created2 = handler.handle(session, raw, sign(raw), payload)
    session.commit()

    assert created is True and created2 is False
    assert session.query(PaystackWebhookEvent).count() == 1
    assert session.get(Payment, payment.id).status == "success"


def test_webhook_rejects_invalid_signature(monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_secret")
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    payment = Payment(location_id=location.id, reference="ref-1", amount=Decimal("100.00"), currency="ZAR", status="initialized")
    session.add(payment); session.commit()
    payload = {"event": "charge.success", "data": {"reference": "ref-1", "metadata": {"phanta_location_id": location.id}}}
    raw = b'{"event":"charge.success","data":{"reference":"ref-1","metadata":{"phanta_location_id":1}}}'
    with pytest.raises(PaystackWebhookRejected):
        WebhookHandler().handle(session, raw, "bad", payload)


def test_invoice_failure_marks_subscription_past_due(monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_secret")
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    sub = Subscription(location_id=location.id, paystack_subscription_code="SUB_1", plan_code="PLN_1", status="active")
    session.add(sub); session.commit()
    payload = {"event": "invoice.payment_failed", "data": {"subscription": {"subscription_code": "SUB_1"}, "id": "INV_1", "amount": 10000, "gateway_response": "Insufficient Funds", "metadata": {"phanta_location_id": location.id}}}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    WebhookHandler().handle(session, raw, sign(raw), payload)
    session.commit()
    assert session.get(Subscription, sub.id).status == "past_due"
    assert session.query(Invoice).one().status == "failed"


def test_refund_processed_creates_refund_record(monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_secret")
    session = make_session()
    owner = Owner(name="Owner", email="owner@example.com", active=True)
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Workshop")
    session.add(location); session.flush()
    payment = Payment(location_id=location.id, reference="ref-1", amount=Decimal("100.00"), currency="ZAR", status="success")
    session.add(payment); session.commit()
    payload = {"event": "refund.processed", "data": {"id": "RF_1", "amount": 5000, "status": "processed", "transaction": {"reference": "ref-1"}, "metadata": {"phanta_location_id": location.id}}}
    raw = json.dumps(payload, separators=(",", ":")).encode()
    WebhookHandler().handle(session, raw, sign(raw), payload)
    session.commit()
    refund = session.query(Refund).one()
    assert refund.amount == Decimal("50.00")
    assert refund.paystack_refund_id == "RF_1"
