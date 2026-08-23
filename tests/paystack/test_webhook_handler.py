import hashlib, hmac, json
import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.core import Base
from models import integration_models
from models.core import Location, Owner
from models.integration_models import Payment
from integrations.paystack.webhooks.webhook_handler import WebhookHandler, PaystackWebhookRejected

@pytest.fixture
def db(monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    s = Session(); s.add(Owner(id=1, name="Owner", email="owner@example.com", active=True)); s.add(Location(id=1, owner_id=1, name="Workshop")); s.add(Payment(location_id=1, reference="REF1", amount=100, currency="ZAR", status="initialized")); s.commit(); yield s; s.close()

def signed(payload, secret="secret"):
    raw = json.dumps(payload, separators=(",", ":")).encode()
    sig = hmac.new(secret.encode(), raw, hashlib.sha512).hexdigest()
    return raw, sig

def test_webhook_is_idempotent_and_updates_payment(db, monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "secret")
    payload = {"event":"charge.success","data":{"reference":"REF1","id":99,"status":"success","gateway_response":"Successful","channel":"card"}}
    raw, sig = signed(payload)
    handler = WebhookHandler()
    event, created = handler.handle(db, raw, sig, payload)
    db.commit()
    assert created is True
    assert db.query(Payment).one().status == "success"
    event2, created2 = handler.handle(db, raw, sig, payload)
    assert created2 is False
    assert event2.id == event.id

def test_invalid_signature_rejected(db, monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "secret")
    payload = {"event":"charge.success","data":{"reference":"REF1"}}
    raw, _ = signed(payload)
    with pytest.raises(PaystackWebhookRejected):
        WebhookHandler().handle(db, raw, "bad", payload)
