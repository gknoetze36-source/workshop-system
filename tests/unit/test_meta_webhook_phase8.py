import hashlib, hmac
import os
from sqlalchemy import select
import pytest

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models.core import Base
from models.core import Location, Customer, Conversation, Message, Owner
from models.integration_models import MetaBusinessConnection, MetaWebhookEvent
from integrations.meta.webhook.handshake_handler import MetaHandshakeHandler
from integrations.meta.webhook.webhook_router import MetaWebhookRouter


def setup_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    from models import integration_models  # noqa: F401
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


def test_handshake_returns_exact_challenge():
    assert MetaHandshakeHandler("abc").verify("subscribe", "abc", "random") == "random"


def test_handshake_rejects_wrong_token():
    with pytest.raises(PermissionError):
        MetaHandshakeHandler("abc").verify("subscribe", "bad", "random")


def seed_connection(session):
    location = Location(owner=Owner(), name="Test Workshop")
    session.add(location); session.flush()
    conn = MetaBusinessConnection(location_id=location.id, waba_id="waba-1", phone_number_id="phone-1", connection_status="connected")
    session.add(conn)
    customer = Customer(location_id=location.id, first_name="Jane", last_name="Doe", whatsapp_number="27820000000")
    session.add(customer); session.commit()
    return location.id, customer.id


def test_inbound_message_is_persisted_and_deduped():
    session = setup_session(); location_id, customer_id = seed_connection(session)
    payload = {"object":"whatsapp_business_account", "entry":[{"id":"waba-1","changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"phone-1"},"messages":[{"from":"27820000000","id":"wamid-1","timestamp":"1","type":"text","text":{"body":"Hi"}}]}}]}]}
    router = MetaWebhookRouter(session)
    first = router.dispatch(payload)
    session.commit()
    second = router.dispatch(payload)
    assert first["results"][0]["result"]["stored"] is True
    assert second["results"][0]["duplicate"] is True
    assert session.query(Message).filter_by(whatsapp_message_id="wamid-1").count() == 1
    assert session.query(MetaWebhookEvent).filter_by(external_event_id="inbound_message:wamid-1").count() == 1
    session.close()


def test_delivery_status_updates_message():
    session = setup_session(); location_id, customer_id = seed_connection(session)
    conversation = Conversation(location_id=location_id, customer_id=customer_id, channel="whatsapp")
    session.add(conversation); session.flush()
    msg = Message(location_id=location_id, conversation_id=conversation.id, direction="outbound", channel="whatsapp", body="Hello", whatsapp_message_id="wamid-out", status="sent")
    session.add(msg); session.commit()
    payload = {"object":"whatsapp_business_account", "entry":[{"id":"waba-1","changes":[{"field":"messages","value":{"metadata":{"phone_number_id":"phone-1"},"statuses":[{"id":"wamid-out","status":"delivered","timestamp":"2","recipient_id":"27820000000"}]}}]}]}
    result = MetaWebhookRouter(session).dispatch(payload)
    session.commit()
    assert result["results"][0]["result"]["updated"] is True
    assert session.get(Message, msg.id).status == "delivered"
    session.close()


def test_phase12_unknown_whatsapp_sender_creates_customer_and_conversation():
    session = setup_session()
    location_id, _ = seed_connection(session)
    payload = {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "waba-1",
            "changes": [{
                "field": "messages",
                "value": {
                    "metadata": {"phone_number_id": "phone-1"},
                    "messages": [{
                        "from": "27820009999",
                        "id": "wamid-new-1",
                        "timestamp": "1",
                        "type": "text",
                        "text": {"body": "Hi, I need help with my car"},
                    }],
                },
            }],
        }],
    }

    result = MetaWebhookRouter(session).dispatch(payload)
    session.commit()

    event = result["results"][0]["result"]
    assert event["stored"] is True
    assert event["customer_id"]
    assert event["conversation_id"]
    assert event["body"] == "Hi, I need help with my car"
    customer = session.scalar(
        select(Customer).where(Customer.location_id == location_id, Customer.whatsapp_number == "27820009999")
    )
    assert customer is not None
    assert customer.first_name == "New"
    assert session.scalar(select(Message).where(Message.whatsapp_message_id == "wamid-new-1")) is not None
