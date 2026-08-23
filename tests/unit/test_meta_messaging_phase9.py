from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models.core import Base, Owner, Location, Customer, Conversation, Message
from models.integration_models import MetaBusinessConnection, MetaMessageTemplate, MetaMessageAttempt
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingError, MetaMessagingService
from integrations.meta.messaging.retry_policy import MetaRetryPolicy
from integrations.meta.messaging.session_window import WhatsAppSessionWindow


def setup_session():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    return Session()


def seed(session, *, inbound_age_hours=1):
    owner = Owner(name="Owner", email="owner@example.com")
    session.add(owner); session.flush()
    location = Location(owner_id=owner.id, name="Phase 9 Workshop")
    session.add(location); session.flush()
    customer = Customer(
        location_id=location.id, first_name="Jane", last_name="Doe",
        whatsapp_number="27820000000"
    )
    session.add(customer); session.flush()
    conversation = Conversation(location_id=location.id, customer_id=customer.id, channel="whatsapp")
    session.add(conversation); session.flush()
    inbound = Message(
        location_id=location.id, conversation_id=conversation.id, direction="inbound",
        channel="whatsapp", body="Hi", whatsapp_message_id="inbound-1",
        status="received", created_at=datetime.now(timezone.utc) - timedelta(hours=inbound_age_hours)
    )
    session.add(inbound)
    conn = MetaBusinessConnection(
        location_id=location.id, waba_id="waba-1", phone_number_id="phone-1",
        connection_status="connected"
    )
    session.add(conn); session.flush()
    token_store = MetaTokenStore(Fernet.generate_key())
    token_store.save_customer_token(session, conn, "customer-token")
    session.commit()
    return location.id, conversation.id, conn.id, token_store


class FakeGraph:
    def __init__(self, response=None, error=None):
        self.response = response or {"messages": [{"id": "wamid-new"}]}
        self.error = error

    def post_with_token(self, access_token, path, *, data=None, json_data=None, timeout=15.0):
        assert access_token == "customer-token"
        assert path == "/phone-1/messages"
        assert json_data["messaging_product"] == "whatsapp"
        if self.error:
            raise self.error
        return self.response


def test_session_window_open_and_closed():
    session = setup_session()
    location_id, conversation_id, _, _ = seed(session, inbound_age_hours=23)
    assert WhatsAppSessionWindow.is_open(session, location_id=location_id, conversation_id=conversation_id)
    session.close()

    session = setup_session()
    location_id, conversation_id, _, _ = seed(session, inbound_age_hours=25)
    assert not WhatsAppSessionWindow.is_open(session, location_id=location_id, conversation_id=conversation_id)
    session.close()


def test_send_text_persists_message_and_wamid():
    session = setup_session()
    location_id, conversation_id, _, token_store = seed(session)
    service = MetaMessagingService(session, graph=FakeGraph(), token_store=token_store)
    msg = service.send_text(
        location_id=location_id, conversation_id=conversation_id,
        to="27820000000", body="Hello from PHANTA"
    )
    session.commit()

    assert msg.status == "sent"
    assert msg.whatsapp_message_id == "wamid-new"
    assert session.query(Message).filter_by(id=msg.id).one().status == "sent"
    attempt = session.query(MetaMessageAttempt).filter_by(message_id=msg.id).one()
    assert attempt.attempt_number == 1
    assert attempt.status == "sent"


def test_send_text_is_blocked_outside_24_hour_window():
    session = setup_session()
    location_id, conversation_id, _, token_store = seed(session, inbound_age_hours=25)
    service = MetaMessagingService(session, graph=FakeGraph(), token_store=token_store)
    with pytest.raises(MetaMessagingError, match="customer_service_window_closed"):
        service.send_text(
            location_id=location_id, conversation_id=conversation_id,
            to="27820000000", body="Should not send"
        )


def test_send_utility_template_requires_approved_template():
    session = setup_session()
    location_id, conversation_id, _, token_store = seed(session, inbound_age_hours=25)
    session.add(MetaMessageTemplate(
        location_id=location_id, waba_id="waba-1", name="booking_confirmation",
        language="en_ZA", category="UTILITY", status="APPROVED"
    ))
    session.commit()
    service = MetaMessagingService(session, graph=FakeGraph(), token_store=token_store)
    msg = service.send_utility_template(
        location_id=location_id, conversation_id=conversation_id,
        to="27820000000", name="booking_confirmation"
    )
    assert msg.status == "sent"


def test_template_must_be_approved_and_utility():
    session = setup_session()
    location_id, conversation_id, _, token_store = seed(session, inbound_age_hours=25)
    session.add(MetaMessageTemplate(
        location_id=location_id, waba_id="waba-1", name="pending",
        language="en_ZA", category="UTILITY", status="PENDING"
    ))
    session.commit()
    service = MetaMessagingService(session, graph=FakeGraph(), token_store=token_store)
    with pytest.raises(MetaMessagingError, match="template_not_sendable"):
        service.send_utility_template(
            location_id=location_id, conversation_id=conversation_id,
            to="27820000000", name="pending"
        )


def test_retry_policy():
    policy = MetaRetryPolicy()
    assert policy.decide(attempt_number=1, http_status=503).retryable
    assert not policy.decide(attempt_number=1, http_status=400).retryable
    assert not policy.decide(attempt_number=1, meta_error_code="200").retryable
    assert policy.decide(attempt_number=1).retryable
    assert not policy.decide(attempt_number=3, http_status=503).retryable


def test_template_status_webhook_mirrors_template_state():
    from integrations.meta.webhook.webhook_router import MetaWebhookRouter
    session = setup_session()
    location_id, _, _, _ = seed(session)
    payload = {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "waba-1",
            "changes": [{
                "field": "message_template_status_update",
                "value": {
                    "message_template_id": "tmpl-1",
                    "message_template_name": "booking_confirmation",
                    "language": "en_ZA",
                    "category": "UTILITY",
                    "event": "APPROVED",
                    "reason": "",
                }
            }]
        }]
    }
    result = MetaWebhookRouter(session).dispatch(payload)
    session.commit()
    assert result["results"][0]["result"]["event"] == "APPROVED"
    template = session.query(MetaMessageTemplate).filter_by(
        location_id=location_id, name="booking_confirmation", language="en_ZA"
    ).one()
    assert template.status == "APPROVED"
    assert template.meta_template_id == "tmpl-1"
