"""Proof for this loop's System 6 (Communication & WhatsApp) audit.

One genuine repair this loop: ai/follow_up/service.py's _send() had zero
consent/opt-out checking at all, unlike ai/communications/lifecycle.py's
identical _send(), which already gates on services/consent_service.py. Dormant
in practice today -- every current follow_up message type (service_due,
booking_reminder, ready_for_collection_nudge) is an OPERATIONAL category, so
the gate is a no-op for all of them -- but a real, closed architectural gap
for the moment a marketing-category message is ever added to that class.

Everything else audited this loop (duplicate webhook protection, foreign
conversation/connection isolation, the Flyer Lady/WhatsApp boundary, message
lifecycle states) was found already correctly implemented by prior loops'
work, verified fresh here rather than re-fixed. This file proves both: the
one new repair, and two of the adversarial checks the brief calls out
explicitly that had no existing direct test coverage.
"""
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def comms_env(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")
    monkeypatch.setenv("META_WHATSAPP_APP_ID", "123456789012345")
    monkeypatch.setenv("META_WHATSAPP_APP_SECRET", "test-only-not-a-real-app-secret-value")

    from database import initialize_database
    initialize_database(run_migrations=False)
    yield


def _make_location(label):
    from database import execute_db, query_db, utc_now
    suffix = uuid.uuid4().hex[:10]
    email = f"{label}-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"{label} Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                  monthly_base_price, monthly_message_limit, overage_price_per_message,
                                  contact_email, access_locked)
           VALUES (%s,%s,'workshop',TRUE,%s,%s,1000,50,0.5,%s,FALSE)""",
        (owner_id, f"{label} Workshop {suffix}", utc_now(), utc_now(), email),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    return {"owner_id": owner_id, "location_id": location_id, "email": email}


# ---------------------------------------------------------------------------
# 1. The new repair: follow_up.py's _send() now has a real consent gate.
# ---------------------------------------------------------------------------

def test_follow_up_send_blocks_marketing_category_without_opt_in(comms_env):
    """Direct proof of the new gate: an explicit MARKETING-category call
    through _send() must be suppressed by default (no recorded opt-in),
    matching lifecycle.py's _may_send() semantics exactly. This category
    is not used by any real caller in this class today, so this test
    calls _send() directly rather than through process_due()."""
    from database import SessionLocal, set_location_id, execute_db, query_db, utc_now
    from constants.message_categories import MARKETING
    from ai.follow_up.service import DeterministicFollowUpService

    loc = _make_location("ConsentGate")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    from models.core import Customer
    customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X", whatsapp_number="27821119000")
    db.add(customer); db.flush()
    db.commit()

    class _BoomIfCalled:
        def send_auto(self, **kwargs):
            raise AssertionError("messaging.send_auto must never be reached when consent blocks the send")

    service = DeterministicFollowUpService(db, messaging=_BoomIfCalled())
    result = service._send(loc["location_id"], customer.id, "some promotional text", category=MARKETING)
    assert result is None, "an unconsented marketing send must be suppressed, not attempted"
    db.close()


def test_follow_up_send_still_delivers_operational_messages_unaffected(comms_env, monkeypatch):
    """The gate must not regress the actual, real message types this class
    sends today -- all operational, never suppressed by consent."""
    from integrations.meta.services.graph_api_client import GraphApiClient

    class _FakeGraphOK:
        def post_with_token(self, access_token, path, *, data=None, json_data=None, timeout=15.0):
            return {"messages": [{"id": "wamid-" + uuid.uuid4().hex[:8]}]}

    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphOK().post_with_token)

    from database import SessionLocal, set_location_id
    from models.core import Customer, Conversation, Message
    from constants.message_categories import SERVICE_FOLLOWUP
    from ai.follow_up.service import DeterministicFollowUpService
    from integrations.meta.messaging.messaging_service import MetaMessagingService
    from integrations.meta.auth.capability_config import WhatsAppMetaConfig
    from integrations.meta.auth.token_store import MetaTokenStore
    from models.integration_models import MetaBusinessConnection

    loc = _make_location("ConsentOperational")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    conn = MetaBusinessConnection(location_id=loc["location_id"], waba_id="w1", phone_number_id="p1",
                                   connection_status="connected")
    db.add(conn); db.flush()
    MetaTokenStore().save_customer_token(db, conn, "customer-token")
    customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X", whatsapp_number="27821119001")
    db.add(customer); db.flush()
    conversation = Conversation(location_id=loc["location_id"], customer_id=customer.id, channel="whatsapp")
    db.add(conversation); db.flush()
    db.add(Message(location_id=loc["location_id"], conversation_id=conversation.id, direction="inbound",
                    channel="whatsapp", body="hi", whatsapp_message_id="in-" + uuid.uuid4().hex[:8],
                    status="received", created_at=datetime.now(timezone.utc)))
    db.flush()
    db.commit()

    messaging = MetaMessagingService(db, graph=GraphApiClient(WhatsAppMetaConfig.from_env()), token_store=MetaTokenStore())
    service = DeterministicFollowUpService(db, messaging=messaging)
    result = service._send(loc["location_id"], customer.id, "your vehicle is due", category=SERVICE_FOLLOWUP)
    assert result is not None, "an operational message must never be blocked by the consent gate"
    db.close()


# ---------------------------------------------------------------------------
# 2. Foreign conversation: Location A staff must not reach Location B's
#    conversation through the Service Advisor route's ORM path.
# ---------------------------------------------------------------------------

def test_service_advisor_reply_rejects_foreign_location_conversation(comms_env):
    from database import SessionLocal, set_location_id
    from models.core import Customer, Conversation
    from integrations.ai.conversations.conversation_service import AIConversationService

    a = _make_location("SAForeignA")
    b = _make_location("SAForeignB")

    db = SessionLocal()
    set_location_id(db, b["location_id"])
    customer_b = Customer(location_id=b["location_id"], first_name="B", last_name="Customer", whatsapp_number="27821119002")
    db.add(customer_b); db.flush()
    conversation_b = Conversation(location_id=b["location_id"], customer_id=customer_b.id, channel="whatsapp")
    db.add(conversation_b); db.flush()
    db.commit()
    conversation_b_id = conversation_b.id
    customer_b_id = customer_b.id
    db.close()

    # Location A's own session, scoped to A, attempting to reply into B's
    # conversation/customer IDs -- the exact "foreign object ID" attack
    # Phase 2/8 describe.
    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    service = AIConversationService(dispatcher=None)
    with pytest.raises(ValueError, match="conversation not found"):
        service.reply(
            session=db_a, location_id=a["location_id"], conversation_id=conversation_b_id,
            customer_id=customer_b_id, user_text="hello",
        )
    db_a.close()


# ---------------------------------------------------------------------------
# 3. Duplicate inbound webhook must not create a duplicate business action.
# ---------------------------------------------------------------------------

def test_duplicate_inbound_webhook_does_not_fire_automation_twice(comms_env):
    from database import SessionLocal, set_location_id, execute_db, query_db, utc_now
    from models.integration_models import MetaBusinessConnection
    from integrations.meta.webhook.webhook_router import MetaWebhookRouter

    loc = _make_location("DupeWebhook")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    conn = MetaBusinessConnection(location_id=loc["location_id"], waba_id="WABA_DUPE", phone_number_id="PHONE_DUPE",
                                   connection_status="connected")
    db.add(conn); db.flush()
    db.commit()
    db.close()

    payload = {
        "object": "whatsapp_business_account",
        "entry": [{
            "id": "WABA_DUPE",
            "changes": [{
                "field": "messages",
                "value": {
                    "metadata": {"phone_number_id": "PHONE_DUPE"},
                    "messages": [{
                        "from": "27821119003", "id": "wamid.duplicate_test",
                        "type": "text", "text": {"body": "Hi there"},
                    }],
                },
            }],
        }],
    }

    db2 = SessionLocal()
    router = MetaWebhookRouter(db2)
    first = router.dispatch(payload)
    db2.commit()
    db2.close()

    db3 = SessionLocal()
    router2 = MetaWebhookRouter(db3)
    second = router2.dispatch(payload)
    db3.commit()
    db3.close()

    assert first["results"][0]["duplicate"] is False, "the first delivery of a webhook must be processed"
    assert second["results"][0]["duplicate"] is True, "a redelivered webhook (same event id) must be recognised as duplicate"

    from database import query_db as qdb
    count = qdb(
        "SELECT count(*) AS c FROM messages WHERE location_id=%s AND direction='inbound' "
        "AND whatsapp_message_id='wamid.duplicate_test'",
        (loc["location_id"],), one=True,
    )["c"]
    assert count == 1, f"a duplicate webhook delivery must not create a second inbound message row, found {count}"
