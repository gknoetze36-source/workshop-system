"""Proof for this loop's WhatsApp session-window / approved-template repairs.

MetaMessagingService.send_auto() was already correctly refusing to send a
business-initiated message to a customer whose 24-hour session window is
closed when no approved template is supplied -- that refusal is Meta policy,
correctly enforced, and this loop does not touch it. The defect was entirely
on VANTA's side of that refusal:

1. integrations/meta/messaging/messaging_service.py raised the generic
   MetaMessagingError for this case, indistinguishable from a genuine send
   failure to every catcher. Added MetaSessionWindowClosedError, a specific
   subclass, so callers can tell "provider correctly refused, needs a
   template" apart from "something actually broke".

2. ai/follow_up/service.py's process_due() and ai/communications/lifecycle.py's
   process_due_followups() both collapsed this into status="failed" via a
   generic except, which (a) gave staff/reporting no way to distinguish a
   real failure from an expected provider refusal, and (b) did not stop it -- a
   FollowUp already excluded from "failed" is FollowUp.status == "scheduled"
   is process_due's own re-selection filter, so a status of "failed" already
   prevented re-selection; what "blocked_requires_template" actually adds is
   distinguishability, not retry-loop prevention (that was already correct by
   construction). Both now catch MetaSessionWindowClosedError specifically and
   record status="blocked_requires_template".

3. Far more serious: ai/communications/review.py's send_for_booking(),
   ai/communications/lifecycle.py's booking_missed(), and its
   ready_for_collection() are all called *synchronously* from staff dashboard
   actions (routes/bookings.py, routes/lifecycle.py) whose except clauses only
   catch (KeyError, ValueError) -- MetaMessagingError is a RuntimeError, so an
   uncaught MetaSessionWindowClosedError propagated all the way to an
   unhandled 500, and because the request shares one session, the booking's
   own status change (to completed / no_show / ready_for_collection) was
   never committed either. A customer whose session window happens to be
   closed -- plausible for all three, since each fires at the moment staff
   closes out the job, not necessarily right after customer contact --
   blocked the core staff action entirely. All three now catch the specific
   exception, record a blocked_requires_template FollowUp, and return None
   (the same shape as their existing "nothing to do" returns), letting the
   staff action succeed regardless.

Mocked at the correct integration boundary throughout: GraphApiClient.post_with_token
is patched (as the existing test suite already does elsewhere), never a real
Meta account.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def session_window_env(monkeypatch):
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


def _seed_connection_and_customer(db, location_id, *, whatsapp_number, with_open_session=False):
    from models.integration_models import MetaBusinessConnection
    from integrations.meta.auth.token_store import MetaTokenStore
    from models.core import Customer, Conversation, Message

    conn = MetaBusinessConnection(
        location_id=location_id, waba_id="waba-" + uuid.uuid4().hex[:6],
        phone_number_id="phone-" + uuid.uuid4().hex[:6], connection_status="connected",
    )
    db.add(conn); db.flush()
    MetaTokenStore().save_customer_token(db, conn, "customer-token")

    customer = Customer(location_id=location_id, first_name="C", last_name="X", whatsapp_number=whatsapp_number)
    db.add(customer); db.flush()

    if with_open_session:
        conversation = Conversation(location_id=location_id, customer_id=customer.id, channel="whatsapp")
        db.add(conversation); db.flush()
        db.add(Message(
            location_id=location_id, conversation_id=conversation.id, direction="inbound",
            channel="whatsapp", body="Hi", whatsapp_message_id="inbound-" + uuid.uuid4().hex[:8],
            status="received", created_at=datetime.now(timezone.utc),
        ))
        db.flush()
    return customer


class _FakeGraphAlwaysOK:
    def post_with_token(self, access_token, path, *, data=None, json_data=None, timeout=15.0):
        return {"messages": [{"id": "wamid-fake-" + uuid.uuid4().hex[:8]}]}


# ---------------------------------------------------------------------------
# Exception classification itself
# ---------------------------------------------------------------------------

def test_session_window_closed_raises_the_specific_subclass(session_window_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    from integrations.meta.messaging.messaging_service import (
        MetaMessagingService, MetaMessagingError, MetaSessionWindowClosedError,
    )
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import SessionLocal, set_location_id
    loc = _make_location("ExcType")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = _seed_connection_and_customer(db, loc["location_id"], whatsapp_number="27821110000",
                                              with_open_session=False)
    db.commit()

    from models.core import Conversation
    conversation = db.scalar(__import__("sqlalchemy").select(Conversation).where(
        Conversation.location_id == loc["location_id"], Conversation.customer_id == customer.id,
    ))
    if conversation is None:
        conversation = Conversation(location_id=loc["location_id"], customer_id=customer.id, channel="whatsapp")
        db.add(conversation); db.flush(); db.commit()

    from integrations.meta.auth.capability_config import WhatsAppMetaConfig
    from integrations.meta.auth.token_store import MetaTokenStore
    service = MetaMessagingService(db, graph=GraphApiClient(WhatsAppMetaConfig.from_env()), token_store=MetaTokenStore())

    with pytest.raises(MetaSessionWindowClosedError) as exc_info:
        service.send_auto(location_id=loc["location_id"], conversation_id=conversation.id,
                           to=customer.whatsapp_number, body="test")
    # Must still satisfy any existing caller that only knows about the base class.
    assert isinstance(exc_info.value, MetaMessagingError)
    db.close()


# ---------------------------------------------------------------------------
# follow_up.py's process_due(): open unchanged, closed blocked (not failed),
# no infinite retry, duplicate run doesn't repeat.
# ---------------------------------------------------------------------------

def test_follow_up_open_session_still_sends_normally(session_window_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import SessionLocal, set_location_id
    from models.core import Vehicle, Recommendation
    from ai.follow_up.service import DeterministicFollowUpService

    loc = _make_location("FUOpen")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = _seed_connection_and_customer(db, loc["location_id"], whatsapp_number="27821110001",
                                              with_open_session=True)
    vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla", mileage=100000)
    db.add(vehicle); db.flush()
    rec = Recommendation(location_id=loc["location_id"], vehicle_id=vehicle.id, service_type="Oil change",
                          status="open", due_mileage=90000)
    db.add(rec); db.flush()
    followup = DeterministicFollowUpService(db).schedule_service_due(loc["location_id"], rec.id)
    db.commit()
    db.close()

    from jobs.follow_up import run_follow_up_worker
    results = run_follow_up_worker()
    mine = next(r for r in results if r["location_id"] == loc["location_id"])
    assert followup.id in mine["sent_followups"], f"open session must still send normally, got {mine}"

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    from models.core import FollowUp
    row = db.get(FollowUp, followup.id)
    assert row.status == "sent"
    db.close()


def test_follow_up_closed_session_is_blocked_not_falsely_sent(session_window_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import SessionLocal, set_location_id
    from models.core import Vehicle, Recommendation, FollowUp
    from ai.follow_up.service import DeterministicFollowUpService

    loc = _make_location("FUClosed")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = _seed_connection_and_customer(db, loc["location_id"], whatsapp_number="27821110002",
                                              with_open_session=False)
    vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla", mileage=100000)
    db.add(vehicle); db.flush()
    rec = Recommendation(location_id=loc["location_id"], vehicle_id=vehicle.id, service_type="Oil change",
                          status="open", due_mileage=90000)
    db.add(rec); db.flush()
    followup = DeterministicFollowUpService(db).schedule_service_due(loc["location_id"], rec.id)
    db.commit()
    followup_id = followup.id
    db.close()

    from jobs.follow_up import run_follow_up_worker
    first = run_follow_up_worker()
    mine = next(r for r in first if r["location_id"] == loc["location_id"])
    assert followup_id not in mine["sent_followups"], "a closed-session message must never falsely appear sent"

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    row = db.get(FollowUp, followup_id)
    assert row.status == "blocked_requires_template", (
        f"expected 'blocked_requires_template', got {row.status!r} -- must not collapse into 'failed'"
    )
    db.close()

    from database import query_db
    sent_messages = query_db(
        "SELECT count(*) AS c FROM messages WHERE location_id=%s AND direction='outbound'",
        (loc["location_id"],), one=True,
    )["c"]
    assert sent_messages == 0, "no message row should exist -- nothing was actually sent"

    # No infinite retry: a second run must not re-attempt (query filters on
    # status == "scheduled", which this row has left), and must not create
    # any second FollowUp/blocked record for the same recommendation.
    second = run_follow_up_worker()
    second_mine = next(r for r in second if r["location_id"] == loc["location_id"])
    assert second_mine["sent_followups"] == []
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    count = db.scalar(__import__("sqlalchemy").select(__import__("sqlalchemy").func.count()).select_from(FollowUp).where(
        FollowUp.location_id == loc["location_id"], FollowUp.type == "service_due",
    ))
    assert count == 1, f"exactly one FollowUp row must exist across both runs, found {count}"
    db.close()


# ---------------------------------------------------------------------------
# lifecycle.py's process_due_followups(): same proof, different call site.
# ---------------------------------------------------------------------------

def test_lifecycle_closed_session_yearly_message_is_blocked_not_failed(session_window_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import SessionLocal, set_location_id, execute_db, query_db, utc_now
    from models.core import FollowUp

    loc = _make_location("LifecycleClosed")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = _seed_connection_and_customer(db, loc["location_id"], whatsapp_number="27821110003",
                                              with_open_session=False)
    followup = FollowUp(
        location_id=loc["location_id"], customer_id=customer.id, type="yearly_message",
        scheduled_for=datetime.now(timezone.utc) - timedelta(minutes=5), channel="whatsapp",
        payload={"message_kind": "yearly_message"},
    )
    db.add(followup); db.flush()
    db.commit()
    followup_id = followup.id
    db.close()

    from jobs.lifecycle_communication import run_lifecycle_communication
    results = run_lifecycle_communication()
    mine = next(r for r in results if r["location_id"] == loc["location_id"])
    assert followup_id not in mine.get("sent_followups", []), f"must not falsely appear sent, got {mine}"

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    row = db.get(FollowUp, followup_id)
    assert row.status == "blocked_requires_template", f"expected blocked_requires_template, got {row.status!r}"
    db.close()


# ---------------------------------------------------------------------------
# The most serious fixes: synchronous staff actions must succeed even when
# the customer message is blocked.
# ---------------------------------------------------------------------------

def test_post_service_review_blocked_session_does_not_raise_and_records_blocked(session_window_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import SessionLocal, set_location_id
    from models.core import Booking, FollowUp
    from ai.communications.review import PostServiceReviewService
    from integrations.meta.messaging.messaging_service import MetaMessagingService
    from integrations.meta.auth.capability_config import WhatsAppMetaConfig
    from integrations.meta.auth.token_store import MetaTokenStore

    loc = _make_location("ReviewClosed")
    from database import execute_db
    execute_db(
        "UPDATE locations SET review_request_enabled=TRUE, review_platform='google', "
        "review_url='https://g.page/test-workshop' WHERE id=%s",
        (loc["location_id"],),
    )

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = _seed_connection_and_customer(db, loc["location_id"], whatsapp_number="27821110004",
                                              with_open_session=False)

    booking = Booking(
        location_id=loc["location_id"], customer_id=customer.id, status="completed",
        source="public_web", service_type="Oil change",
        start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db.add(booking); db.flush()
    db.commit()
    booking_id = booking.id
    db.close()

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    messaging = MetaMessagingService(db, graph=GraphApiClient(WhatsAppMetaConfig.from_env()), token_store=MetaTokenStore())
    service = PostServiceReviewService(db, messaging)
    # The core proof: this must not raise. Before the fix, it propagated
    # MetaSessionWindowClosedError uncaught -- which, called synchronously
    # from routes/bookings.py's change_booking_status() (whose except only
    # catches KeyError/ValueError), meant staff could not complete the
    # booking at all.
    result = service.send_for_booking(loc["location_id"], booking_id)
    db.commit()
    assert result is None, "a blocked send must return None, not raise, so the caller's request can still commit"

    db2 = SessionLocal()
    set_location_id(db2, loc["location_id"])
    row = db2.scalar(__import__("sqlalchemy").select(FollowUp).where(
        FollowUp.location_id == loc["location_id"], FollowUp.type == "post_service_review",
    ))
    assert row is not None, "a blocked attempt must still leave a durable record"
    assert row.status == "blocked_requires_template"
    db2.close()


def test_change_booking_status_route_succeeds_even_when_review_request_is_blocked(session_window_env, monkeypatch):
    """End-to-end proof at the actual route level: the real defect was staff
    being unable to complete a booking at all. Exercise the real HTTP route
    through a genuine registration/onboarding/login flow, not a hand-built
    session, following the same pattern tests/unit/test_dashboard_actions.py
    already uses successfully for this exact route."""
    import re
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    from database import execute_db, query_db

    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        if m:
            return m.group(1)
        # The dashboard's own page carries the token as a <meta> tag for its
        # JS-driven fetch() calls, not a form <input> -- same fallback
        # tests/unit/test_dashboard_actions.py's helper already uses.
        m2 = re.search(r'name="csrf-token" content="([^"]+)"', html)
        return m2.group(1) if m2 else None

    suffix = uuid.uuid4().hex[:10]
    email = f"route-session-window-{suffix}@test.example"
    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Route Test", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Route Session Window Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })
    location_id = query_db(
        "SELECT l.id FROM locations l JOIN users u ON u.location_id=l.id WHERE u.email=%s",
        (email,), one=True,
    )["id"]

    execute_db(
        "UPDATE locations SET review_request_enabled=TRUE, review_platform='google', "
        "review_url='https://g.page/test-workshop' WHERE id=%s",
        (location_id,),
    )

    from database import SessionLocal, set_location_id
    from models.core import Booking, Vehicle
    db = SessionLocal()
    set_location_id(db, location_id)
    customer = _seed_connection_and_customer(db, location_id, whatsapp_number="27821110005",
                                              with_open_session=False)
    # templates/dashboard/workshop.html renders booking.vehicle.id
    # unconditionally, so a real vehicle is required for the dashboard page
    # itself to render -- unrelated to the fix under test, but needed for
    # this end-to-end route-level proof to even reach the status route.
    vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla")
    db.add(vehicle); db.flush()
    # confirmed -> completed is not a legal direct transition
    # (ai/booking/service.py's _ALLOWED_TRANSITIONS requires going through
    # ready_for_collection or collected first); seed the booking already
    # at ready_for_collection so the single status-change call under test
    # is the legal ready_for_collection -> completed transition.
    booking = Booking(
        location_id=location_id, customer_id=customer.id, vehicle_id=vehicle.id, status="ready_for_collection",
        source="public_web", service_type="Oil change",
        start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db.add(booking); db.flush(); db.commit()
    booking_id = booking.id
    db.close()

    status_token = csrf_from("/dashboard")
    response = client.post(
        f"/bookings/{booking_id}/status",
        json={"status": "completed", "actor": "staff"},
        headers={"X-CSRFToken": status_token},
    )
    assert response.status_code == 200, (
        f"staff must be able to complete the booking even when the review request is blocked, "
        f"got {response.status_code}: {response.get_data(as_text=True)[:500]}"
    )
    body = response.get_json()
    assert body["status"] == "completed"
    assert body["review_message_id"] is None, "no message was actually sent, so this must be None, not a fabricated id"
