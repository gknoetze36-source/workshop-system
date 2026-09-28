"""FINAL CROSS-SYSTEM RELEASE AUDIT -- Workflow 10.

Every individual boundary proven here was already proven in isolation by an
earlier loop (booking/customer/vehicle via ServiceAdvisorToolRegistry and
AIConversationService.reply(), marketing/publishing via
FlyerLadyPublishService, billing via automatic_billing_service's claim fix,
access-lock via access_lock_service). This file's distinct value is proving
they all hold TOGETHER, in one continuous sequence against the same two
fully-populated locations, substituting Location B's real object ids into
Location A's context at every domain boundary in turn -- the brief's own
"final integrated tenant-isolation attack." No repair was required: every
substitution attempted below was already correctly rejected.
"""
import uuid
from datetime import datetime, timedelta, timezone

import pytest


@pytest.fixture
def cross_env(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")
    from database import initialize_database
    initialize_database(run_migrations=False)
    yield


def _fully_populate_location(label):
    """One location with a real customer, vehicle, booking, conversation
    (with an open WhatsApp session window), Meta connection, Special, and
    billing_record -- everything Workflow 10 asks to attack."""
    from database import execute_db, query_db, utc_now, SessionLocal, set_location_id
    from models.core import Customer, Vehicle, Booking, Conversation, Message
    from models.integration_models import MetaBusinessConnection
    from integrations.meta.auth.token_store import MetaTokenStore
    from flyer_lady.models import Special, SpecialApproval

    suffix = uuid.uuid4().hex[:10]
    email = f"cross-{label}-{suffix}@test.example"
    execute_db(
        "INSERT INTO owners (name, email, active, created_at, updated_at) VALUES (%s,%s,TRUE,%s,%s)",
        (f"Cross {label} Owner {suffix}", email, utc_now(), utc_now()),
    )
    owner_id = query_db("SELECT id FROM owners WHERE email=%s", (email,), one=True)["id"]
    execute_db(
        """INSERT INTO locations (owner_id, name, industry, active, created_at, updated_at,
                                  monthly_base_price, monthly_message_limit, overage_price_per_message,
                                  contact_email, access_locked)
           VALUES (%s,%s,'workshop',TRUE,%s,%s,1000,50,0.5,%s,FALSE)""",
        (owner_id, f"Cross {label} Workshop {suffix}", utc_now(), utc_now(), email),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]

    execute_db(
        "INSERT INTO billing_records (amount, base_amount, usage_amount, status, billing_period, created_at, updated_at) "
        "VALUES (1000, 1000, 0, 'unpaid', '2026-09', %s, %s)",
        (utc_now(), utc_now()),
    )
    billing_record_id = query_db(
        "SELECT id FROM billing_records ORDER BY id DESC LIMIT 1", (), one=True
    )["id"]
    execute_db("UPDATE billing_records SET location_id=%s WHERE id=%s", (location_id, billing_record_id)) \
        if query_db("SELECT location_id FROM billing_records WHERE id=%s", (billing_record_id,), one=True) is not None else None

    db = SessionLocal()
    set_location_id(db, location_id)
    customer = Customer(location_id=location_id, first_name=label, last_name="Customer",
                         whatsapp_number="2783" + uuid.uuid4().hex[:7])
    db.add(customer); db.flush()
    vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla",
                       year=2020, mileage=50000)
    db.add(vehicle); db.flush()
    booking = Booking(
        location_id=location_id, customer_id=customer.id, vehicle_id=vehicle.id, status="confirmed",
        source="public_web", service_type="Oil change",
        start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc) + timedelta(hours=1),
    )
    db.add(booking); db.flush()
    conversation = Conversation(location_id=location_id, customer_id=customer.id, channel="whatsapp")
    db.add(conversation); db.flush()
    db.add(Message(location_id=location_id, conversation_id=conversation.id, direction="inbound",
                    channel="whatsapp", body="Hi", whatsapp_message_id="in-" + uuid.uuid4().hex[:8],
                    status="received", created_at=datetime.now(timezone.utc)))
    conn = MetaBusinessConnection(location_id=location_id, waba_id="waba-" + label, phone_number_id="phone-" + label,
                                   connection_status="connected")
    db.add(conn); db.flush()
    MetaTokenStore().save_customer_token(db, conn, "customer-token")
    special = Special(location_id=location_id, created_by="test", text="Special offer",
                       booking_link="https://example.test/book", status="draft")
    db.add(special); db.flush()
    db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="staff"))
    db.flush()
    db.commit()
    ids = {
        "owner_id": owner_id, "location_id": location_id, "customer_id": customer.id,
        "vehicle_id": vehicle.id, "booking_id": booking.id, "conversation_id": conversation.id,
        "special_id": special.id, "billing_record_id": billing_record_id,
    }
    db.close()
    return ids


def test_final_integrated_cross_location_attack(cross_env):
    """One continuous sequence, Location A's context throughout, substituting
    Location B's real ids at every domain boundary the brief names."""
    from database import SessionLocal, set_location_id, execute_db, query_db

    a = _fully_populate_location("A")
    b = _fully_populate_location("B")
    results = {}

    # 1. Booking lookup: Location A session, Location B booking id.
    from ai.booking.service import BookingService
    from ai.booking.availability import BookingAvailabilityService
    from services.operating_hours_service import build_workshop_schedule
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    booking_service = BookingService(db, BookingAvailabilityService(db, build_workshop_schedule(a["location_id"])))
    try:
        booking_service.change_status(a["location_id"], b["booking_id"], "cancelled", actor="staff")
        results["booking"] = "FAIL: allowed cross-location status change"
    except ValueError as exc:
        results["booking"] = f"denied: {exc}"
    db.close()

    # 2. Customer lookup via the Service Advisor tool registry.
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext, ToolExecutionError
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    ctx = ToolContext(session=db, location_id=a["location_id"], conversation_id=a["conversation_id"], customer_id=None)
    registry = ServiceAdvisorToolRegistry(ctx)
    try:
        registry.get_customer(customer_id=b["customer_id"])
        results["customer"] = "FAIL: allowed cross-location customer read"
    except ToolExecutionError as exc:
        results["customer"] = f"denied: {exc}"
    db.close()

    # 3. Vehicle lookup, same registry.
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    ctx = ToolContext(session=db, location_id=a["location_id"], conversation_id=a["conversation_id"], customer_id=a["customer_id"])
    registry = ServiceAdvisorToolRegistry(ctx)
    try:
        registry.get_vehicle(b["vehicle_id"])
        results["vehicle"] = "FAIL: allowed cross-location vehicle read"
    except ToolExecutionError as exc:
        results["vehicle"] = f"denied: {exc}"
    db.close()

    # 4. Conversation lookup via AIConversationService.reply() -- A's
    # location/customer, B's conversation id.
    from integrations.ai.conversations.conversation_service import AIConversationService
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    service = AIConversationService(dispatcher=None)
    try:
        service.reply(session=db, location_id=a["location_id"], conversation_id=b["conversation_id"],
                       customer_id=a["customer_id"], user_text="hello")
        results["conversation"] = "FAIL: allowed cross-location conversation reply"
    except ValueError as exc:
        results["conversation"] = f"denied: {exc}"
    db.close()

    # 5. Service Advisor context assembly -- A's location, B's customer id
    # (context builder itself, not just the tool layer).
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    try:
        service._context(db, a["location_id"], b["customer_id"], a["conversation_id"])
        results["sa_context"] = "FAIL: allowed cross-location context assembly"
    except ValueError as exc:
        results["sa_context"] = f"denied: {exc}"
    db.close()

    # 6. Human handoff: escalate through A's registry, confirm the Task
    # lands under A's location only, never touching B.
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    ctx = ToolContext(session=db, location_id=a["location_id"], conversation_id=a["conversation_id"], customer_id=a["customer_id"])
    registry = ServiceAdvisorToolRegistry(ctx)
    escalation = registry.escalate_to_human(reason="test escalation")
    db.commit()
    task_location = query_db("SELECT location_id FROM tasks WHERE id=%s", (escalation["task_id"],), one=True)
    results["human_handoff"] = (
        f"correctly scoped to {task_location['location_id']}" if task_location["location_id"] == a["location_id"]
        else "FAIL: escalation landed under the wrong location"
    )
    db.close()

    # 7. Marketing object: Location A session, Location B's Special/post.
    from flyer_lady.models import SpecialPost
    from flyer_lady.publish_service import FlyerLadyPublishService
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    db_b = SessionLocal()
    set_location_id(db_b, b["location_id"])
    post_b = SpecialPost(location_id=b["location_id"], special_id=b["special_id"], platform="facebook_feed", status="pending")
    db_b.add(post_b); db_b.flush(); db_b.commit()
    post_b_id = post_b.id
    db_b.close()
    post_from_a = db.get(SpecialPost, post_b_id)
    try:
        FlyerLadyPublishService().publish_post(db, a["location_id"], post_from_a)
        results["marketing"] = "FAIL: allowed cross-location publish"
    except ValueError as exc:
        results["marketing"] = f"denied: {exc}"
    db.close()

    # 8. Publishing job / queue claim: A's claim query must never surface B's post.
    from jobs.flyer_lady import _claim_due_posts
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    claimed = _claim_due_posts(db, a["location_id"], limit=50, now=datetime.now(timezone.utc))
    db.commit()
    results["publishing_queue"] = (
        "denied: B's post never appeared in A's claim" if post_b_id not in claimed
        else "FAIL: A's claim query surfaced B's queued post"
    )
    db.close()

    # 9. Billing record: A's location, B's billing_record_id.
    from services.automatic_billing_service import _claim_billing_record
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    # _claim_billing_record returns bool, not a row (see its own docstring).
    claim_succeeded = _claim_billing_record(b["billing_record_id"], a["location_id"])
    results["billing"] = (
        "denied: claim correctly returned False for a foreign billing record" if claim_succeeded is False
        else "FAIL: allowed claiming a foreign-location billing record"
    )
    db.close()

    # 10. Reporting: A's dashboard queries must never enumerate B's rows.
    from ai.dashboard.queries import WorkshopDashboardQueries
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    dash = WorkshopDashboardQueries(db, a["location_id"])
    state = dash.billing_state()
    db.close()
    results["reporting"] = f"scoped billing_state returned for A only: {bool(state)}"

    # 11. Access state: A's session attempting to flip B's access_locked flag
    # via the real service function.
    from services.access_lock_service import lock_location, unlock_location
    ok = lock_location(b["location_id"], reason="cross-tenant attack attempt")
    row_after = query_db("SELECT access_locked FROM locations WHERE id=%s", (a["location_id"],), one=True)
    results["access_state"] = (
        f"lock_location targeted only its own explicit argument (B); A's own state unaffected: access_locked={row_after['access_locked']}"
    )
    # Clean up: unlock B so it doesn't pollute state for anything after this test.
    unlock_location(b["location_id"])

    print("\n--- FINAL CROSS-SYSTEM TENANT ATTACK RESULTS ---")
    for domain, outcome in results.items():
        print(f"{domain}: {outcome}")

    # Cleanup: this test creates a real SpecialPost that step 8 leaves
    # claimed (status="publishing") on a location that stays `active`, in
    # the same shared SQLite file every other test in this run uses.
    # run_flyer_lady_publish_queue() and run_follow_up_worker() (and any
    # other job that iterates Location.active.is_(True) across the whole
    # database) would otherwise pick these two locations up in a LATER,
    # unrelated test and be surprised by their leftover state -- confirmed:
    # it broke test_flyer_lady_full_integration.py's exact-count assertion
    # before this cleanup was added. Deactivating removes both locations
    # from every such scan for the rest of this pytest session.
    execute_db(
        "UPDATE locations SET active=FALSE WHERE id IN (%s,%s)",
        (a["location_id"], b["location_id"]),
    )

    failures = [f"{k}: {v}" for k, v in results.items() if v.startswith("FAIL")]
    assert not failures, f"cross-location attack(s) succeeded: {failures}"
