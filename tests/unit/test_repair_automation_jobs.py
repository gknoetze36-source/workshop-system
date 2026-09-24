"""Proof for this loop's System 4 (Automation) repairs.

Three genuine defects found and fixed, all in jobs/*.py's location-selection
and error-handling, not in the underlying services (which were already
mostly correct):

1. access_locked bypass (jobs/booking_expiry.py, jobs/meta_token_monitor.py,
   jobs/follow_up.py): `Location.active` and `locations.access_locked` are
   separate columns -- lock_location() (services/access_lock_service.py)
   never touches `active`. The payment wall (services/auth_service.py) is
   enforced on every authenticated route, but background jobs have no
   request/session context to check against, so they kept operating on
   locked-out, non-paying locations indefinitely -- most seriously for
   follow_up, which sends real WhatsApp messages through PHANTA's own paid
   Meta API credentials.

2. booking_expiry's per-location loop used `except Exception: raise`,
   aborting every remaining location for the rest of that 5-minute cycle
   the moment any single location errored. meta_token_monitor.py and
   follow_up.py already used the correct pattern (record the error against
   that location, continue the loop) -- booking_expiry didn't.

3. ai/follow_up/service.py's process_due(): audit.record() ran inside the
   same try block as the actual send. If it threw *after* a successful
   send, the except set status="failed" and re-raised, which
   jobs/follow_up.py rolls the whole location's transaction back for --
   reverting the FollowUp row to "scheduled" even though the customer
   already received the message, so the next run would send it again.
   Fixed with a SAVEPOINT (session.begin_nested()) so the audit write's
   failure can't poison the "sent" status that was set immediately after
   the send succeeded, before the audit call.
"""
import uuid
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest


@pytest.fixture
def job_env(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")
    monkeypatch.setenv("META_WHATSAPP_APP_ID", "123456789012345")
    monkeypatch.setenv("META_WHATSAPP_APP_SECRET", "test-only-not-a-real-app-secret-value")

    from database import initialize_database
    initialize_database(run_migrations=False)
    yield


def _make_location(label, *, access_locked=False, active=True):
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
           VALUES (%s,%s,'workshop',%s,%s,%s,1000,50,0.5,%s,%s)""",
        (owner_id, f"{label} Workshop {suffix}", active, utc_now(), utc_now(), email, access_locked),
    )
    location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
    return {"owner_id": owner_id, "location_id": location_id, "email": email}


# ---------------------------------------------------------------------------
# 1. access_locked exclusion -- proven identically for all three jobs, since
#    all three share the exact same defect and the exact same fix shape.
# ---------------------------------------------------------------------------

def test_booking_expiry_skips_access_locked_locations(job_env):
    from database import execute_db, query_db, utc_now, set_location_id, SessionLocal
    from models.core import Booking, Customer, Vehicle

    unlocked = _make_location("BEUnlocked")
    locked = _make_location("BELocked", access_locked=True)

    stale = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    for loc in (unlocked, locked):
        db = SessionLocal()
        set_location_id(db, loc["location_id"])
        customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X",
                             whatsapp_number=f"2782{loc['location_id']:07d}")
        db.add(customer); db.flush()
        vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla")
        db.add(vehicle); db.flush()
        booking = Booking(
            location_id=loc["location_id"], customer_id=customer.id, vehicle_id=vehicle.id,
            status="pending", source="public_web", service_type="Oil change",
            start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        db.add(booking); db.flush()
        db.execute(
            __import__("sqlalchemy").text("UPDATE bookings SET created_at=:c WHERE id=:i"),
            {"c": stale, "i": booking.id},
        )
        db.commit()
        loc["booking_id"] = booking.id
        db.close()

    from jobs.booking_expiry import run_booking_expiry
    result = run_booking_expiry()

    assert unlocked["booking_id"] in result["expired_booking_ids"], (
        "the unlocked location's stale booking must be expired"
    )
    assert locked["booking_id"] not in result["expired_booking_ids"], (
        "an access_locked location must not be processed at all -- its booking must be left untouched"
    )

    db = SessionLocal()
    still_pending = db.get(Booking, locked["booking_id"])
    assert still_pending.status == "pending", "locked location's booking must not have been touched"
    db.close()


def test_meta_token_monitor_skips_access_locked_locations(job_env):
    from database import SessionLocal

    unlocked = _make_location("MTMUnlocked")
    locked = _make_location("MTMLocked", access_locked=True)

    from jobs.meta_token_monitor import run_meta_token_monitor
    results = run_meta_token_monitor()

    checked_location_ids = {r["location_id"] for r in results}
    assert unlocked["location_id"] in checked_location_ids
    assert locked["location_id"] not in checked_location_ids, (
        "an access_locked location must not be included in the monitor run at all"
    )


def test_follow_up_worker_skips_access_locked_locations(job_env):
    unlocked = _make_location("FUUnlocked")
    locked = _make_location("FULocked", access_locked=True)

    from jobs.follow_up import run_follow_up_worker
    results = run_follow_up_worker()

    processed_location_ids = {r["location_id"] for r in results}
    assert unlocked["location_id"] in processed_location_ids
    assert locked["location_id"] not in processed_location_ids, (
        "a non-paying, access_locked location's customers must not receive automated "
        "WhatsApp messages through PHANTA's own Meta API credentials"
    )


# ---------------------------------------------------------------------------
# 2. booking_expiry: one location's failure must not abort the rest.
# ---------------------------------------------------------------------------

def test_booking_expiry_one_location_failure_does_not_stop_others(job_env, monkeypatch):
    from database import execute_db, query_db, utc_now, set_location_id, SessionLocal
    from models.core import Booking, Customer, Vehicle

    good = _make_location("BEGood")
    bad = _make_location("BEBad")

    stale = (datetime.now(timezone.utc) - timedelta(hours=72)).isoformat()
    for loc in (good, bad):
        db = SessionLocal()
        set_location_id(db, loc["location_id"])
        customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X",
                             whatsapp_number=f"2783{loc['location_id']:07d}")
        db.add(customer); db.flush()
        vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla")
        db.add(vehicle); db.flush()
        booking = Booking(
            location_id=loc["location_id"], customer_id=customer.id, vehicle_id=vehicle.id,
            status="pending", source="public_web", service_type="Oil change",
            start_time=datetime.now(timezone.utc), end_time=datetime.now(timezone.utc) + timedelta(hours=1),
        )
        db.add(booking); db.flush()
        db.execute(
            __import__("sqlalchemy").text("UPDATE bookings SET created_at=:c WHERE id=:i"),
            {"c": stale, "i": booking.id},
        )
        db.commit()
        loc["booking_id"] = booking.id
        db.close()

    from ai.booking.expiry_service import BookingExpiryService
    original = BookingExpiryService.expire_stale_pending

    def _boom(self, location_id, **kwargs):
        if location_id == bad["location_id"]:
            raise RuntimeError("simulated failure for this location only")
        return original(self, location_id, **kwargs)

    monkeypatch.setattr(BookingExpiryService, "expire_stale_pending", _boom)

    from jobs.booking_expiry import run_booking_expiry
    result = run_booking_expiry()

    assert good["booking_id"] in result["expired_booking_ids"], (
        "the healthy location must still be processed even though another location failed"
    )
    assert any(f["location_id"] == bad["location_id"] for f in result.get("failed_locations", [])), (
        "the failing location must be recorded as failed, not silently dropped"
    )


# ---------------------------------------------------------------------------
# 3. follow_up: duplicate-send prevention across the whole eligible/ineligible/
#    duplicate-run/rollback-safety matrix the brief asks for.
# ---------------------------------------------------------------------------

class _FakeGraphAlwaysOK:
    def post_with_token(self, access_token, path, *, data=None, json_data=None, timeout=15.0):
        return {"messages": [{"id": "wamid-fake-" + uuid.uuid4().hex[:8]}]}


def _seed_follow_up_location(label):
    from database import execute_db, query_db, utc_now, set_location_id, SessionLocal
    from cryptography.fernet import Fernet
    from models.integration_models import MetaBusinessConnection
    from integrations.meta.auth.token_store import MetaTokenStore

    loc = _make_location(label)
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    conn = MetaBusinessConnection(
        location_id=loc["location_id"], waba_id="waba-" + label, phone_number_id="phone-" + label,
        connection_status="connected",
    )
    db.add(conn); db.flush()
    store = MetaTokenStore()
    store.save_customer_token(db, conn, "customer-token")
    db.commit()
    db.close()
    return loc


def _open_session_window(db, *, location_id, customer_id):
    """Seed a Conversation with a recent inbound message so
    WhatsAppSessionWindow.is_open() is True -- send_auto() (called with no
    template_name by every real caller across the app, not just follow_up:
    ai/communications/review.py, ai/communications/lifecycle.py,
    ai/service_advisor/runtime.py, routes/meta_messaging.py all do the
    same) requires an open session or an approved template, and PHANTA has
    no approved utility template wired up for any of these paths yet. This
    is a genuine, separately-scoped finding reported alongside this loop's
    repairs, not fixed here -- a real fix needs an actual Meta-approved
    template, which is an external, days-long process, not a code change.
    Opening the window here reflects a real, common case (a customer who
    messaged recently) so these tests can verify the actual repairs made
    this loop without being blocked by that separate, wider gap."""
    from models.core import Conversation, Message
    conversation = Conversation(location_id=location_id, customer_id=customer_id, channel="whatsapp")
    db.add(conversation); db.flush()
    db.add(Message(
        location_id=location_id, conversation_id=conversation.id, direction="inbound",
        channel="whatsapp", body="Hi", whatsapp_message_id="inbound-seed-" + uuid.uuid4().hex[:8],
        status="received", created_at=datetime.now(timezone.utc),
    ))
    db.flush()
    return conversation


def _seed_due_service_followup(loc, *, whatsapp_number):
    """Create a customer/vehicle/recommendation and, critically, call
    schedule_service_due() directly rather than going through
    seed_due_followups() -- the latter discovers recommendations itself via
    ServiceRuleEngine.persist_due_recommendations(), which needs a
    configured service-rule catalogue for the location's industry and
    ignores a hand-inserted Recommendation row entirely. Calling
    schedule_service_due() directly is the documented, supported way to
    create one FollowUp for an already-open, already-due recommendation
    without needing that whole rule-discovery machinery -- and it's the
    send path (process_due, exercised via run_follow_up_worker) that these
    tests are actually about, not discovery."""
    from database import set_location_id, SessionLocal
    from models.core import Customer, Vehicle, Recommendation
    from ai.follow_up.service import DeterministicFollowUpService

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X", whatsapp_number=whatsapp_number)
    db.add(customer); db.flush()
    _open_session_window(db, location_id=loc["location_id"], customer_id=customer.id)
    vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla", mileage=100000)
    db.add(vehicle); db.flush()
    rec = Recommendation(
        location_id=loc["location_id"], vehicle_id=vehicle.id, service_type="Oil change",
        status="open", due_mileage=90000,
    )
    db.add(rec); db.flush()
    service = DeterministicFollowUpService(db)
    followup = service.schedule_service_due(loc["location_id"], rec.id)
    assert followup is not None, "test setup: the recommendation must be scheduled as due"
    db.commit()
    db.close()
    return {"customer_id": customer.id, "recommendation_id": rec.id, "followup_id": followup.id}


def test_follow_up_eligible_recommendation_produces_exactly_one_sent_action(job_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    loc = _seed_follow_up_location("FUEligible")
    _seed_due_service_followup(loc, whatsapp_number="27821110000")

    from jobs.follow_up import run_follow_up_worker
    results = run_follow_up_worker()
    mine = next(r for r in results if r["location_id"] == loc["location_id"])
    assert len(mine["sent_followups"]) == 1, f"expected exactly one sent follow-up, got {mine}"


def test_follow_up_duplicate_run_sends_no_duplicate_message(job_env, monkeypatch):
    from integrations.meta.services.graph_api_client import GraphApiClient
    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    loc = _seed_follow_up_location("FUDupe")
    _seed_due_service_followup(loc, whatsapp_number="27821110001")

    from jobs.follow_up import run_follow_up_worker
    first = run_follow_up_worker()
    second = run_follow_up_worker()

    first_sent = next(r for r in first if r["location_id"] == loc["location_id"])["sent_followups"]
    second_sent = next(r for r in second if r["location_id"] == loc["location_id"])["sent_followups"]
    assert len(first_sent) == 1
    assert len(second_sent) == 0, (
        "a second run must not re-send the same follow-up -- got "
        f"{second_sent} on top of {first_sent}"
    )

    from database import query_db
    count = query_db(
        "SELECT count(*) AS c FROM messages WHERE location_id=%s AND direction='outbound'",
        (loc["location_id"],), one=True,
    )["c"]
    assert count == 1, f"exactly one outbound message must exist for this customer, found {count}"


def test_follow_up_ineligible_recommendation_produces_no_action(job_env, monkeypatch):
    from database import set_location_id, SessionLocal
    from models.core import Customer, Vehicle, Recommendation
    from integrations.meta.services.graph_api_client import GraphApiClient

    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    loc = _seed_follow_up_location("FUIneligible")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer = Customer(location_id=loc["location_id"], first_name="C", last_name="X", whatsapp_number="27821110002")
    db.add(customer); db.flush()
    # Mileage well below due_mileage -- not actually due yet.
    vehicle = Vehicle(location_id=loc["location_id"], customer_id=customer.id, make="Toyota", model="Corolla", mileage=1000)
    db.add(vehicle); db.flush()
    rec = Recommendation(
        location_id=loc["location_id"], vehicle_id=vehicle.id, service_type="Oil change",
        status="open", due_mileage=90000,
    )
    db.add(rec); db.flush()
    db.commit()
    db.close()

    from jobs.follow_up import run_follow_up_worker
    results = run_follow_up_worker()
    mine = next(r for r in results if r["location_id"] == loc["location_id"])
    assert mine["sent_followups"] == [], f"a not-yet-due recommendation must produce no action, got {mine}"


def test_follow_up_survives_audit_log_failure_without_reverting_to_scheduled(job_env, monkeypatch):
    """The core proof for repair #3: simulate audit.record() throwing right
    after a successful send, and confirm the FollowUp row still ends up
    "sent" (not reverted to "scheduled", which would cause a real duplicate
    WhatsApp send on the next run) and the message really was delivered."""
    from database import set_location_id, SessionLocal, query_db
    from models.core import FollowUp
    from integrations.meta.services.graph_api_client import GraphApiClient
    from repositories.audit_repo import AuditLogRepository

    monkeypatch.setattr(GraphApiClient, "post_with_token", _FakeGraphAlwaysOK().post_with_token)

    loc = _seed_follow_up_location("FUAuditFail")
    # Seed BEFORE patching audit.record() to always fail: schedule_service_due()
    # (called by the seeding helper) makes its own legitimate audit.record()
    # call and must not be broken by the fault this test is about to inject
    # into process_due()'s later, separate audit.record() call.
    _seed_due_service_followup(loc, whatsapp_number="27821110003")

    call_count = {"n": 0}

    def _flaky_record(self, *args, **kwargs):
        call_count["n"] += 1
        raise RuntimeError("simulated audit log failure, e.g. a transient DB error")

    monkeypatch.setattr(AuditLogRepository, "record", _flaky_record)

    from jobs.follow_up import run_follow_up_worker
    results = run_follow_up_worker()
    mine = next(r for r in results if r["location_id"] == loc["location_id"])

    assert call_count["n"] >= 1, "the audit log failure must actually have been exercised"
    assert len(mine["sent_followups"]) == 1, (
        f"the send succeeded and must still be reported as sent despite the audit failure, got {mine}"
    )

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    followup = db.get(FollowUp, mine["sent_followups"][0])
    assert followup.status == "sent", (
        f"expected status='sent' to survive the audit-log failure via the savepoint, got {followup.status!r} -- "
        "if this reads 'scheduled', the transaction was rolled back and the next run would re-send "
        "a message the customer already received"
    )
    db.close()

    count = query_db(
        "SELECT count(*) AS c FROM messages WHERE location_id=%s AND direction='outbound'",
        (loc["location_id"],), one=True,
    )["c"]
    assert count == 1, "exactly one message must have actually been sent, regardless of the audit failure"

    # And critically: a second run must not re-send, proving the fix closes
    # the actual customer-facing duplicate-message risk, not just the status field.
    second = run_follow_up_worker()
    second_mine = next(r for r in second if r["location_id"] == loc["location_id"])
    assert second_mine["sent_followups"] == [], (
        f"a second run after the audit failure must not re-send the same follow-up, got {second_mine}"
    )


# ---------------------------------------------------------------------------
# 4. scheduler: one job's failure must not stop the others in the same cycle.
# ---------------------------------------------------------------------------

def test_scheduler_one_job_failure_does_not_stop_other_jobs(monkeypatch):
    import jobs.scheduler as scheduler_module

    def _boom():
        raise RuntimeError("simulated job failure")

    monkeypatch.setattr(scheduler_module, "run_meta_token_monitor", _boom)
    monkeypatch.setattr(scheduler_module, "run_lifecycle_communication", lambda: {"ok": True})
    monkeypatch.setattr(scheduler_module, "run_follow_up_worker", lambda: {"ok": True})
    monkeypatch.setattr(scheduler_module, "run_flyer_lady_publish_queue", lambda: {"ok": True})
    monkeypatch.setattr(scheduler_module, "run_booking_expiry", lambda: {"ok": True})
    monkeypatch.setattr(scheduler_module, "run_paystack_reconciliation", lambda: {"ok": True})
    monkeypatch.setattr(scheduler_module, "process_due_automation_jobs", lambda: {"ok": True})

    result = scheduler_module.run_scheduled_jobs()

    assert result["meta_token_monitor"]["status"] == "error"
    assert "simulated job failure" in result["meta_token_monitor"]["error"]
    for name in ("lifecycle_communication", "follow_up", "flyer_lady", "booking_expiry",
                 "paystack_reconciliation", "automation_engine"):
        assert result[name]["status"] == "ok", f"{name} must have run despite meta_token_monitor's failure"
