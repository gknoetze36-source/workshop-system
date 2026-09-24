"""Live, behavioral proof for System 6b's Phase 3/10 adversarial checks.

tests/unit/test_step8_ai_ownership.py already exists and checks that the
relevant source code *contains* a location_id filter, via inspect.getsource()
string matching. That proves the scoping code is present; it does not prove a
real cross-tenant attempt is actually rejected at runtime, which is what this
brief explicitly asks for ("Attempt: Location A + Location B vehicle ID.
Expected: DENIED"). This file exercises the real objects with real seeded
two-location fixtures instead.

No repair was needed for any of these -- every attempt below was already
correctly rejected on first run, by code written in earlier loops
(ServiceAdvisorToolRegistry._customer()/_vehicle(), ServiceRuleEngine's
location_id-or-NULL filter). This file closes the proof gap, not a defect.
"""
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def sa_env(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")
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
    return {"owner_id": owner_id, "location_id": location_id}


def _seed_customer_and_vehicle(db, location_id, *, name):
    from models.core import Customer, Vehicle
    customer = Customer(location_id=location_id, first_name=name, last_name="Customer",
                         whatsapp_number="2782" + uuid.uuid4().hex[:7])
    db.add(customer); db.flush()
    vehicle = Vehicle(location_id=location_id, customer_id=customer.id, make="Toyota", model="Corolla", mileage=50000)
    db.add(vehicle); db.flush()
    return customer, vehicle


# ---------------------------------------------------------------------------
# Phase 3: live tool-level foreign-ID rejection, one level deeper than
# AIConversationService.reply()'s own outer check.
# ---------------------------------------------------------------------------

def test_tool_registry_rejects_foreign_location_customer_id(sa_env):
    from database import SessionLocal, set_location_id
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext, ToolExecutionError

    a = _make_location("ToolA")
    b = _make_location("ToolB")

    db_b = SessionLocal()
    set_location_id(db_b, b["location_id"])
    customer_b, _ = _seed_customer_and_vehicle(db_b, b["location_id"], name="B")
    db_b.commit()
    customer_b_id = customer_b.id
    db_b.close()

    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    ctx = ToolContext(session=db_a, location_id=a["location_id"], conversation_id=1, customer_id=None)
    registry = ServiceAdvisorToolRegistry(ctx)
    with pytest.raises(ToolExecutionError, match="customer not found"):
        registry.get_customer(customer_id=customer_b_id)
    db_a.close()


def test_tool_registry_rejects_foreign_location_vehicle_id(sa_env):
    from database import SessionLocal, set_location_id
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext, ToolExecutionError

    a = _make_location("VehA")
    b = _make_location("VehB")

    db_b = SessionLocal()
    set_location_id(db_b, b["location_id"])
    _, vehicle_b = _seed_customer_and_vehicle(db_b, b["location_id"], name="B")
    db_b.commit()
    vehicle_b_id = vehicle_b.id
    db_b.close()

    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    customer_a, _ = _seed_customer_and_vehicle(db_a, a["location_id"], name="A")
    db_a.commit()
    ctx = ToolContext(session=db_a, location_id=a["location_id"], conversation_id=1, customer_id=customer_a.id)
    registry = ServiceAdvisorToolRegistry(ctx)
    with pytest.raises(ToolExecutionError, match="vehicle not found"):
        registry.get_vehicle(vehicle_b_id)
    with pytest.raises(ToolExecutionError, match="vehicle not found"):
        registry.get_vehicle_history(vehicle_b_id)
    with pytest.raises(ToolExecutionError):
        # An AI attempting to book Location B's vehicle from Location A's
        # session/context -- must fail before any booking is created.
        registry.create_booking(vehicle_id=vehicle_b_id, booking_date="2027-01-04", service_type="Oil change")
    db_a.close()


def test_tool_registry_rejects_same_location_wrong_customers_vehicle(sa_env):
    """Defense in depth beyond tenant isolation: within the SAME location,
    a vehicle belonging to a different customer than the current
    conversation's customer_id must also be rejected -- proven directly
    against ServiceAdvisorToolRegistry._vehicle()'s own extra check."""
    from database import SessionLocal, set_location_id
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext, ToolExecutionError

    loc = _make_location("SameLocDiffCustomer")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    customer_1, _ = _seed_customer_and_vehicle(db, loc["location_id"], name="One")
    customer_2, vehicle_2 = _seed_customer_and_vehicle(db, loc["location_id"], name="Two")
    db.commit()

    ctx = ToolContext(session=db, location_id=loc["location_id"], conversation_id=1, customer_id=customer_1.id)
    registry = ServiceAdvisorToolRegistry(ctx)
    with pytest.raises(ToolExecutionError, match="does not belong to current customer"):
        registry.get_vehicle(vehicle_2.id)
    db.close()


def test_tool_registry_rejects_foreign_location_escalation_reason_leak(sa_env):
    """escalate_to_human() must create its Task under the calling
    context's own location_id, never the target of any ID supplied in
    arguments (it takes no customer/location id at all -- confirms it
    can't be pointed at another tenant even in principle)."""
    from database import SessionLocal, set_location_id, query_db
    from integrations.ai.tools.registry import ServiceAdvisorToolRegistry, ToolContext

    a = _make_location("EscA")
    db = SessionLocal()
    set_location_id(db, a["location_id"])
    customer_a, _ = _seed_customer_and_vehicle(db, a["location_id"], name="A")
    db.commit()
    ctx = ToolContext(session=db, location_id=a["location_id"], conversation_id=42, customer_id=customer_a.id)
    registry = ServiceAdvisorToolRegistry(ctx)
    result = registry.escalate_to_human(reason="customer is distressed")
    db.commit()
    from database import raw_location_scope
    with raw_location_scope(a["location_id"]):
        row = query_db("SELECT location_id FROM tasks WHERE id=%s", (result["task_id"],), one=True)
    assert row["location_id"] == a["location_id"]
    db.close()


# ---------------------------------------------------------------------------
# Phase 10: the explicit shared-vs-isolated knowledge test named in the brief.
# ---------------------------------------------------------------------------

def test_service_rules_isolated_per_tenant_but_universal_rules_shared(sa_env):
    """Location A's tenant-specific rule must be invisible to Location B and
    vice versa, while a rule with no location_id (location_id IS NULL --
    the universal/automotive-knowledge convention ServiceRuleEngine already
    uses) must be visible to both."""
    from database import SessionLocal, set_location_id, execute_db, query_db, utc_now
    from models.core import Vehicle, Customer
    from ai.recommendations.rule_engine import ServiceRuleEngine

    a = _make_location("KnowledgeA")
    b = _make_location("KnowledgeB")

    # Location A's own tenant-specific rule (e.g. this workshop's own
    # brake-fluid interval), and a universal rule with no location_id at
    # all -- the shared/automotive-knowledge convention. A location_id IS
    # NULL rule is, by definition, global -- it would otherwise leak into
    # every other test in this same run that creates a vehicle in the
    # shared SQLite file (confirmed: it does, breaking an unrelated
    # follow_up test whose vehicles have no `year` set, which this
    # interval_months branch needs when there's no service history).
    # Delete both rows at the end so this test's fixtures don't outlive it.
    execute_db(
        "INSERT INTO service_rules (location_id, service_type, interval_months, interval_km, active, created_at, updated_at) "
        "VALUES (%s,'Brake fluid flush (workshop A only)',24,30000,TRUE,%s,%s)",
        (a["location_id"], utc_now(), utc_now()),
    )
    execute_db(
        "INSERT INTO service_rules (location_id, service_type, interval_months, interval_km, active, created_at, updated_at) "
        "VALUES (NULL,'Universal oil change',12,10000,TRUE,%s,%s)",
        (utc_now(), utc_now()),
    )

    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    customer_a = Customer(location_id=a["location_id"], first_name="A", last_name="Cust", whatsapp_number="27821130001")
    db_a.add(customer_a); db_a.flush()
    vehicle_a = Vehicle(location_id=a["location_id"], customer_id=customer_a.id, make="Toyota", model="Corolla", year=2020, mileage=35000)
    db_a.add(vehicle_a); db_a.flush()
    db_a.commit()

    engine_a = ServiceRuleEngine(db_a, a["location_id"])
    result_a = engine_a.due_services(vehicle_a.id)
    service_types_seen_by_a = {item["service_type"] for item in result_a["due_services"]}
    db_a.close()

    assert "Brake fluid flush (workshop A only)" in service_types_seen_by_a, (
        "Location A must see its own tenant-specific rule"
    )
    assert "Universal oil change" in service_types_seen_by_a, (
        "a location_id IS NULL rule (shared/automotive knowledge) must be visible to every location"
    )

    db_b = SessionLocal()
    set_location_id(db_b, b["location_id"])
    customer_b = Customer(location_id=b["location_id"], first_name="B", last_name="Cust", whatsapp_number="27821130002")
    db_b.add(customer_b); db_b.flush()
    vehicle_b = Vehicle(location_id=b["location_id"], customer_id=customer_b.id, make="Toyota", model="Corolla", year=2020, mileage=35000)
    db_b.add(vehicle_b); db_b.flush()
    db_b.commit()

    engine_b = ServiceRuleEngine(db_b, b["location_id"])
    result_b = engine_b.due_services(vehicle_b.id)
    service_types_seen_by_b = {item["service_type"] for item in result_b["due_services"]}
    db_b.close()

    assert "Brake fluid flush (workshop A only)" not in service_types_seen_by_b, (
        "Location B must never see Location A's tenant-specific rule -- knowledge leak"
    )
    assert "Universal oil change" in service_types_seen_by_b, (
        "the same universal rule must also be visible to Location B -- shared knowledge must not be accidentally scoped away"
    )

    # Cleanup: a location_id IS NULL rule is global by definition, so it
    # would otherwise outlive this test in the shared SQLite file and
    # affect every other test in the same run that creates a vehicle
    # without a `year` (confirmed: it broke an unrelated follow_up test).
    execute_db(
        "DELETE FROM service_rules WHERE service_type IN "
        "('Brake fluid flush (workshop A only)', 'Universal oil change')"
    )
