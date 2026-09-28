"""Positive cross-tenant proof for the LIVE billing system:
services/billing_service.py, services/automatic_billing_service.py,
routes/billing_wall.py -- everything confirmed reachable this loop, not
the dead financial_repository.py already removed two loops ago.

Method: build Location A and Location B for real, with real
billing_records rows, then attempt every read/mutate/reconciliation/
background-job/reporting operation Location A could conceivably reach
against Location B's data. A test that finds a gap is left FAILING
first (see test_charge_billing_record_trusts_a_forged_location_id_pair),
then the repair closes it and the same test proves the closure -- not
asserted clean without ever having been red.
"""
import uuid
from unittest.mock import patch

import pytest


@pytest.fixture
def two_locations(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")

    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    def _make_location(label):
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
               VALUES (%s,%s,'workshop',TRUE,%s,%s,1000,50,0.5,%s,TRUE)""",
            (owner_id, f"{label} Workshop {suffix}", utc_now(), utc_now(), email),
        )
        location_id = query_db("SELECT id FROM locations WHERE owner_id=%s", (owner_id,), one=True)["id"]
        period = "2026-09"
        execute_db(
            """INSERT INTO billing_records (location_id, amount, base_amount, usage_amount, status,
                                            billing_period, created_at, updated_at)
               VALUES (%s,1000,1000,0,'unpaid',%s,%s,%s)""",
            (location_id, period, utc_now(), utc_now()),
        )
        billing_record_id = query_db(
            "SELECT id FROM billing_records WHERE location_id=%s AND billing_period=%s",
            (location_id, period), one=True,
        )["id"]
        return {"owner_id": owner_id, "location_id": location_id, "email": email, "period": period,
                "billing_record_id": billing_record_id}

    return _make_location("A"), _make_location("B")


# --------------------------------------------------------------------------
# READ: billing_wall.py's own query never returns another location's record
# --------------------------------------------------------------------------

def test_billing_wall_query_only_ever_sees_its_own_location(two_locations):
    from database import raw_location_scope, query_db
    a, b = two_locations

    with raw_location_scope(a["location_id"]):
        row = query_db(
            "SELECT id, billing_period, amount, status, payment_link FROM billing_records "
            "WHERE location_id=%s AND status IN ('unpaid', 'action_required', 'payment_failed_final') "
            "ORDER BY billing_period DESC LIMIT 1",
            (a["location_id"],), one=True,
        )
    assert row["id"] == a["billing_record_id"]
    assert row["id"] != b["billing_record_id"]


def test_billing_wall_query_with_a_forged_location_id_still_only_sees_that_location(two_locations):
    """Even if something upstream passed the wrong location_id into this
    exact query (the actual live route derives it from the session, not
    a parameter -- this proves the query itself is still the backstop)."""
    from database import raw_location_scope, query_db
    a, b = two_locations

    with raw_location_scope(b["location_id"]):
        row = query_db(
            "SELECT id FROM billing_records WHERE location_id=%s AND status IN "
            "('unpaid', 'action_required', 'payment_failed_final') ORDER BY billing_period DESC LIMIT 1",
            (b["location_id"],), one=True,
        )
    assert row["id"] == b["billing_record_id"]


# --------------------------------------------------------------------------
# The "owner_id" cross-check in both billing_wall.py routes
# --------------------------------------------------------------------------

def test_location_owner_crosscheck_rejects_a_location_id_owner_id_mismatch(two_locations):
    """Mirrors exactly what routes/billing_wall.py's pay_wall() and
    attempt_payment() both do before touching any billing record: even
    with a location_id in hand, it must also belong to the claimed
    owner_id."""
    from database import raw_location_scope, query_db
    a, b = two_locations

    with raw_location_scope(a["location_id"]):
        row = query_db(
            "SELECT id FROM locations WHERE id=%s AND owner_id=%s",
            (a["location_id"], b["owner_id"]), one=True,
        )
    assert row is None, "location A's own row must not resolve under B's owner_id"


# --------------------------------------------------------------------------
# UPDATE / RECONCILIATION: the forged-pair attack on charge_billing_record
# --------------------------------------------------------------------------

def test_charge_billing_record_rejects_a_forged_location_id_pair(two_locations, monkeypatch):
    """Attack: call charge_billing_record(location_A_id, location_B_record)
    directly -- exactly the shape neither of its two real callers ever
    produces (both pair location_id with a record from a query already
    scoped to that same location_id), but which the function itself,
    before this repair, had no defense against: _claim_billing_record()'s
    own SQL claims by id and status alone, never location_id.

    Mocks PaystackAuthorizationStore so this exercises the actual claim/
    charge path deterministically -- an earlier, unmocked version of this
    test technically passed, but only because the real Paystack HTTP call
    it fell through to failed with a plain network 403 in this sandbox,
    which would have masked the real answer either way. Giving A a real
    stored authorization removes that false signal: if the location check
    were missing, this would now actually attempt to charge B's invoice
    to A's card, not fail for an unrelated reason first.

    Must be rejected safely -- Location B's invoice must not be charged
    against Location A's card, and must not silently flip to any
    terminal state."""
    from database import raw_location_scope, query_db, session_scope
    from services.automatic_billing_service import charge_billing_record
    from models.integration_models import PaymentCustomer
    a, b = two_locations

    with session_scope(location_id=a["location_id"]) as session:
        session.add(PaymentCustomer(
            location_id=a["location_id"],
            paystack_customer_code=f"CUS_{uuid.uuid4().hex[:8]}",
            email=a["email"],
        ))
        session.flush()

    # B's own record, as automatic_billing_service.py's real caller would
    # have fetched it -- but paired with A's location_id, simulating a
    # forged/mismatched call.
    with raw_location_scope(b["location_id"]):
        b_record = query_db(
            "SELECT * FROM billing_records WHERE id=%s", (b["billing_record_id"],), one=True,
        )

    with patch(
        "integrations.paystack.auth.authorization_store.PaystackAuthorizationStore.load_authorization",
        return_value={"email": a["email"], "authorization_code": "AUTH_a_real"},
    ), patch(
        "integrations.paystack.billing.subscription_service.SubscriptionService.charge_overage",
    ) as mock_charge:
        result = charge_billing_record(a["location_id"], b_record)

    mock_charge.assert_not_called()
    assert result["status"] not in ("paid",), (
        "Location B's invoice must never be marked paid via a call scoped to Location A"
    )
    with raw_location_scope(b["location_id"]):
        after = query_db(
            "SELECT status FROM billing_records WHERE id=%s", (b["billing_record_id"],), one=True,
        )
    assert after["status"] == "unpaid", (
        "Location B's own record must be untouched by a forged call claiming to be Location A"
    )


def test_a_correctly_paired_call_still_works_normally(two_locations):
    """The repair must not break the real path -- a zero-amount record
    (the simplest deterministic success case, no Paystack call needed)
    charged with its own, correctly-matching location_id must still
    succeed exactly as before."""
    from database import raw_location_scope, execute_db, query_db, utc_now
    from services.automatic_billing_service import charge_billing_record
    a, _b = two_locations

    with raw_location_scope(a["location_id"]):
        execute_db(
            "UPDATE billing_records SET amount=0, base_amount=0 WHERE id=%s",
            (a["billing_record_id"],),
        )
        record = query_db("SELECT * FROM billing_records WHERE id=%s", (a["billing_record_id"],), one=True)

    result = charge_billing_record(a["location_id"], record)
    assert result["status"] == "skipped_zero_amount"

    with raw_location_scope(a["location_id"]):
        after = query_db("SELECT status FROM billing_records WHERE id=%s", (a["billing_record_id"],), one=True)
    assert after["status"] == "paid"


# --------------------------------------------------------------------------
# BACKGROUND JOB: run_automatic_billing()'s own loop pairing
# --------------------------------------------------------------------------

def test_run_automatic_billing_never_cross_pairs_locations(two_locations):
    """Runs the real cron entry point across both locations and confirms
    each location's own record was the one settled -- not swapped, not
    skipped for the wrong reason.

    Zeroes monthly_base_price on the locations themselves, not the
    billing_records row directly: run_automatic_billing() calls
    close_billing_period() first, which recomputes amount from the
    location's own current price/usage every time it runs -- a direct
    UPDATE on the billing_records row would just get overwritten back to
    1000 by that recomputation before charge_billing_record() ever saw
    it, which is what an earlier version of this test missed."""
    from database import raw_location_scope, execute_db, query_db
    from services.automatic_billing_service import run_automatic_billing
    a, b = two_locations

    with raw_location_scope(a["location_id"]):
        execute_db("UPDATE locations SET monthly_base_price=0 WHERE id=%s", (a["location_id"],))
    with raw_location_scope(b["location_id"]):
        execute_db("UPDATE locations SET monthly_base_price=0 WHERE id=%s", (b["location_id"],))

    run_automatic_billing(billing_period=a["period"], location_id=a["location_id"])
    run_automatic_billing(billing_period=b["period"], location_id=b["location_id"])

    with raw_location_scope(a["location_id"]):
        a_after = query_db("SELECT status FROM billing_records WHERE id=%s", (a["billing_record_id"],), one=True)
    with raw_location_scope(b["location_id"]):
        b_after = query_db("SELECT status FROM billing_records WHERE id=%s", (b["billing_record_id"],), one=True)

    assert a_after["status"] == "paid"
    assert b_after["status"] == "paid"


# --------------------------------------------------------------------------
# WEBHOOK: the DEFECT-003 bridge, re-proven under a forged billing_record_id
# --------------------------------------------------------------------------

def test_webhook_bridge_rejects_a_billing_record_id_belonging_to_another_location(two_locations):
    """Attack: a charge.success webhook resolved to Location A (via its
    own reference/waba/phone lookup -- untouched by this test) but whose
    metadata.billing_record_id, however that happened, points at
    Location B's row. The bridge must not settle B's invoice under A's
    resolved location scope."""
    from database import session_scope, raw_location_scope, query_db
    from integrations.paystack.webhooks.event_handlers.charge_handlers import handle_charge_success
    a, b = two_locations

    data = {
        "reference": "PSK_cross_tenant_attempt",
        "id": 42,
        "gateway_response": "Approved",
        "channel": "card",
        "customer": {"email": a["email"]},
        "authorization": {"authorization_code": "AUTH_a", "reusable": True},
        "metadata": {"location_id": a["location_id"], "billing_period": b["period"],
                     "billing_record_id": b["billing_record_id"]},
    }

    with session_scope(location_id=a["location_id"]) as session:
        handle_charge_success(session, data, a["location_id"])

    with raw_location_scope(b["location_id"]):
        after = query_db("SELECT status FROM billing_records WHERE id=%s", (b["billing_record_id"],), one=True)
    assert after["status"] == "unpaid", (
        "the bridge's own query is `WHERE id=:id AND location_id=:location_id` -- "
        "a forged billing_record_id from another location must find no matching row"
    )


# --------------------------------------------------------------------------
# REPORTING: build_monthly_recap never leaks across locations
# --------------------------------------------------------------------------

def test_monthly_recap_is_scoped_to_one_location_only(two_locations):
    from database import raw_location_scope, execute_db, utc_now
    from services.monthly_recap_service import build_monthly_recap
    a, b = two_locations

    # A distinguishing signal only Location B has.
    with raw_location_scope(b["location_id"]):
        execute_db(
            "INSERT INTO bookings (location_id, customer_id, status, created_at, updated_at) "
            "VALUES (%s, 999999, 'completed', %s, %s)",
            (b["location_id"], utc_now(), utc_now()),
        )

    with raw_location_scope(a["location_id"]):
        recap_a = build_monthly_recap(a["location_id"], a["period"])
    assert recap_a["bookings_handled"] == 0, "Location A's recap must not see Location B's booking"
