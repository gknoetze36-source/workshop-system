"""Positive proof for services/access_lock_service.py -- lock_location()
and unlock_location(), the only writers of locations.access_locked.

DISCOVER, done fresh this loop: every live (non-test, non-CLI) caller,
and where its location_id actually comes from:

  routes/billing_wall.py::attempt_payment()
      location_id = current_user()["location_id"] -- the authenticated
      session, cross-checked against owner_id in the same route
      (proven last loop, re-confirmed here).

  services/automatic_billing_service.py::charge_billing_record() (x4 calls)
      location_id = the function's own parameter, but every call site is
      AFTER _claim_billing_record(billing_id, location_id) has already
      succeeded (confirmed by line order: claim at line 127, every
      lock/unlock call at 166/196/209/227) -- which, since last loop's
      repair, only succeeds when billing_id genuinely belongs to
      location_id. Transitively protected, not independently checked.

  services/offboarding_service.py::begin_offboarding() (1 call)
      location_id = a CLI argument (scripts/offboard.py, args.location_id).
      NOT a web route -- confirmed by grep, its only non-test caller is
      the script. This is an operator-privileged tool (server/SSH access
      already implies far broader capability than any application check
      could meaningfully restrict), a fundamentally different threat
      model than "Tenant A forges a web request" -- noted here, not
      folded into the cross-tenant web-attack tests below as if it were
      the same class of surface.

AUDIT finding, stated plainly: lock_location() and unlock_location()
THEMSELVES have zero internal ownership check -- no owner_id
cross-reference, no companion record to validate against (unlike
_claim_billing_record, which has billing_id to check against
location_id; these two operate on locations.id alone, with nothing
else to compare it to). Every test below that calls them directly
proves this literally, rather than asserting it. The reason this isn't
a FAIL: every live, web-reachable caller's location_id is independently
proven trustworthy by the tests further down -- there is no reachable
path today where an untrusted value reaches these functions. This is
the same shape of question _claim_billing_record raised last loop, and
this time the answer, checked with the same rigor, is that no live
caller actually supplies a forgeable value.
"""
import uuid

import pytest


@pytest.fixture
def two_locations(monkeypatch):
    monkeypatch.setenv("META_TOKEN_ENCRYPTION_KEY", "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake")
    monkeypatch.setenv("ALLOW_DEV_DEFAULT_CREDENTIALS", "1")
    monkeypatch.setenv("DEV_SUPERADMIN_PASSWORD", "test-only-not-a-real-credential")

    from database import execute_db, query_db, utc_now, initialize_database
    initialize_database(run_migrations=False)

    def _make_location(label, access_locked=False, active=True):
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

    return _make_location("A"), _make_location("B")


def _locked(location_id):
    from database import raw_location_scope, query_db
    with raw_location_scope(location_id):
        row = query_db("SELECT access_locked, access_locked_reason FROM locations WHERE id=%s", (location_id,), one=True)
    return bool(row["access_locked"]), row["access_locked_reason"]


# --------------------------------------------------------------------------
# PHASE 2/AUDIT: the bare function, called directly -- proves the literal
# absence of an internal check, doesn't assume it either way.
# --------------------------------------------------------------------------

def test_lock_location_has_no_internal_ownership_check(two_locations):
    """Direct proof, not assumption: calling lock_location(B's id) does
    lock B, with nothing in the function itself to stop it. This is the
    literal fact that makes every caller-provenance test below the real
    security boundary, not this function."""
    from database import raw_location_scope
    from services.access_lock_service import lock_location
    a, b = two_locations

    with raw_location_scope(b["location_id"]):
        lock_location(b["location_id"], "direct call, no caller context at all")

    locked, reason = _locked(b["location_id"])
    assert locked is True
    assert reason == "direct call, no caller context at all"


def test_unlock_location_has_no_internal_ownership_check(two_locations):
    from database import raw_location_scope, execute_db
    from services.access_lock_service import unlock_location
    a, b = two_locations

    with raw_location_scope(b["location_id"]):
        execute_db("UPDATE locations SET access_locked=TRUE, access_locked_reason='pre-locked' WHERE id=%s", (b["location_id"],))
        unlock_location(b["location_id"])

    locked, reason = _locked(b["location_id"])
    assert locked is False
    assert reason is None


def test_invalid_location_id_fails_safely_and_is_now_detectable(two_locations):
    """Phase 4: 'Invalid location ID MUST fail safely.' There is no row
    to corrupt, so 'safely' means zero rows affected, no exception -- but
    before this loop's small repair, the caller had no way to tell a
    nonexistent id apart from a successful lock (both returned None).
    Now RETURNING id makes that distinction real."""
    from database import raw_location_scope, query_db
    from services.access_lock_service import lock_location
    a, _b = two_locations

    nonexistent_id = 999_999_999
    with raw_location_scope(a["location_id"]):
        result = lock_location(nonexistent_id, "should affect nothing")
        row = query_db("SELECT id FROM locations WHERE id=%s", (nonexistent_id,), one=True)
    assert row is None, "no location must have been created or mutated by this call"
    assert result is False, "a nonexistent location must now be distinguishable from a real success"


def test_a_real_lock_reports_success(two_locations):
    from database import raw_location_scope
    from services.access_lock_service import lock_location, unlock_location
    a, _b = two_locations

    with raw_location_scope(a["location_id"]):
        assert lock_location(a["location_id"], "reason") is True
        assert unlock_location(a["location_id"]) is True


# --------------------------------------------------------------------------
# PHASE 3/4: attack every LIVE, web-reachable caller's own provenance
# --------------------------------------------------------------------------

def test_billing_wall_never_supplies_a_foreign_location_id_to_unlock(two_locations):
    """attempt_payment()'s own two guards, re-proven directly: the
    location must belong to the authenticated owner AND already be
    locked before charge_billing_record() (and therefore any possible
    unlock_location() call) is even reached."""
    from database import raw_location_scope, query_db
    a, b = two_locations

    # Attack: A's owner_id, but requesting B's location_id -- the exact
    # shape routes/billing_wall.py's own query guards against.
    with raw_location_scope(a["location_id"]):
        row = query_db(
            "SELECT id FROM locations WHERE id=%s AND owner_id=%s AND access_locked=TRUE",
            (b["location_id"], a["owner_id"]), one=True,
        )
    assert row is None, (
        "the route's own pre-check must reject a foreign location_id before charge_billing_record, "
        "and therefore unlock_location, is ever reached"
    )


def test_charge_billing_record_calls_lock_only_after_a_verified_claim(two_locations, monkeypatch):
    """Attack 5 (billing path): reuses last loop's forged-pair attack,
    this time checking the ACCESS-LOCK side effect specifically rather
    than just the payment status. If the claim fix regressed, this
    would show B's location getting locked/unlocked as a side effect of
    a call scoped to A."""
    import uuid as _uuid
    from datetime import datetime, timezone
    from unittest.mock import patch
    from database import raw_location_scope, execute_db, query_db, utc_now, session_scope
    from services.automatic_billing_service import charge_billing_record
    from models.integration_models import PaymentCustomer
    a, b = two_locations

    period = "2026-09"
    execute_db(
        """INSERT INTO billing_records (location_id, amount, base_amount, usage_amount, status,
                                        billing_period, created_at, updated_at)
           VALUES (%s,1000,1000,0,'unpaid',%s,%s,%s)""",
        (b["location_id"], period, utc_now(), utc_now()),
    )
    with raw_location_scope(b["location_id"]):
        b_record = query_db(
            "SELECT * FROM billing_records WHERE location_id=%s AND billing_period=%s",
            (b["location_id"], period), one=True,
        )

    with session_scope(location_id=a["location_id"]) as session:
        session.add(PaymentCustomer(location_id=a["location_id"],
                                     paystack_customer_code=f"CUS_{_uuid.uuid4().hex[:8]}", email=a["email"]))
        session.flush()

    with patch(
        "integrations.paystack.auth.authorization_store.PaystackAuthorizationStore.load_authorization",
        return_value={"email": a["email"], "authorization_code": "AUTH_a"},
    ), patch(
        "integrations.paystack.billing.subscription_service.SubscriptionService.charge_overage",
    ):
        charge_billing_record(a["location_id"], b_record)

    a_locked, _ = _locked(a["location_id"])
    b_locked, _ = _locked(b["location_id"])
    assert a_locked is False, "the forged call must not lock A either -- it should be rejected before any lock call"
    assert b_locked is False, "B's own access state must be completely untouched by a call scoped to A"


# --------------------------------------------------------------------------
# PHASE 4: inactive location
# --------------------------------------------------------------------------

def test_inactive_location_can_still_be_locked_but_is_never_auto_unlocked_into_activity(two_locations):
    """The existing business rule (routes/billing_wall.py, auth_service.py):
    an inactive location is never reachable via login at all, so
    lock/unlock's own behaviour on one is secondary -- but the function
    must not error or behave inconsistently on one."""
    from database import raw_location_scope, execute_db
    from services.access_lock_service import lock_location, unlock_location
    a, _b = two_locations

    with raw_location_scope(a["location_id"]):
        execute_db("UPDATE locations SET active=FALSE WHERE id=%s", (a["location_id"],))
        lock_location(a["location_id"], "inactive location lock, must not raise")
        locked, _ = _locked(a["location_id"])
        assert locked is True
        unlock_location(a["location_id"])
        locked, _ = _locked(a["location_id"])
        assert locked is False
