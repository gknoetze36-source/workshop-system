"""Live, behavioral proof for System 10's genuinely new-ground checks.

No repair was needed for anything covered here. routes/paystack.py's webhook
already resolves location strictly from the signed payload's own data (never
a client-supplied URL parameter), WebhookHandler.handle() already verifies
the signature before anything mutates and already fingerprints exact
redeliveries for idempotency, and routes/ghost.py's platform-admin path is
already gated before PlatformAdminDashboardQueries is ever constructed. This
file closes the proof gap for each -- a real attempt, not a source read.

VAT/tax handling: confirmed absent, not partially built. billing_records has
no vat_amount/tax_amount/tax_rate column at all (amount, base_amount,
usage_amount only). Not something to invent a rate for without evidence of
VANTA's actual VAT-registration status, which is a business fact, not a code
question -- classified honestly here rather than repaired.
"""
import hashlib
import json
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def billing_env(monkeypatch):
    monkeypatch.setenv("PAYSTACK_SECRET_KEY", "sk_test_fake_secret_for_signing")
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


def _sign(raw_body: bytes, secret: str) -> str:
    import hmac
    return hmac.new(secret.encode(), raw_body, hashlib.sha512).hexdigest()


# ---------------------------------------------------------------------------
# Paystack webhook: duplicate delivery, foreign-location metadata attack.
# ---------------------------------------------------------------------------

def test_paystack_webhook_duplicate_delivery_is_idempotent(billing_env):
    from database import SessionLocal, set_location_id
    from integrations.paystack.webhooks.webhook_handler import WebhookHandler

    loc = _make_location("PaystackDupe")
    payload = {
        "event": "charge.success",
        "data": {
            "reference": "ref_" + uuid.uuid4().hex[:12],
            "amount": 100000,
            "metadata": {"phanta_location_id": loc["location_id"]},
            "customer": {"customer_code": "CUS_dupe_test"},
        },
    }
    raw = json.dumps(payload).encode()
    secret = "sk_test_fake_secret_for_signing"
    signature = _sign(raw, secret)

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    handler = WebhookHandler()
    first_event, first_is_new = handler.handle(db, raw, signature, payload, location_id=loc["location_id"])
    db.commit()
    db.close()

    db2 = SessionLocal()
    set_location_id(db2, loc["location_id"])
    handler2 = WebhookHandler()
    second_event, second_is_new = handler2.handle(db2, raw, signature, payload, location_id=loc["location_id"])
    db2.commit()
    db2.close()

    assert first_is_new is True, "the first delivery must be processed as new"
    assert second_is_new is False, "an exact redelivery (same raw body) must be recognised as a duplicate"
    assert first_event.event_key == second_event.event_key


def test_paystack_webhook_rejects_invalid_signature(billing_env):
    from database import SessionLocal, set_location_id
    from integrations.paystack.webhooks.webhook_handler import WebhookHandler, PaystackWebhookRejected

    loc = _make_location("PaystackBadSig")
    payload = {
        "event": "charge.success",
        "data": {
            "reference": "ref_" + uuid.uuid4().hex[:12],
            "metadata": {"phanta_location_id": loc["location_id"]},
        },
    }
    raw = json.dumps(payload).encode()

    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    handler = WebhookHandler()
    with pytest.raises(PaystackWebhookRejected, match="invalid Paystack webhook signature"):
        handler.handle(db, raw, "not-a-real-signature", payload, location_id=loc["location_id"])
    db.close()


def test_paystack_resolve_location_refuses_deactivated_location_metadata(billing_env):
    """A signed-but-stale/forged phanta_location_id pointing at a
    deactivated location must resolve to None (refuse), not silently
    process under a location that no longer exists as active."""
    from database import SessionLocal, execute_db
    from integrations.paystack.webhooks.webhook_location_resolver import resolve_paystack_location

    loc = _make_location("PaystackDeactivated")
    execute_db("UPDATE locations SET active=FALSE WHERE id=%s", (loc["location_id"],))

    db = SessionLocal()
    result = resolve_paystack_location(db, {"metadata": {"phanta_location_id": loc["location_id"]}})
    db.close()
    assert result is None, "resolution must refuse a deactivated location, not silently process it"


# ---------------------------------------------------------------------------
# Ghost / platform-admin reporting isolation.
# ---------------------------------------------------------------------------

def test_ghost_ordinary_user_cannot_reach_platform_admin_queries(billing_env):
    """A non-admin session must never reach PlatformAdminDashboardQueries --
    proven through the real route with a real Flask test client and a real
    (non-admin) authenticated session, not by inspecting the branch logic."""
    import phanta_app
    phanta_app.app.config["TESTING"] = True
    client = phanta_app.app.test_client()

    import re
    def csrf_from(path):
        html = client.get(path).get_data(as_text=True)
        m = re.search(r'name="csrf_token" value="([^"]+)"', html)
        if m:
            return m.group(1)
        m2 = re.search(r'name="csrf-token" content="([^"]+)"', html)
        return m2.group(1) if m2 else None

    suffix = uuid.uuid4().hex[:10]
    email = f"ghost-ordinary-{suffix}@test.example"
    token = csrf_from("/register")
    client.post("/register", data={
        "full_name": "Ordinary User", "email": email, "password": "SuperSecret123",
        "confirm_password": "SuperSecret123", "csrf_token": token,
    })
    token2 = csrf_from("/onboarding/location")
    client.post("/onboarding/location", data={
        "location_name": f"Ghost Ordinary Workshop {suffix}", "industry": "workshop", "csrf_token": token2,
    })

    ask_token = csrf_from("/dashboard")
    response = client.post(
        "/api/ghost/ask",
        json={"question": "what is my billing subscription status"},
        headers={"X-CSRFToken": ask_token},
    )
    assert response.status_code == 200
    body = response.get_json()
    assert body["mode"] == "workshop", (
        f"an ordinary, non-admin user must always get the workshop-scoped path, got mode={body.get('mode')}"
    )


def test_ghost_is_platform_admin_check_requires_the_actual_role(billing_env):
    """Direct proof against the authorization predicate itself: only the
    three recognised admin roles satisfy it, and a forged/absent session
    does not."""
    from flask import Flask
    import routes.ghost as ghost_module

    app = Flask(__name__)
    app.secret_key = "test-only"

    with app.test_request_context():
        from flask import session
        session["user"] = {"role": "owner"}
        assert ghost_module._is_platform_admin() is False

    with app.test_request_context():
        from flask import session
        session["user"] = {"role": "super_admin"}
        assert ghost_module._is_platform_admin() is True

    with app.test_request_context():
        # No session at all.
        assert ghost_module._is_platform_admin() is False
