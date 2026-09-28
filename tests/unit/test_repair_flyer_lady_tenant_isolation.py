"""Live, behavioral proof for System 7's Phase 5/6/7 adversarial checks.

No repair was needed for anything covered here -- flyer_lady/publish_service.py
already explicitly location-scopes its Special lookup, flyer_lady/models.py's
SpecialPost already has a real UniqueConstraint("special_id", "platform"),
and jobs/flyer_lady.py already uses a claim-then-commit, per-post-transaction
pattern with documented historical reasoning for exactly the duplicate-publish
risk this brief asks about. This file closes the proof gap Phase 12 asks for
(a real cross-location attempt, not a static source-inspection check) and adds
one explicit, permanent regression guard for the WhatsApp boundary.
"""
import uuid
from datetime import datetime, timezone

import pytest


@pytest.fixture
def fl_env(monkeypatch):
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


def _seed_special(db, location_id, *, approved=False):
    from flyer_lady.models import Special, SpecialApproval
    special = Special(
        location_id=location_id, created_by="test", text="20% off oil changes this week",
        booking_link="https://example.test/book", status="draft",
    )
    db.add(special); db.flush()
    if approved:
        db.add(SpecialApproval(location_id=location_id, special_id=special.id, decision="approved", decided_by="staff"))
        db.flush()
    return special


# ---------------------------------------------------------------------------
# Phase 5: real cross-location attempts, not static source inspection.
# ---------------------------------------------------------------------------

def test_publish_post_rejects_special_from_foreign_location(fl_env):
    from database import SessionLocal, set_location_id
    from flyer_lady.models import SpecialPost
    from flyer_lady.publish_service import FlyerLadyPublishService

    a = _make_location("PubA")
    b = _make_location("PubB")

    db_b = SessionLocal()
    set_location_id(db_b, b["location_id"])
    special_b = _seed_special(db_b, b["location_id"], approved=True)
    post_b = SpecialPost(location_id=b["location_id"], special_id=special_b.id, platform="facebook_feed", status="pending")
    db_b.add(post_b); db_b.flush()
    db_b.commit()
    post_b_id = post_b.id
    db_b.close()

    # Location A's own session attempts to publish Location B's post by
    # passing A's own location_id alongside B's post object -- the exact
    # "foreign object ID" attack this engagement has tested at every other
    # system.
    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    post_from_a_session = db_a.get(SpecialPost, post_b_id)
    service = FlyerLadyPublishService()
    with pytest.raises(ValueError, match="special not found"):
        service.publish_post(db_a, a["location_id"], post_from_a_session)
    db_a.close()


def test_claim_due_posts_only_claims_own_locations_posts(fl_env):
    """The queue-claiming query itself must never cross the location
    boundary -- proven directly against _claim_due_posts(), not just
    inferred from the WHERE clause."""
    from database import SessionLocal, set_location_id
    from flyer_lady.models import SpecialPost
    from jobs.flyer_lady import _claim_due_posts

    a = _make_location("ClaimA")
    b = _make_location("ClaimB")

    for loc in (a, b):
        db = SessionLocal()
        set_location_id(db, loc["location_id"])
        special = _seed_special(db, loc["location_id"], approved=True)
        post = SpecialPost(location_id=loc["location_id"], special_id=special.id, platform="facebook_feed", status="pending")
        db.add(post); db.flush()
        db.commit()
        loc["post_id"] = post.id
        db.close()

    db_a = SessionLocal()
    set_location_id(db_a, a["location_id"])
    claimed = _claim_due_posts(db_a, a["location_id"], limit=20, now=datetime.now(timezone.utc))
    db_a.close()

    assert a["post_id"] in claimed
    assert b["post_id"] not in claimed, "claiming for Location A must never claim Location B's queued post"


def test_unapproved_special_cannot_publish(fl_env):
    from database import SessionLocal, set_location_id
    from flyer_lady.models import SpecialPost
    from flyer_lady.publish_service import FlyerLadyPublishService

    loc = _make_location("Unapproved")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    special = _seed_special(db, loc["location_id"], approved=False)
    post = SpecialPost(location_id=loc["location_id"], special_id=special.id, platform="facebook_feed", status="pending")
    db.add(post); db.flush()
    db.commit()

    service = FlyerLadyPublishService()
    with pytest.raises(ValueError, match="has not been approved"):
        service.publish_post(db, loc["location_id"], post)
    db.close()


def test_duplicate_post_for_same_special_and_platform_is_rejected_by_schema(fl_env):
    """SpecialPost's UniqueConstraint("special_id", "platform") is the
    actual duplicate-publish guard Phase 8 asks about -- proven live
    against a real insert attempt, not just read from the model
    definition."""
    from database import SessionLocal, set_location_id
    from flyer_lady.models import SpecialPost
    from sqlalchemy.exc import IntegrityError

    loc = _make_location("DupeSchema")
    db = SessionLocal()
    set_location_id(db, loc["location_id"])
    special = _seed_special(db, loc["location_id"], approved=True)
    db.add(SpecialPost(location_id=loc["location_id"], special_id=special.id, platform="facebook_feed", status="pending"))
    db.commit()

    db.add(SpecialPost(location_id=loc["location_id"], special_id=special.id, platform="facebook_feed", status="pending"))
    with pytest.raises(IntegrityError):
        db.commit()
    db.rollback()
    db.close()


# ---------------------------------------------------------------------------
# Phase 6: explicit, permanent regression guard for the WhatsApp boundary.
# ---------------------------------------------------------------------------

def test_flyer_lady_has_no_customer_messaging_import_path():
    """Confirmed clean by exhaustive grep across flyer_lady/ and
    advertising/ this loop: zero references to MetaMessagingService,
    Conversation, or Message anywhere. This test makes that a permanent,
    automatic regression guard rather than a one-time manual check --
    if a future change ever imports the customer-messaging pipes into
    Flyer Lady, this fails immediately rather than requiring another
    full manual audit to notice."""
    import ast
    from pathlib import Path

    forbidden_names = {"MetaMessagingService", "Conversation", "Message"}
    root = Path(__file__).resolve().parents[2]
    violations = []
    for package in ("flyer_lady", "advertising"):
        for path in (root / package).rglob("*.py"):
            tree = ast.parse(path.read_text(), filename=str(path))
            for node in ast.walk(tree):
                if isinstance(node, ast.ImportFrom):
                    for alias in node.names:
                        if alias.name in forbidden_names:
                            violations.append(f"{path.relative_to(root)}: imports {alias.name}")
                elif isinstance(node, ast.Import):
                    for alias in node.names:
                        if alias.name.split(".")[-1] in forbidden_names:
                            violations.append(f"{path.relative_to(root)}: imports {alias.name}")

    assert not violations, (
        "Flyer Lady/advertising must never import the customer-messaging pipes "
        f"directly: {violations}"
    )
