"""Master dashboard: per-workshop status, "needs attention" flags, activity,
security events, team and VANTA's own setup checklist/notes.

Every function takes a platform-admin session (database.get_platform_session),
so RLS lets it read across workshops. Bulk functions take a list of location ids
and run one grouped query per source, not one per workshop.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

from sqlalchemy import func, select, text

from flyer_lady.models import FlyerBoost, Special
from models.core import Booking, Location, Message
from models.integration_models import (
    AIUsageLog, ClientAdminRecord, GoogleAdsConnection, MetaAdAccountConnection,
    MetaBusinessConnection, MetaSocialConnection, MetaWebhookEvent, Subscription,
)

SETUP_CHECKLIST = (
    ("authority", "Written permission from the owner received"),
    ("page_access", "Facebook Page full control checked"),
    ("tester", "Meta tester added and invites accepted"),
    ("account", "PHANTA account created, opening hours set"),
    ("whatsapp", "WhatsApp connected"),
    ("whatsapp_card", "Card added for WhatsApp messages"),
    ("facebook", "Facebook Page connected"),
    ("ads", "Ad account connected (ZAR, card added)"),
    ("google", "Google Ads connected (ZAR, card added)"),
    ("tests", "All tests passed with the owner"),
    ("handover", "Owner shown Inbox, Flyer Lady and Results"),
)
FAILED_LOGIN_ALERT = 10  # failed logins on one workshop's accounts in 24 hours
QUIET_DAYS = 7
BAD_SUBSCRIPTION = {"past_due", "failed", "unpaid", "cancelled", "canceled"}


def _by_location(session, model, ids):
    return {r.location_id: r for r in session.scalars(select(model).where(model.location_id.in_(ids))).all()}


def _counts(session, column, location_col, ids, *where):
    rows = session.execute(select(location_col, func.count(column)).where(location_col.in_(ids), *where).group_by(location_col)).all()
    return {loc: int(n) for loc, n in rows}


def _failed_logins(session, ids, since: datetime) -> dict[int, int]:
    if not ids:
        return {}
    rows = session.execute(text(
        "SELECT location_id, COUNT(*) FROM security_events WHERE outcome='failure' AND created_at >= :since "
        "AND location_id IN (" + ",".join(str(int(i)) for i in ids) + ") GROUP BY location_id"
    ), {"since": since.replace(tzinfo=None, microsecond=0).isoformat()}).all()
    return {int(loc): int(n) for loc, n in rows}


def overview(session, locations, now: datetime | None = None) -> dict[int, dict]:
    """{location_id: {status columns..., "flags": [..]}} for the client list."""
    now = now or datetime.now(timezone.utc)
    ids = [loc.id for loc in locations]
    if not ids:
        return {}
    week, day = now - timedelta(days=QUIET_DAYS), now - timedelta(days=1)
    wa = _by_location(session, MetaBusinessConnection, ids)
    fb = _by_location(session, MetaSocialConnection, ids)
    ads = _by_location(session, MetaAdAccountConnection, ids)
    google = _by_location(session, GoogleAdsConnection, ids)
    subs = {}
    for s in session.scalars(select(Subscription).where(Subscription.location_id.in_(ids)).order_by(Subscription.id.desc())).all():
        subs.setdefault(s.location_id, s)
    bookings_week = _counts(session, Booking.id, Booking.location_id, ids, Booking.created_at >= week)
    chats_week = _counts(session, Message.id, Message.location_id, ids, Message.direction == "inbound", Message.created_at >= week)
    webhook_fails = _counts(session, MetaWebhookEvent.id, MetaWebhookEvent.location_id, ids,
                            MetaWebhookEvent.processing_status.in_(["failed", "error"]), MetaWebhookEvent.received_at >= week)
    failed_logins = _failed_logins(session, ids, day)
    last_booking = dict(session.execute(select(Booking.location_id, func.max(Booking.created_at)).where(Booking.location_id.in_(ids)).group_by(Booking.location_id)).all())
    prices = {int(r[0]): float(r[1] or 0) for r in session.execute(text(
        "SELECT id, monthly_base_price FROM locations WHERE id IN (" + ",".join(str(int(i)) for i in ids) + ")")).all()}
    last_message = dict(session.execute(select(Message.location_id, func.max(Message.created_at)).where(Message.location_id.in_(ids)).group_by(Message.location_id)).all())

    def soon(expires):
        if not expires:
            return False
        expires = expires if expires.tzinfo else expires.replace(tzinfo=timezone.utc)
        return expires <= now + timedelta(days=7)

    out = {}
    for loc in locations:
        i, flags = loc.id, []
        if (c := wa.get(i)) and c.connection_status != "connected":
            flags.append(f"WhatsApp: {c.connection_status.replace('_', ' ')}")
        if (c := fb.get(i)) and c.connection_status != "connected":
            flags.append("Facebook posting disconnected")
        if (c := ads.get(i)) and c.connection_status != "connected":
            flags.append("Facebook ads disconnected")
        if (c := google.get(i)) and c.connection_status != "connected":
            flags.append("Google Ads disconnected")
        if any(soon(getattr(c, "token_expires_at", None)) for c in (fb.get(i), ads.get(i)) if c and c.connection_status == "connected"):
            flags.append("Meta connection expires within 7 days")
        if webhook_fails.get(i):
            flags.append(f"{webhook_fails[i]} failed WhatsApp event(s) this week")
        if loc.access_locked and not loc.billing_exempt:
            flags.append("Payment wall locked")
        if (s := subs.get(i)) and (s.status or "").lower() in BAD_SUBSCRIPTION:
            flags.append(f"Billing: {s.status}")
        if failed_logins.get(i, 0) >= FAILED_LOGIN_ALERT:
            flags.append(f"{failed_logins[i]} failed logins in 24 hours")
        created = loc.created_at if (loc.created_at is None or loc.created_at.tzinfo) else loc.created_at.replace(tzinfo=timezone.utc)
        if loc.active and created and created < week and not bookings_week.get(i) and not chats_week.get(i):
            flags.append(f"No bookings or WhatsApp chats in {QUIET_DAYS} days")
        last = max([d for d in (last_booking.get(i), last_message.get(i)) if d], default=None)
        out[i] = {
            "facebook_ads": ads[i].connection_status if i in ads else None,
            "google_ads": google[i].connection_status if i in google else None,
            "whatsapp_quality": wa[i].quality_rating if i in wa else None,
            "whatsapp_tier": wa[i].messaging_tier if i in wa else None,
            "monthly_price": prices.get(i, 0.0),
            "last_activity": last,
            "failed_logins_24h": failed_logins.get(i, 0),
            "flags": flags,
        }
    return out


def activity(session, location_id: int, now: datetime | None = None, days: int = 30) -> dict:
    now = now or datetime.now(timezone.utc)
    since = now - timedelta(days=days)
    one = lambda q: session.scalar(q) or 0
    return {
        "days": days,
        "bookings": int(one(select(func.count(Booking.id)).where(Booking.location_id == location_id, Booking.created_at >= since))),
        "whatsapp_messages_in": int(one(select(func.count(Message.id)).where(Message.location_id == location_id, Message.direction == "inbound", Message.created_at >= since))),
        "specials": int(one(select(func.count(Special.id)).where(Special.location_id == location_id, Special.created_at >= since))),
        "ad_budget": int(one(select(func.sum(FlyerBoost.budget_minor)).where(FlyerBoost.location_id == location_id, FlyerBoost.created_at >= since))) // 100,
        "ai_cost": float(one(select(func.sum(AIUsageLog.estimated_cost)).where(AIUsageLog.location_id == location_id, AIUsageLog.created_at >= since))),
    }


def security(session, location_id: int, limit: int = 50) -> list[dict]:
    rows = session.execute(text(
        "SELECT event_type, outcome, identifier, ip_address, created_at FROM security_events "
        "WHERE location_id = :loc ORDER BY created_at DESC LIMIT :n"), {"loc": location_id, "n": limit}).mappings().all()
    return [dict(r) for r in rows]


def team(session, location_id: int) -> list[dict]:
    rows = session.execute(text(
        "SELECT id, COALESCE(email, username) AS login, full_name, role, active, last_login FROM users "
        "WHERE location_id = :loc ORDER BY role, id"), {"loc": location_id}).mappings().all()
    return [dict(r) for r in rows]


def admin_record(session, location_id: int) -> dict:
    row = session.get(ClientAdminRecord, location_id)
    done = json.loads(row.checklist_json or "{}") if row else {}
    return {
        "checklist": [{"key": k, "label": label, "done": bool(done.get(k))} for k, label in SETUP_CHECKLIST],
        "notes": row.notes if row else "",
        "updated_by": row.updated_by if row else None,
        "updated_at": row.updated_at if row else None,
    }


def save_admin_record(session, location_id: int, *, checked: set[str], notes: str, actor: str) -> None:
    row = session.get(ClientAdminRecord, location_id) or ClientAdminRecord(location_id=location_id)
    row.checklist_json = json.dumps({k: (k in checked) for k, _ in SETUP_CHECKLIST})
    row.notes = (notes or "")[:20000]
    row.updated_by, row.updated_at = actor[:255], datetime.now(timezone.utc)
    session.add(row)
