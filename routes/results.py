"""Results: everything PHANTA brought a workshop in a period -- web bookings per
Flyer Lady post, WhatsApp chats and bookings, boosts -- so the workshop can see
what it is paying for."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from flask import Blueprint, render_template, request
from sqlalchemy import func, select

from database import get_session
from flyer_lady.models import FlyerBoost, FlyerLinkClick, Special, SpecialPost
from helpers.location import current_location_id
from helpers.permission import MANAGER_ROLES, require_role
from models.core import Booking, Conversation
from services.auth_service import login_required

results_bp = Blueprint("results", __name__, url_prefix="/dashboard/results")

PERIODS = (7, 30, 90)
SOURCES = [  # (booking.source, label shown to the workshop, brought in by PHANTA?)
    ("flyer_lady", "Flyer Lady posts (booking link)", True),
    ("public_web", "Booking page (direct)", True),
    ("whatsapp", "WhatsApp (AI assistant)", True),
    ("manual", "Manual bookings by staff", False),
]


def results_for(session, location_id: int, since: datetime) -> dict:
    bookings = select(Booking.source, func.count()).where(
        Booking.location_id == location_id, Booking.created_at >= since, Booking.status != "cancelled",
    ).group_by(Booking.source)
    by_source = dict(session.execute(bookings).all())
    rows = [{"label": label, "count": by_source.get(key, 0), "phanta": phanta} for key, label, phanta in SOURCES]
    other = sum(n for key, n in by_source.items() if key not in {k for k, _, _ in SOURCES})
    if other:
        rows.append({"label": "Other", "count": other, "phanta": False})

    def per_special(column, model, *where):
        return dict(session.execute(select(column, func.count()).where(model.location_id == location_id, *where).group_by(column)).all())

    clicks = per_special(FlyerLinkClick.special_id, FlyerLinkClick, FlyerLinkClick.created_at >= since)
    booked = per_special(Booking.flyer_special_id, Booking, Booking.flyer_special_id.isnot(None),
                         Booking.created_at >= since, Booking.status != "cancelled")
    budgets = dict(session.execute(select(FlyerBoost.special_id, func.sum(FlyerBoost.budget_minor)).where(
        FlyerBoost.location_id == location_id, FlyerBoost.created_at >= since).group_by(FlyerBoost.special_id)).all())
    ids = set(clicks) | set(booked) | set(budgets)
    created = session.scalars(select(Special.id).where(Special.location_id == location_id, Special.created_at >= since)).all()
    ids |= set(created)
    specials = session.scalars(select(Special).where(Special.location_id == location_id, Special.id.in_(ids)).order_by(Special.created_at.desc())).all() if ids else []
    posts = [{"text": s.text, "created_at": s.created_at, "clicks": clicks.get(s.id, 0), "bookings": booked.get(s.id, 0),
              "boost_budget": (budgets.get(s.id) or 0) // 100} for s in specials]

    return {
        "sources": rows,
        "brought_in": sum(r["count"] for r in rows if r["phanta"]),
        "chats": session.scalar(select(func.count()).select_from(Conversation).where(
            Conversation.location_id == location_id, Conversation.started_at >= since)) or 0,
        "posts_published": session.scalar(select(func.count()).select_from(SpecialPost).where(
            SpecialPost.location_id == location_id, SpecialPost.status == "published", SpecialPost.published_at >= since)) or 0,
        "clicks": sum(clicks.values()),
        "boost_budget": sum(budgets.values()) // 100,
        "posts": posts,
    }


@results_bp.get("")
@login_required
@require_role(*MANAGER_ROLES)
def results():
    days = request.args.get("days", type=int)
    days = days if days in PERIODS else 30
    since = datetime.now(timezone.utc) - timedelta(days=days)
    session = get_session()
    try:
        data = results_for(session, current_location_id(), since)
    finally:
        session.close()
    return render_template("results.html", r=data, days=days, periods=PERIODS)
