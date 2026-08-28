"""The public booking page.

Every Flyer Lady special's booking_link has pointed here since the
feature was first built (services/location_service.py's
public_booking_url() already generates exactly this /book/<slug> shape,
and repositories/location_repository.py's get_location_for_public_
booking() already exists specifically to look up a location by slug for
this purpose) -- but the page itself never existed. A visitor clicking
"Book here" on any social post has been landing on a 404 this whole
time. This is that page.

Genuinely public: no login, no location context from a session --
everything is scoped by the slug in the URL, matching the same pattern
routes/flyer_lady.py's redirect_special() already uses for its own
public, unauthenticated entry point.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for

from extensions import limiter
from sqlalchemy import select

from database import location_transaction
from services.location_service import location_for_public_booking
from services.operating_hours_service import build_workshop_schedule

public_booking_bp = Blueprint("public_booking", __name__)


def _upcoming_days(location_id: int, count: int = 14):
    schedule = build_workshop_schedule(location_id)
    today = datetime.now(timezone.utc).date()
    days = []
    d = today
    while len(days) < count:
        windows = schedule.windows_for(d)
        days.append({
            "date": d.isoformat(),
            "label": d.strftime("%a %d %b"),
            "open": bool(windows),
            "opens_at": windows[0].start.strftime("%H:%M") if windows else None,
        })
        d += timedelta(days=1)
    return days


@public_booking_bp.get("/book/<slug>")
def show(slug):
    location = location_for_public_booking(slug)
    if not location:
        abort(404)
    return render_template("public_booking.html", location=location, days=_upcoming_days(location["id"]))


@public_booking_bp.post("/book/<slug>")
@limiter.limit("5 per minute; 30 per hour")
def submit(slug):
    location = location_for_public_booking(slug)
    if not location:
        abort(404)
    location_id = location["id"]

    full_name = (request.form.get("full_name") or "").strip()
    whatsapp_number = "".join(ch for ch in (request.form.get("whatsapp_number") or "") if ch.isdigit())
    vehicle_make = (request.form.get("vehicle_make") or "").strip()
    vehicle_model = (request.form.get("vehicle_model") or "").strip()
    vehicle_year_raw = (request.form.get("vehicle_year") or "").strip()
    service_type = (request.form.get("service_type") or "").strip()
    booking_date = (request.form.get("booking_date") or "").strip()

    errors = []
    if not full_name:
        errors.append("Please enter your name.")
    if len(whatsapp_number) < 9:
        errors.append("Please enter a valid WhatsApp number.")
    if not service_type:
        errors.append("Please describe what you need done.")
    if not booking_date:
        errors.append("Please choose a date.")

    schedule = build_workshop_schedule(location_id)
    day = None
    try:
        day = datetime.fromisoformat(booking_date).date()
    except ValueError:
        errors.append("That date isn't valid.")
    windows = schedule.windows_for(day) if day else ()
    if day and not windows:
        errors.append(f"The workshop is closed on {day.strftime('%A')}. Please choose another date.")

    if errors:
        for message in errors:
            flash(message, "error")
        return render_template("public_booking.html", location=location, days=_upcoming_days(location_id), form=request.form), 400

    name_parts = full_name.split(" ", 1)
    first_name = name_parts[0]
    last_name = name_parts[1] if len(name_parts) > 1 else ""

    from models.core import Customer, Vehicle

    # location_transaction() (not a plain get_session()) is what actually
    # makes this work under RLS in production -- confirmed the hard way:
    # get_session() gives no app.location_id, and every table this route
    # writes to has a FORCE ROW LEVEL SECURITY policy requiring it. First
    # version of this route used get_session() and passed every test on
    # SQLite and even against a real Postgres superuser connection --
    # both bypass RLS entirely, superuser unconditionally and SQLite by
    # not having RLS at all, so neither caught this. Only reproduced by
    # actually connecting as the same restricted, non-superuser role
    # production runs under: INSERT INTO customers failed with
    # "new row violates row-level security policy for table customers".
    # Matches the same fix already applied to routes/flyer_lady.py's
    # redirect_special() for the identical reason -- a public,
    # unauthenticated route has no ordinary session to set
    # app.location_id from automatically.
    with location_transaction(location_id) as session:
        customer = session.scalar(select(Customer).where(
            Customer.location_id == location_id, Customer.whatsapp_number == whatsapp_number,
            Customer.deleted_at.is_(None),
        ))
        if customer is None:
            customer = Customer(location_id=location_id, first_name=first_name, last_name=last_name, whatsapp_number=whatsapp_number)
            session.add(customer)
            session.flush()

        vehicle = Vehicle(
            location_id=location_id, customer_id=customer.id,
            make=vehicle_make or "Not specified", model=vehicle_model or "Not specified",
            # Optional on the form -- a customer often doesn't have the
            # exact model year to hand while booking. Falls back to the
            # current year rather than leaving this NOT NULL column unset,
            # since there's no real "unknown" sentinel for it.
            year=int(vehicle_year_raw) if vehicle_year_raw.isdigit() else datetime.now(timezone.utc).year,
        )
        session.add(vehicle)
        session.flush()

        opening = datetime.combine(day, windows[0].start, tzinfo=timezone.utc)
        end = opening + timedelta(minutes=60)

        from ai.booking.service import BookingService
        from ai.booking.availability import BookingAvailabilityService
        booking_service = BookingService(session, BookingAvailabilityService(session, schedule))
        try:
            booking = booking_service.create_booking(
                location_id=location_id, customer_id=customer.id, vehicle_id=vehicle.id,
                start_time=opening, end_time=end, service_type=service_type[:100],
                source="public_web", notes=(request.form.get("notes") or "")[:2000],
            )
        except ValueError as exc:
            # Explicit rollback before returning: location_transaction's
            # __exit__ only rolls back on a propagated exception -- simply
            # returning a response from inside the `with` block would let
            # it exit normally and commit the customer/vehicle rows
            # already added above, orphaned without the booking that was
            # the actual point of the whole submission.
            session.rollback()
            flash(str(exc), "error")
            return render_template("public_booking.html", location=location, days=_upcoming_days(location_id), form=request.form), 400

        # The web form submission itself is the customer's explicit
        # confirmation -- there's no automated prompt being replied to the
        # way there is for a WhatsApp-originated booking, so this goes
        # straight to 'confirmed' rather than through
        # BookingConfirmationService.confirm() (which is specifically
        # scoped to recording a reply to that prompt, and is hardcoded to
        # reject any channel other than "whatsapp" for exactly that
        # reason).
        booking.status = "confirmed"
        session.flush()

        # Best-effort: a workshop that hasn't connected WhatsApp yet must
        # still be able to take bookings through this page. A missing
        # connection should not block the booking itself from being saved.
        try:
            from integrations.meta.auth.config import MetaAuthConfig
            from integrations.meta.auth.token_store import MetaTokenStore
            from integrations.meta.services.graph_api_client import GraphApiClient
            from integrations.meta.messaging.messaging_service import MetaMessagingService
            from ai.communications.lifecycle import LifecycleCommunicationService

            messaging = MetaMessagingService(session, graph=GraphApiClient(MetaAuthConfig.from_env()), token_store=MetaTokenStore())
            lifecycle = LifecycleCommunicationService(session, messaging)
            lifecycle.booking_confirmed(booking)
            lifecycle.schedule_booking_reminder(booking)
        except Exception:
            pass

        booking_id = booking.id

    return redirect(url_for("public_booking.confirmed", slug=slug, booking_id=booking_id))


@public_booking_bp.get("/book/<slug>/confirmed/<int:booking_id>")
def confirmed(slug, booking_id):
    location = location_for_public_booking(slug)
    if not location:
        abort(404)
    from database import query_db
    booking = query_db(
        "SELECT start_time, service_type FROM bookings WHERE id=%s AND location_id=%s",
        (booking_id, location["id"]), one=True,
    )
    if not booking:
        abort(404)
    return render_template("public_booking_confirmed.html", location=location, booking=booking)
