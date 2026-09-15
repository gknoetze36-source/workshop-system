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

import logging
from datetime import datetime, timedelta, timezone

from flask import Blueprint, abort, flash, redirect, render_template, request, url_for

from extensions import limiter
from sqlalchemy import select

from database import location_transaction
from services.location_service import location_for_public_booking
from services.operating_hours_service import build_workshop_schedule

logger = logging.getLogger(__name__)

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

        # A number typed into a web form is unproven -- unlike a
        # WhatsApp-originated booking, nobody has replied from it yet. The
        # booking stays at its model default of 'pending' (create_booking()
        # already put the slot itself out of bounds for anyone else via
        # BookingAvailabilityService, so nothing is lost by not confirming
        # immediately) and BookingConfirmationService.confirm() -- the same
        # WhatsApp yes/no path an AI-originated booking already uses --
        # becomes the only way it reaches 'confirmed'.
<<<<<<< HEAD
        from integrations.meta.auth.config import MetaAuthConfig
=======
        from integrations.meta.auth.capability_config import WhatsAppMetaConfig
>>>>>>> meta-app-seperation
        from integrations.meta.auth.token_store import MetaTokenStore
        from integrations.meta.services.graph_api_client import GraphApiClient
        from integrations.meta.messaging.messaging_service import MetaMessagingError, MetaMessagingService
        from ai.communications.lifecycle import LifecycleCommunicationService
        from models.integration_models import MetaBusinessConnection

        # Whether WhatsApp can even be attempted at all -- the shared Meta
        # App itself might not be configured on this deployment (a
<<<<<<< HEAD
        # RuntimeError from MetaAuthConfig.from_env(), distinct from and
=======
        # RuntimeError from WhatsAppMetaConfig.from_env(), distinct from and
>>>>>>> meta-app-seperation
        # checked before any single location's own connection), or this
        # workshop specifically might not have connected WhatsApp yet.
        # Checked directly rather than inferred from which error a send
        # attempt raises -- send_utility_template() looks up the template
        # before it ever reaches the per-location connection check, so "no
        # WhatsApp connected" and "no template configured yet" would
        # otherwise be indistinguishable from the exception alone.
        lifecycle = None
        whatsapp_connected = False
        try:
<<<<<<< HEAD
            messaging = MetaMessagingService(session, graph=GraphApiClient(MetaAuthConfig.from_env()), token_store=MetaTokenStore())
=======
            messaging = MetaMessagingService(session, graph=GraphApiClient(WhatsAppMetaConfig.from_env()), token_store=MetaTokenStore())
>>>>>>> meta-app-seperation
            lifecycle = LifecycleCommunicationService(session, messaging)
            whatsapp_connected = session.scalar(
                select(MetaBusinessConnection).where(
                    MetaBusinessConnection.location_id == location_id,
                    MetaBusinessConnection.connection_status == "connected",
                )
            ) is not None
        except Exception:
            logger.exception(
                "meta_messaging_unavailable_for_public_booking location_id=%s booking_id=%s",
                location_id, booking.id,
            )

        if not whatsapp_connected:
            # No channel exists to ask the customer to confirm on, so the
            # web form submission itself has to stand as confirmation,
            # exactly as it did before this feature existed.
            booking.status = "confirmed"
            session.flush()
            try:
                if lifecycle is not None:
                    lifecycle.booking_confirmed(booking)
                    lifecycle.schedule_booking_reminder(booking)
            except Exception:
                logger.exception(
                    "booking_confirmed_message_failed location_id=%s booking_id=%s",
                    location_id, booking.id,
                )
        else:
            # WhatsApp is connected -- attempt the real double opt-in. If it
            # fails (most likely BOOKING_CONFIRMATION_REQUEST_TEMPLATE_NAME
            # is not yet an approved template on this workshop's WABA), the
            # booking stays pending rather than being silently confirmed
            # unverified; the confirmation page tells the customer plainly
            # that WhatsApp verification didn't go out.
            try:
                lifecycle.booking_awaiting_confirmation(booking)
            except MetaMessagingError as exc:
                logger.warning(
                    "booking_confirmation_request_failed location_id=%s booking_id=%s error=%s",
                    location_id, booking.id, exc,
                )
            except Exception:
                logger.exception(
                    "booking_confirmation_request_failed location_id=%s booking_id=%s",
                    location_id, booking.id,
                )

        booking_id = booking.id

    return redirect(url_for("public_booking.confirmed", slug=slug, booking_id=booking_id))


@public_booking_bp.get("/book/<slug>/confirmed/<int:booking_id>")
def confirmed(slug, booking_id):
    location = location_for_public_booking(slug)
    if not location:
        abort(404)
    from database import query_db
    # status is read fresh from the row rather than passed through the
    # redirect, so this page is honest even if reloaded or bookmarked --
    # e.g. after the customer has already replied YES on WhatsApp, or
    # after the 48-hour expiry job has cancelled an unanswered request.
    booking = query_db(
        "SELECT start_time, service_type, status FROM bookings WHERE id=%s AND location_id=%s",
        (booking_id, location["id"]), one=True,
    )
    if not booking:
        abort(404)
    return render_template("public_booking_confirmed.html", location=location, booking=booking)
