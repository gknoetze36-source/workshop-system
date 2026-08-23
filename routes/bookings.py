"""Phase 11 booking engine API."""
from __future__ import annotations

from helpers.location import current_location_id

from datetime import date, datetime, time, timedelta

from flask import Blueprint, jsonify, request, g

from database import get_session, fetch_one
from ai.booking.availability import BookingAvailabilityService, OperatingWindow, WorkshopSchedule
from ai.booking.service import BookingService, BookingStatus
from ai.booking.confirmation import BookingConfirmationService
from ai.communications.lifecycle import LifecycleCommunicationService
from ai.communications.review import PostServiceReviewService
from integrations.meta.auth.config import MetaAuthConfig
from integrations.meta.auth.token_store import MetaTokenStore
from integrations.meta.messaging.messaging_service import MetaMessagingService
from integrations.meta.services.graph_api_client import GraphApiClient

bookings_bp = Blueprint("bookings", __name__, url_prefix="/bookings")


def _morning_window(location_id: int, day: date) -> tuple[datetime, datetime]:
    """Return an internal booking window at the configured shop opening.

    Customers never choose or receive an exact time; the system uses the
    workshop's opening time internally so the booking remains date + morning.
    """
    import json
    from datetime import time as dt_time

    row = fetch_one("SELECT operating_hours_json FROM locations WHERE id=%s", (location_id,))
    opening = "08:00"
    closing = "17:00"
    try:
        settings = json.loads((row or {}).get("operating_hours_json") or "{}")
        key = day.strftime("%A").lower()
        if settings.get(f"{key}_enabled", True):
            opening = settings.get(f"{key}_open") or opening
            closing = settings.get(f"{key}_close") or closing
    except (TypeError, ValueError):
        pass
    start = datetime.combine(day, dt_time.fromisoformat(opening))
    close = datetime.combine(day, dt_time.fromisoformat(closing))
    end = min(start + timedelta(hours=1), close)
    if end <= start:
        end = start + timedelta(hours=1)
    return start, end


def _schedule_from_payload(payload):
    # Internal validation only. Customer-facing booking remains date + morning.
    return WorkshopSchedule({
        weekday: [OperatingWindow(time(8, 0), time(17, 0))]
        for weekday in range(7)
    })


def _dt(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=None)
    return parsed


@bookings_bp.post("/availability")
def availability():
    """Return morning availability only; exact time slots are not exposed."""
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    try:
        day = date.fromisoformat(payload["date"])
        start, end = _morning_window(location_id, day)
        return jsonify({"date": day.isoformat(), "arrival": "morning", "available": True})
    except (KeyError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

@bookings_bp.post("")
def create_booking():
    """Create a booking from date + morning; no customer-selected time slots."""
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    session = get_session()
    try:
        if str(payload.get("arrival", "morning")).lower() != "morning":
            raise ValueError("arrival must be morning")
        day = date.fromisoformat(str(payload["date"]))
        start_time, end_time = _morning_window(location_id, day)
        service = BookingService(session, BookingAvailabilityService(session, _schedule_from_payload(payload)))
        booking = service.create_booking(
            location_id=location_id,
            customer_id=int(payload["customer_id"]),
            vehicle_id=int(payload["vehicle_id"]),
            start_time=start_time,
            end_time=end_time,
            service_type=str(payload["service_type"]),
            source=str(payload.get("source") or "whatsapp"),
            notes=payload.get("notes"),
            conversation_id=payload.get("conversation_id"),
            confirmation_template=payload.get("confirmation_template"),
        )
        session.commit()
        return jsonify({
            "id": booking.id,
            "status": booking.status,
            "date": day.isoformat(),
            "arrival": "morning",
        }), 201
    except (KeyError, ValueError) as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 409
    except Exception:
        session.rollback()
        return jsonify({"error": "booking creation failed"}), 500
    finally:
        session.close()

@bookings_bp.post("/<int:booking_id>/status")
def change_booking_status(booking_id: int):
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    session = get_session()
    try:
        booking_service = BookingService(
            session,
            BookingAvailabilityService(session, _schedule_from_payload(payload)),
        )
        previous_status = booking_service.bookings.get_by_id(location_id, booking_id)
        previous_status_value = previous_status.status if previous_status else None
        booking = booking_service.change_status(
            location_id, booking_id, payload["status"], actor=str(payload.get("actor") or "staff")
        )
        review_message_id = None
        if previous_status_value != BookingStatus.COMPLETED and booking.status == BookingStatus.COMPLETED:
            review_message = PostServiceReviewService(
                session,
                MetaMessagingService(
                    session,
                    graph=GraphApiClient(MetaAuthConfig.from_env()),
                    token_store=MetaTokenStore(),
                ),
            ).send_for_booking(location_id, booking.id)
            review_message_id = review_message.id if review_message else None
        session.commit()
        return jsonify({"id": booking.id, "status": booking.status, "review_message_id": review_message_id})
    except (KeyError, ValueError) as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 409
    finally:
        session.close()


@bookings_bp.post("/<int:booking_id>/confirm")
def confirm_booking(booking_id: int):
    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401
    payload = request.get_json(silent=True) or {}
    customer_id = payload.get("customer_id")
    raw_message = payload.get("raw_message")
    session = get_session()
    try:
        if not isinstance(customer_id, int) or customer_id <= 0:
            raise ValueError("customer_id is required")
        record = BookingConfirmationService(session).confirm(
            location_id=location_id,
            customer_id=customer_id,
            booking_id=booking_id,
            raw_message=str(raw_message or ""),
            channel="whatsapp",
        )
        if record.decision == "confirmed":
            lifecycle = LifecycleCommunicationService(
                session,
                MetaMessagingService(
                    session,
                    graph=GraphApiClient(MetaAuthConfig.from_env()),
                    token_store=MetaTokenStore(),
                ),
            )
            lifecycle.schedule_booking_reminder(record.booking)
            lifecycle.booking_confirmed(record.booking)
        session.commit()
        return jsonify({
            "booking_confirmation_id": record.id,
            "booking_id": booking_id,
            "decision": record.decision,
            "booking_status": "confirmed" if record.decision == "confirmed" else "cancelled",
            "date": record.booking.start_time.date().isoformat(),
            "arrival": "morning",
        })
    except ValueError as exc:
        session.rollback()
        return jsonify({"error": str(exc)}), 409
    finally:
        session.close()
