"""Release public-web booking slots nobody ever confirmed on WhatsApp.

See ai/booking/expiry_service.py for why 48 hours and why public_web only.
"""
from __future__ import annotations

from sqlalchemy import select

from ai.booking.availability import BookingAvailabilityService, WorkshopSchedule
from ai.booking.expiry_service import BookingExpiryService
from ai.booking.service import BookingService
from database import SessionLocal, set_location_id
from models.core import Location


def run_booking_expiry() -> dict:
    admin_session = SessionLocal()
    try:
        location_ids = list(admin_session.scalars(select(Location.id).where(Location.active.is_(True))))
    finally:
        admin_session.close()

    attempted = 0
    expired: list[int] = []
    for location_id in location_ids:
        db = SessionLocal()
        try:
            set_location_id(db, location_id)
            # change_status() -- the only method this job calls -- never
            # consults availability, so an empty schedule is fine here and
            # avoids reading this location's operating hours just to cancel
            # bookings.
            booking_service = BookingService(db, BookingAvailabilityService(db, WorkshopSchedule({})))
            service = BookingExpiryService(db, booking_service)
            ids = service.expire_stale_pending(location_id)
            attempted += 1
            expired.extend(ids)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            db.close()

    return {"locations_checked": attempted, "expired_booking_ids": expired}
