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
        # See jobs/follow_up.py's identical filter for why access_locked is
        # checked separately from active -- the payment wall's lock does not
        # set active=False, only access_locked=True.
        location_ids = list(admin_session.scalars(
            select(Location.id).where(Location.active.is_(True), Location.access_locked.is_(False))
        ))
    finally:
        admin_session.close()

    attempted = 0
    expired: list[int] = []
    failed_locations: list[dict] = []
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
        except Exception as exc:
            # Previously `raise` here: one location's failure aborted every
            # remaining location for this entire 5-minute cycle, since
            # nothing caught it before the for-loop. jobs/meta_token_monitor.py
            # and jobs/follow_up.py already use the correct pattern -- record
            # the failure against this location only, keep going. Only that
            # location's transaction is rolled back; already-committed
            # locations from earlier in this same loop are unaffected.
            db.rollback()
            failed_locations.append({"location_id": location_id, "error": str(exc)})
        finally:
            db.close()

    result = {"locations_checked": attempted, "expired_booking_ids": expired}
    if failed_locations:
        result["failed_locations"] = failed_locations
    return result
