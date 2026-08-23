"""Booking availability engine for PHANTA Phase 11.

The booking table is the source of truth.  This module only calculates
availability and validates proposed booking windows; it does not talk to
Google/Outlook or an external scheduling SaaS.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from itertools import product
from typing import Iterable, Mapping, Sequence

from repositories.booking_repo import BookingRepository


ACTIVE_BOOKING_STATUSES = {"pending", "confirmed", "vehicle_received", "diagnosis_started", "repair_started"}


@dataclass(frozen=True)
class OperatingWindow:
    start: time
    end: time

    def __post_init__(self):
        if self.end <= self.start:
            raise ValueError("operating window end must be after start")


@dataclass(frozen=True)
class AvailableSlot:
    start_time: datetime
    end_time: datetime
    bay_id: int | None = None
    technician_id: int | None = None


class WorkshopSchedule:
    """Weekly operating hours.

    ``hours`` maps Python weekday numbers (Monday=0) to one or more windows.
    The engine deliberately requires the caller to provide workshop hours
    instead of hiding a business-hours assumption in the booking logic.
    """

    def __init__(self, hours: Mapping[int, Sequence[OperatingWindow]]):
        self.hours = {int(day): tuple(windows) for day, windows in hours.items()}
        for day in self.hours:
            if day < 0 or day > 6:
                raise ValueError("weekday must be between 0 and 6")

    def windows_for(self, day: date) -> tuple[OperatingWindow, ...]:
        return self.hours.get(day.weekday(), ())

    def contains(self, start: datetime, end: datetime) -> bool:
        if start.date() != end.date() or end <= start:
            return False
        local_start = start.timetz().replace(tzinfo=None)
        local_end = end.timetz().replace(tzinfo=None)
        return any(w.start <= local_start and local_end <= w.end for w in self.windows_for(start.date()))


class BookingAvailabilityError(ValueError):
    pass


class BookingAvailabilityService:
    """Calculates free booking windows against the relational booking table."""

    def __init__(self, session, schedule: WorkshopSchedule):
        self.session = session
        self.schedule = schedule
        self.bookings = BookingRepository(session)

    def assert_available(
        self,
        location_id: int,
        start_time: datetime,
        end_time: datetime,
        *,
        bay_id: int | None = None,
        technician_id: int | None = None,
        exclude_booking_id: int | None = None,
    ) -> None:
        self._validate_range(start_time, end_time)
        if not self.schedule.contains(start_time, end_time):
            raise BookingAvailabilityError("booking must fall within workshop operating hours")

        overlaps = self.bookings.overlaps(
            location_id, start_time, end_time, bay_id=bay_id, technician_id=technician_id
        )
        if exclude_booking_id is not None:
            overlaps = [b for b in overlaps if b.id != exclude_booking_id]
        if overlaps:
            raise BookingAvailabilityError("booking conflicts with an existing active booking")

    def available_slots(
        self,
        location_id: int,
        day: date,
        duration: timedelta,
        *,
        interval: timedelta = timedelta(minutes=30),
        bay_ids: Iterable[int] | None = None,
        technician_ids: Iterable[int] | None = None,
        now: datetime | None = None,
    ) -> list[AvailableSlot]:
        """Return discrete candidate starts for a day.

        Resource lists are optional. If both are supplied, a free bay/technician
        pair is returned for each candidate. If neither is supplied, the booking
        is treated as capacity-free and the caller can enforce capacity through
        its own resource assignment policy.
        """
        if duration <= timedelta(0):
            raise ValueError("duration must be positive")
        if interval <= timedelta(0):
            raise ValueError("interval must be positive")

        bays = list(dict.fromkeys(bay_ids or []))
        technicians = list(dict.fromkeys(technician_ids or []))
        resources = list(product(bays or [None], technicians or [None]))
        result: list[AvailableSlot] = []

        for window in self.schedule.windows_for(day):
            cursor = datetime.combine(day, window.start)
            window_end = datetime.combine(day, window.end)
            while cursor + duration <= window_end:
                end = cursor + duration
                if now is not None and cursor <= now:
                    cursor += interval
                    continue
                for bay_id, technician_id in resources:
                    if self._resource_pair_available(location_id, cursor, end, bay_id, technician_id):
                        result.append(AvailableSlot(cursor, end, bay_id, technician_id))
                cursor += interval
        return result

    def _resource_pair_available(self, location_id, start, end, bay_id, technician_id) -> bool:
        if bay_id is not None and self.bookings.overlaps(location_id, start, end, bay_id=bay_id):
            return False
        if technician_id is not None and self.bookings.overlaps(location_id, start, end, technician_id=technician_id):
            return False
        return True

    @staticmethod
    def _validate_range(start_time: datetime, end_time: datetime) -> None:
        if not isinstance(start_time, datetime) or not isinstance(end_time, datetime):
            raise TypeError("start_time and end_time must be datetime values")
        if end_time <= start_time:
            raise BookingAvailabilityError("end_time must be after start_time")
