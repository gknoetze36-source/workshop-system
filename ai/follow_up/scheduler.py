"""Follow-up scheduler boundary.

Phase 11 only schedules booking reminders. Delivery/dispatch remains a later
follow-up phase and must use the existing Meta messaging service.
"""
from __future__ import annotations

from repositories.followup_repo import FollowUpRepository


class FollowUpScheduler:
    def __init__(self, session):
        self.session = session
        self.repository = FollowUpRepository(session)

    def due_booking_reminders(self, location_id, now):
        return [
            item for item in self.repository.due(location_id, now)
            if item.type in {"booking_reminder_24h", "booking_reminder_2h"}
        ]
