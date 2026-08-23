"""Calendar boundary for Phase 11.

Phase 11 deliberately does not authenticate to Google/Outlook. The booking
DB remains the source of truth. Phase 18 will implement the one-way export.
"""
from __future__ import annotations


class BookingCalendarSync:
    """Small extension point so booking code never depends on a calendar SDK."""

    def __init__(self, exporter=None):
        self.exporter = exporter

    def sync_confirmed_booking(self, booking):
        if self.exporter is None:
            return {"status": "deferred", "reason": "calendar integration belongs to Phase 18"}
        return self.exporter(booking)
