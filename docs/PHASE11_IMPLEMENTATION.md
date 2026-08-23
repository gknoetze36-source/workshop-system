# PHANTA Phase 11 — Booking Engine

Status: **COMPLETE**

Phase 11 implements the Build Order booking milestone:

- availability queries
- conflict prevention
- booking creation
- controlled booking status changes
- confirmation integration point
- 24h and 2h reminder scheduling
- calendar boundary remains a sync target, not the source of truth

## Architecture

```text
Service Advisor / internal route
        |
        v
BookingService
        |
        +--> BookingAvailabilityService
        |       +--> WorkshopSchedule
        |       +--> BookingRepository
        |
        +--> FollowUp reminder records
        +--> AuditLog
        +--> optional confirmation sender
        |
        v
bookings table  <-- source of truth
```

## Conflict prevention

The application performs an availability check before insert. PostgreSQL also has the Phase 2 EXCLUDE constraints for bay and technician overlap. The repository now checks both resources when a booking has both a bay and technician, rather than checking only the bay.

## Availability

Operating hours are supplied explicitly by the workshop configuration. The engine supports multiple windows per weekday, configurable candidate intervals, optional bay IDs and optional technician IDs.

## Reminders

Reminder schedules are stored in the existing `follow_ups` table for 24 hours and 2 hours before the booking. The booking engine does not create a second scheduler or a calendar dependency.

## Confirmation

`BookingService` accepts an injected confirmation sender. This keeps booking logic independent from the Meta SDK while allowing the existing Meta messaging service to send the actual WhatsApp confirmation in the application layer.

## Calendar

Phase 11 does not implement Google/Outlook OAuth or event export. `BookingCalendarSync` is only the extension boundary. The booking database remains authoritative; calendar integration is Phase 18.

## Tests

Phase 11 covers:
- operating-hour validation
- free/occupied windows
- bay conflicts
- technician conflicts
- both-resource conflicts
- cross-tenant booking protection
- booking creation
- reminder scheduling
- status transition validation
- audit records
