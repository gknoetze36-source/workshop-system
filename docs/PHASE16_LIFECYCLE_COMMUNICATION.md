# Phase 16 — Lifecycle Communication

PHANTA's lifecycle messaging is state-driven and operational. It does not diagnose, price, or authorize repairs.

## Customer messages

- `booking_confirmed`: sent when the customer's explicit YES creates/confirms the booking. Customer sees **date + morning** and is told to bring the vehicle when the workshop opens.
- `booking_reminder`: scheduled for **18:00 the calendar day before** the booking. It does not expose an appointment time.
- `ready_for_collection`: staff presses the dashboard action after the booking is in `ready_for_collection`; PHANTA sends the customer a WhatsApp message.
- `work_to_be_done`: reception/staff records whether outstanding work remains. If `completed=false`, PHANTA schedules a reminder for the following month.
- `yearly_message`: scheduled approximately one year after the latest recorded service for a vehicle and sent as a simple service reminder.

## Dashboard actions

`POST /dashboard/lifecycle/bookings/{booking_id}/ready-for-collection`

This is the backend action for the reception dashboard's **Vehicle ready for collection** button.

`POST /dashboard/lifecycle/bookings/{booking_id}/work-to-be-done`

Payload: `{ "completed": true|false }`.

The existing dashboard can render these as direct actions; the API never permits a different tenant's booking to be addressed.

## Scheduling

The existing Railway cron entry point now runs the lifecycle worker. It processes due booking reminders, work-to-be-done reminders and yearly messages.

WhatsApp delivery continues through the Phase 9 Meta messaging layer, including the 24-hour window and approved Utility-template policy.
