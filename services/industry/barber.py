"""Barber-specific defaults and workflow capabilities."""

PROFILE = {
    "key": "barber",
    "label": "Barber",
    "subject": None,
    "default_services": [
        ("Haircut", "Standard haircut", 45),
        ("Beard Trim", "Beard trim and shaping", 30),
        ("Haircut & Beard", "Combined haircut and beard service", 60),
    ],
    "workflows": {"booking_confirmation", "booking_reminders", "returning_customer_rebooking", "missed_booking_recovery"},
}
