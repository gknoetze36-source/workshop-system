"""Salon-specific defaults and workflow capabilities."""

PROFILE = {
    "key": "salon",
    "label": "Salon",
    "subject": None,
    "default_services": [
        ("Consultation", "Initial consultation", 30),
        ("Hair Appointment", "Standard hair appointment", 60),
        ("Treatment", "Salon treatment", 60),
    ],
    "workflows": {"booking_confirmation", "booking_reminders", "returning_customer_rebooking", "missed_booking_recovery"},
}
