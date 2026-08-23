"""Automotive workshop-specific defaults and workflow capabilities."""

PROFILE = {
    "key": "workshop",
    "label": "Automotive Workshop",
    "subject": "vehicle",
    "default_services": [
        ("Oil Change", "Standard oil change service", 30),
        ("Brake Service", "Brake inspection and pad replacement", 60),
        ("Tire Rotation", "Tire rotation and balancing", 20),
        ("Diagnostic Check", "Engine diagnostic and troubleshooting", 45),
        ("Full Service", "Comprehensive vehicle service", 120),
    ],
    "workflows": {"service_advisor", "vehicle_history", "service_reminders", "missed_booking_recovery"},
}
