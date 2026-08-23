# ============================================================================
# Configuration
# ============================================================================


PLAN_DEFINITIONS = {
    "core": {
        "label": "Core",
        "location_limit": 999999,
        "user_limit": 999999,
        "automation_enabled": True,
        "chatbot_enabled": True,
        "reporting_enabled": True,
        "custom_integrations_enabled": True,
        "priority_support_enabled": True,
    }
}

DEFAULT_SERVICES_BY_INDUSTRY = {
    "workshop": ["Service", "Repairs", "Inspection"],
    "salon": ["Consultation", "Hair Appointment", "Treatment"],
    "dentist": ["Consultation", "Checkup", "Follow-up"],
    "clinic": ["Consultation", "Follow-up", "Procedure"],
    "hotel": ["Room Booking", "Check-in", "Guest Request"],
    "consultant": ["Consultation", "Strategy Session", "Follow-up"],
    "gym": ["Class Booking", "Personal Training", "Assessment"],
    "cleaning": ["Once-off Cleaning", "Recurring Cleaning", "Deep Clean"],
    "repair": ["Repair Booking", "Collection"],
}
from constants.booking_constants import (
    STATUS_OPTIONS,
    QUICK_UPDATE_STATUS_OPTIONS,
    DONE_STATUSES,
)
INQUIRY_STATES = ["NEW_INQUIRY", "ENGAGED", "BOOKING_PENDING", "BOOKED", "LOST"]


