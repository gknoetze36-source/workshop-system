from helpers.dates import human_date
from services.location_service import public_booking_url


def build_booking_message(booking, reminder=None):
    service_label = (booking.get("service_level") or "General").lower()
    due_date = human_date(reminder.get("due_date") if reminder else booking.get("service_due_date"))
    location_name = booking.get("location_name") or booking.get("location") or "your workshop"
    vehicle = " ".join(part for part in [booking.get("make"), booking.get("model")] if part).strip() or "your vehicle"
    location_phone = booking.get("location_contact_phone") or "the location"
    booking_link = public_booking_url(
        {
            "location_slug": booking.get("location_slug"),
            "slug": booking.get("location_slug"),
        }
    )

    lines = [
        f"Hello {booking.get('first_name', '').strip() or 'Customer'},",
        f"This is a reminder from {location_name}.",
        f"Your annual {service_label} service for {vehicle} is due around {due_date}.",
    ]
    if booking.get("work_to_be_done"):
        lines.append(f"Workshop notes: {booking['work_to_be_done']}")
    lines.append(f"Book your next visit here: {booking_link}")
    lines.append(f"Need help? Contact {location_phone}.")
    body = "\n".join(lines)
    subject = f"{location_name}: {service_label.title()} service reminder"
    return subject, body


def build_booking_confirmation_message(booking):
    location_name = booking.get("location_name") or booking.get("location") or "your workshop"
    customer_name = booking.get("first_name") or "Customer"
    service = booking.get("service") or "your booking"
    scheduled = human_date(booking.get("scheduled_date") or booking.get("date"))
    reference = booking.get("booking_reference") or "pending"
    subject = f"{location_name}: booking confirmed"
    body = (
        f"Hi {customer_name}, your booking for {service} at {location_name} "
        f"is confirmed for {scheduled}. Your reference is {reference}. "
        "The workshop will confirm the final time if needed."
    )
    return subject, body


def build_appointment_reminder_message(booking, label):
    location_name = booking.get("location_name") or booking.get("location") or "your workshop"
    customer_name = booking.get("first_name") or "Customer"
    service = booking.get("service") or "your booking"
    scheduled = human_date(booking.get("scheduled_date") or booking.get("date"))
    reference = booking.get("booking_reference") or str(booking.get("id") or "")
    subject = f"{location_name}: {label} booking reminder"
    body = (
        f"Hi {customer_name}, reminder from {location_name}: your {service} booking is {label.lower()} "
        f"({scheduled}). Reference: {reference}. Reply here if you need to change it."
    )
    return subject, body


def build_vehicle_ready_message(booking):
    location_name = booking.get("location_name") or booking.get("location") or "your workshop"
    customer_name = booking.get("first_name") or "Customer"
    vehicle = " ".join(part for part in [booking.get("make"), booking.get("model")] if part).strip() or "your vehicle"
    reference = booking.get("booking_reference") or str(booking.get("id") or "")
    subject = f"{location_name}: vehicle ready for collection"
    body = (
        f"Hi {customer_name}, {vehicle} is ready for collection at {location_name}. "
        f"Reference: {reference}. Please contact the workshop if you need help with collection."
    )
    return subject, body


def build_declined_work_reminder_message(booking):
    location_name = booking.get("location_name") or booking.get("location") or "your workshop"
    customer_name = booking.get("first_name") or "Customer"
    vehicle = " ".join(part for part in [booking.get("make"), booking.get("model")] if part).strip() or "your vehicle"
    work = (booking.get("work_to_be_done") or "the work previously discussed").strip()
    booking_link = public_booking_url(
        {
            "location_slug": booking.get("location_slug"),
            "slug": booking.get("location_slug"),
        }
    )
    subject = f"{location_name}: pending work reminder"
    body = (
        f"Hi {customer_name}, reminder from {location_name}: the following work for {vehicle} is still pending: {work}. "
        f"You can book it here when ready: {booking_link}"
    )
    return subject, body


def _followup_message(inquiry, location, stage):
    service_type = (inquiry.get("service_type") or "your service").strip()
    location_name = location.get("name") or "the workshop"
    if stage == 1:
        return (
            f"{location_name}: just checking in about {service_type}. "
            f"Would you like me to book you in for a suitable date?"
        )
    if stage == 2:
        return (
            f"{location_name}: we still have a few open spots for {service_type}. "
            f"I can help secure a suitable date for you. Please tell me which date works."
        )
    if stage == 3:
        return (
            f"{location_name}: just following up on {service_type}. "
            f"Did you still want to come in? I can help book a suitable date."
        )
    return (
        f"{location_name}: one last check-in about {service_type}. "
        f"Let me know if you'd like me to book something for you."
    )


def _followup_subject(inquiry, location, stage):
    service_type = (inquiry.get("service_type") or "booking").strip()
    return f"{location.get('name')}: inquiry follow-up {stage} for {service_type}"


