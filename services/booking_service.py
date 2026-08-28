"""
Booking Service

Business logic for the Booking entity.
Depends only on the Booking Repository.
"""

# ============================================================================
# Database
# ============================================================================

from database import (
    execute_db,
    query_db,
    fetch_one,
    transaction,
    utc_now,
    iso_date,
    classify_service_level,
)

# ============================================================================
# Helpers
# ============================================================================

from helpers.common import (
    boolish,
    db_bool,
)

from helpers.dates import (
    utc_today,
    compute_service_due_date,
)

from constants.booking_constants import DONE_STATUSES

from validators.phone_validator import normalize_phone

# ============================================================================
# Repository
# ============================================================================

from repositories.booking_repository import (
    get_visible_bookings as _get_visible_bookings,
    get_booking_by_reference as _get_booking_by_reference,
    get_booking_by_reference_raw as _get_booking_by_reference_raw,
    get_booking_by_id as _get_booking_by_id,
    get_booking_by_id_for_user as _get_booking_by_id_for_user,
    get_booking_count_per_location as _get_booking_count_per_location,
    get_bookings_for_customers as _get_bookings_for_customers,
    get_bookings_for_customer_history as _get_bookings_for_customer_history,
    get_booking_service_history_by_vin_and_location as _get_booking_service_history_by_vin_and_location,
    get_booking_count_by_location_and_date as _get_booking_count_by_location_and_date,
    find_duplicate_booking as _find_duplicate_booking,
    create_booking as _create_booking,
    attach_inquiry_to_booking as _attach_inquiry_to_booking,
    generate_booking_reference as _generate_booking_reference,
)

# ============================================================================
# Service Dependencies
# ============================================================================

from services.customer_service import upsert_customer
from services.financial_service import can_create_booking
from services.inquiry_service import find_active_inquiry
from services.catalog_service import ensure_service
from services.vehicle_service import upsert_vehicle



# ============================================================================
# Repository Delegates
# ============================================================================


def get_visible_bookings(user, filters=None):
    """Return visible bookings with optional filters."""
    return _get_visible_bookings(user, filters)


def get_booking_by_reference(reference, user):
    """Return a booking by reference with user scope enforced."""
    return _get_booking_by_reference(reference, user)


def get_booking_by_reference_raw(reference):
    """Return a booking by reference without user scope."""
    return _get_booking_by_reference_raw(reference)


def get_booking_by_id(booking_id, location_id):
    """Return a booking by ID within the authenticated location."""
    return _get_booking_by_id(booking_id, location_id)


def get_booking_by_id_for_user(booking_id, user):
    """Return a booking by ID with location/location scope enforced."""
    return _get_booking_by_id_for_user(booking_id, user)


def get_booking_count_per_location():
    """Return the number of bookings per location."""
    return _get_booking_count_per_location()


def get_booking_for_customers(user):
    """Return bookings for the customer list based on user permissions."""

    if user["role"] == "location_admin":
        clause = "b.location_id=%s"
        args = [user["location_id"]]

    elif user["role"] == "reception":
        clause = "b.location_id=%s"
        args = [user["location_id"]]

    else:  # super_admin
        clause = "1=1"
        args = []

    return _get_bookings_for_customers(clause, args)


def get_bookings_for_customer_history(user, phone):
    """Return customer booking history based on user permissions."""

    if user["role"] == "location_admin":
        clause = "b.location_id=%s"
        args = [user["location_id"]]

    elif user["role"] == "reception":
        clause = "b.location_id=%s"
        args = [user["location_id"]]

    else:  # super_admin
        clause = "1=1"
        args = []

    return _get_bookings_for_customer_history(
        clause,
        args,
        phone,
    )


def get_booking_service_history_by_vin_and_location(vin, location_id):
    """Return service history for a VIN within a location."""
    return _get_booking_service_history_by_vin_and_location(
        vin,
        location_id,
    )


def get_booking_count_by_location_and_date(location_id, date):
    """Return the booking count for a location on a specific date."""
    return _get_booking_count_by_location_and_date(
        location_id,
        date,
    )
# ============================================================================
# Booking Creation
# ============================================================================


def insert_booking(location, form_data, source, status):
    location_record = fetch_one(
        "SELECT * FROM locations WHERE id=%s",
        (location["location_id"],),
    )

    if not can_create_booking(location_record):
        raise PermissionError(
            "This client account is unpaid or inactive, so new bookings are disabled."
        )

    scheduled_date = (
        iso_date(form_data.get("scheduled_date") or form_data.get("date"))
        or utc_today()
    )

    phone = normalize_phone(form_data.get("phone"))
    service = (form_data.get("service") or "").strip()
    service_level = classify_service_level(service)

    duplicate = _find_duplicate_booking(
        location["location_id"],
        phone,
        (form_data.get("vehicle_vin") or "").strip(),
        (form_data.get("registration_number") or "").strip(),
        scheduled_date,
    )

    if duplicate:
        raise ValueError(
            f"Booking already exists ({duplicate['booking_reference']})"
        )

    completed_at = (
        scheduled_date
        if status in DONE_STATUSES
        else None
    )

    service_due_date = compute_service_due_date(
        service_level,
        completed_at,
    )

    booking_reference = _generate_booking_reference(scheduled_date, location["location_id"])
    now = utc_now()

    reminder_opt_in = db_bool(
        form_data.get("reminder_opt_in", "true")
    )

    whatsapp_opt_in = db_bool(
        form_data.get("whatsapp_opt_in", "false")
    )

    privacy_consent_at = (
        now
        if boolish(form_data.get("privacy_consent", "false"))
        else None
    )

    with transaction():
        customer_id = upsert_customer(
            location["location_id"],
            form_data,
        )
        # Upsert vehicle (reuse existing vehicle by registration or VIN)
        make = (form_data.get("make") or "").strip()
        model = (form_data.get("model") or "").strip()
        year_str = (form_data.get("vehicle_year") or "").strip()
        registration = (form_data.get("registration_number") or "").strip()
        colour = (form_data.get("colour") or "").strip()
        vin = (form_data.get("vehicle_vin") or "").strip()
        mileage_str = (form_data.get("current_mileage") or "").strip()
        try:
            year = int(year_str) if year_str.isdigit() else None
        except ValueError:
            year = None
        try:
            mileage = int(mileage_str) if mileage_str.isdigit() else None
        except ValueError:
            mileage = None
        vehicle_id = upsert_vehicle(
            location["id"], customer_id, make, model, year, registration, colour, vin, mileage,
        )

        # ensure_service takes (location_id, service_name). It was being called
        # with three arguments -- the scope id twice -- another franchise-era
        # leftover that would raise TypeError if this path were reached.
        service_id = ensure_service(
            location["location_id"],
            service,
        )

        booking_data = {
            "booking_reference": booking_reference,
            # "location_id" was specified twice here. Python keeps only the
            # last occurrence, so the first value was silently discarded --
            # no error, just the wrong value written. Franchise-era leftover.
            "location_id": location["location_id"],
            "company": location.get("name") or "",
            "location": location.get("name") or "",
            "customer_id": customer_id,
            "vehicle_id": vehicle_id,
            "service_id": service_id,
            "first_name": (form_data.get("first_name") or "").strip(),
            "surname": (form_data.get("surname") or form_data.get("last_name") or "").strip(),
            "customer_email": (form_data.get("customer_email") or form_data.get("email") or "").strip(),
            "phone": phone,
            "preferred_contact_method": form_data.get("preferred_contact_method") or "WhatsApp",
            "make": make,
            "model": model,
            "vehicle_year": year,
            "fuel_type": form_data.get("fuel_type"),
            "vehicle_vin": vin,
            "service": service,
            "service_level": service_level,
            "current_mileage": mileage,
            "scheduled_date": scheduled_date,
            "date": scheduled_date,
            "status": status,
            "service_due_date": service_due_date,
            "work_to_be_done": (form_data.get("work_to_be_done") or "").strip(),
            "public_notes": (form_data.get("public_notes") or "").strip(),
            "internal_notes": (form_data.get("internal_notes") or "").strip(),
            "source": source,
            "quote_declined": (form_data.get("quote_declined") or "No").strip(),
            "contacted": db_bool(form_data.get("contacted", False)),
            "whatsapp_opt_in": whatsapp_opt_in,
            "privacy_consent_at": privacy_consent_at,
            "reminder_opt_in": reminder_opt_in,
            "completed_at": completed_at,
            "created_at": now,
            "updated_at": now,
        }

        # Delegate persistence to repository
        booking_reference = _create_booking(booking_data)

        # Attach inquiry if applicable
        inquiry = find_active_inquiry(
            location["location_id"],
            location["id"],
            phone=phone,
            email=(form_data.get("customer_email") or form_data.get("email") or "").strip(),
        )

        if inquiry:
            followup_bookings = (
                1
                if int(inquiry.get("followups_sent_count") or 0) > 0
                else 0
            )
            _attach_inquiry_to_booking(
                booking_reference,
                inquiry["id"],
                followup_bookings,
                now,
            )

    return booking_reference
