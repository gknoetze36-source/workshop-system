"""
Customer Service

Business logic for the Customer entity.
Depends only on the Customer Repository.
"""

# ============================================================================
# Helpers
# ============================================================================

from helpers.common import db_bool
from validators.phone_validator import normalize_phone

# ============================================================================
# Repository
# ============================================================================

from repositories.customer_repository import (
    get_customer_by_id as repository_get_customer_by_id,
    get_customer_by_id_and_location as repository_get_customer_by_id_and_location,
    get_customer_by_phone_and_location as repository_get_customer_by_phone_and_location,
    get_customer_by_email_and_location as repository_get_customer_by_email_and_location,
    get_customer_count_by_location as repository_get_customer_count_by_location,
    create_customer as repository_create_customer,
    update_customer as repository_update_customer,
)

# ============================================================================
# Repository Delegates
# ============================================================================


def get_customer_by_id(customer_id, location_id):
    """Retrieve a customer by ID within the authenticated location."""
    return repository_get_customer_by_id(customer_id, location_id)


def get_customer_by_id_and_location(customer_id, location_id):
    """Retrieve a customer by ID, ensuring it belongs to the given location."""
    return repository_get_customer_by_id_and_location(customer_id, location_id)


def get_customer_by_phone_and_location(phone, location_id):
    """Retrieve the most recent customer for a given phone and location."""
    return repository_get_customer_by_phone_and_location(phone, location_id)


def get_customer_by_email_and_location(email, location_id):
    """Retrieve the most recent customer for a given email and location."""
    return repository_get_customer_by_email_and_location(email, location_id)


def get_customer_count_by_location(location_id):
    """Return the number of customers in a location."""
    return repository_get_customer_count_by_location(location_id)


# ============================================================================
# Customer Business Logic
# ============================================================================

def upsert_customer(location_id, form_data):
    """Insert or update a customer and return the customer ID."""
    

    # Normalize input
    phone = normalize_phone(form_data.get("phone"))
    email = (form_data.get("customer_email") or form_data.get("email") or "").strip().lower()
    first_name = (form_data.get("first_name") or "").strip()
    surname = (form_data.get("surname") or "").strip()
    full_name = " ".join(part for part in [first_name, surname] if part).strip() or (form_data.get("customer_name") or "").strip()



    # Check if customer exists by phone or email
    existing = None
    if phone:
        existing = repository_get_customer_by_phone_and_location(phone, location_id)
    if existing:

        repository_update_customer(
            customer_id=existing["id"],
            location_id=location_id,
            first_name=first_name,
            surname=surname,
            full_name=full_name,
            phone=phone,
            email=email,
            accepts_whatsapp=db_bool(
                form_data.get("whatsapp_opt_in", "true")
            ),
        )

        return existing["id"]

    return repository_create_customer(
        location_id=location_id,
        first_name=first_name,
        surname=surname,
        full_name=full_name,
        phone=phone,
        email=email,
        accepts_whatsapp=db_bool(
            form_data.get("whatsapp_opt_in", "true")
        ),
    )

