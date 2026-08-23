"""
Lead Service for Workshop System Version 2.

This service contains business logic for lead management.
"""

from validators.phone_validator import normalize_phone

from database import query_db, transaction, utc_now
from helpers.audit import record_audit
from services.customer_service import upsert_customer
from repositories import lead_repository


def create_lead(data):
    """
    Create a new lead.
    """

    required = [
        "location_id",
        "location_id",
        "contact_name",
        "source",
    ]

    for field in required:
        if field not in data:
            raise ValueError(f"Missing required field: {field}")

    data.setdefault("status", "New")

    allowed_sources = {
        "Website",
        "Facebook",
        "Manual",
        "Phone",
        "Walk-in",
    }

    if data["source"] not in allowed_sources:
        raise ValueError(
            f"Invalid source: {data['source']}"
        )

    if data.get("contact_phone"):
        data["contact_phone"] = normalize_phone(
            data["contact_phone"]
        )

    if data.get("contact_email"):
        data["contact_email"] = (
            data["contact_email"]
            .strip()
            .lower()
        )

    lead_id = lead_repository.create_lead(data)

    record_audit(
        "lead_created",
        "lead",
        lead_id,
        None,
        location_id=data["location_id"],
        details={
            "source": data["source"],
            "contact_name": data["contact_name"],
        },
    )

    return lead_id


def get_lead_by_id(lead_id):
    """Retrieve a lead."""
    return lead_repository.get_lead_by_id(lead_id)


def get_leads(location_id, filters=None):
    """Retrieve leads."""
    return lead_repository.get_leads(
        location_id,
        filters,
    )


def update_lead(lead_id, data):
    """Update an existing lead."""

    if data.get("contact_phone"):
        data["contact_phone"] = normalize_phone(
            data["contact_phone"]
        )

    if data.get("contact_email"):
        data["contact_email"] = (
            data["contact_email"]
            .strip()
            .lower()
        )

    lead = lead_repository.get_lead_by_id(lead_id)

    lead_repository.update_lead(
        lead_id,
        data,
    )

    record_audit(
        "lead_updated",
        "lead",
        lead_id,
        None,
        location_id=lead.get("location_id") if lead else None,
        details=data,
    )


def convert_lead_to_customer(lead_id):
    """
    Convert a lead into a customer and link both records.
    """

    lead = lead_repository.get_lead_by_id(lead_id)

    if not lead:
        raise ValueError("Lead not found")

    if lead.get("customer_id"):
        raise ValueError("Lead has already been converted")

    if lead.get("status") not in {"New", "Contacted"}:
        raise ValueError(
            "Lead cannot be converted from its current status"
        )

    contact_name = (lead.get("contact_name") or "").strip()
    parts = contact_name.split(None, 1)

    first_name = parts[0] if parts else ""
    surname = parts[1] if len(parts) > 1 else ""

    customer_data = {
        "first_name": first_name,
        "surname": surname,
        "customer_name": contact_name,
        "phone": lead.get("contact_phone"),
        "email": lead.get("contact_email"),
        "customer_email": lead.get("contact_email"),
        "whatsapp_opt_in": "true",
    }

    with transaction():

        customer_id = upsert_customer(
            lead["location_id"],
            customer_data,
        )

        if not customer_id:
            raise ValueError(
                "Unable to create customer from lead"
            )

        lead_repository.attach_lead_to_customer(
            lead_id,
            customer_id,
            lead["location_id"],
        )

        lead_repository.mark_lead_converted(
            lead_id,
            customer_id,
            lead["location_id"],
        )

    return customer_id


def get_lead_stats(location_id):
    """
    Return lead statistics for the dashboard.

    NOTE:
    This implementation intentionally remains simple for the M1 migration.
    Future versions should replace the repeated repository calls with
    aggregate COUNT/GROUP BY repository methods.
    """

    leads = lead_repository.get_leads(location_id)
    total_leads = len(leads) if leads else 0

    converted = lead_repository.get_leads(
        location_id,
        {"status": "Converted"},
    )
    converted_leads = len(converted) if converted else 0

    conversion_percentage = (
        round((converted_leads / total_leads) * 100, 2)
        if total_leads
        else 0
    )

    source_breakdown = {}

    for source in (
        "Website",
        "Facebook",
        "Manual",
        "Phone",
        "Walk-in",
    ):
        rows = lead_repository.get_leads(
            location_id,
            {"source": source},
        )

        source_breakdown[source] = len(rows) if rows else 0

    return {
        "total_leads": total_leads,
        "converted_leads": converted_leads,
        "conversion_percentage": conversion_percentage,
        "source_breakdown": source_breakdown,
    }