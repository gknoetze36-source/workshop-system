"""Owner/location provisioning helpers.

The canonical hierarchy is one owner -> one location. This module keeps
provisioning logic separate from the operational location repository.
"""
from database import execute_db, query_db, utc_now
from repositories.location_repository import get_location_by_id, get_visible_locations
from services.catalog_service import ensure_service
from services.industry import get_industry_profile


def get_location_by_slug(slug):
    return query_db("SELECT * FROM locations WHERE slug=%s LIMIT 1", (slug,), one=True)


def location_counts(location_id):
    row=query_db("SELECT COUNT(*) AS total FROM users WHERE location_id=%s AND active=TRUE",(location_id,),one=True) or {}
    return {"locations": 1 if get_location_by_id(location_id) else 0, "users": int(row.get("total") or 0)}


def can_add_location(location):
    # Target architecture permits exactly one location per owner.
    return False


def can_add_user(location):
    limit=int(location.get("user_limit") or 0)
    return limit<=0 or location_counts(location["id"])["users"] < limit


def provision_business(location_id, answers=None):
    location=get_location_by_id(location_id)
    if not location: return {"ok":False,"error":"location not found"}
    answers=answers or {}
    industry=(answers.get("industry") or location.get("industry") or "workshop").strip().lower()
    try:
        get_industry_profile(industry)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}
    execute_db("UPDATE locations SET industry=%s, updated_at=%s WHERE id=%s",(industry,utc_now(),location_id))
    return {"ok":True,"location_id":location_id,"industry":industry}


def provision_owner_location(owner_id, name, industry):
    """Create the single location belonging to an owner."""
    try:
        get_industry_profile(industry)
    except ValueError as exc:
        return {"ok": False, "error": str(exc)}

    owner = query_db(
        "SELECT id,user_id,name,email,active FROM owners WHERE id=%s LIMIT 1",
        (owner_id,), one=True,
    )
    if not owner or not owner.get("active", True):
        return {"ok": False, "error": "owner not found or inactive"}

    existing = query_db(
        "SELECT id FROM locations WHERE owner_id=%s LIMIT 1",
        (owner_id,), one=True,
    )
    if existing:
        return {"ok": False, "error": "owner already has a location"}

    slug_base = "".join(ch.lower() if ch.isalnum() else "-" for ch in name).strip("-")
    slug_base = "-".join(part for part in slug_base.split("-") if part)[:180] or "location"
    slug = slug_base
    suffix = 2
    while query_db("SELECT id FROM locations WHERE slug=%s LIMIT 1", (slug,), one=True):
        slug = f"{slug_base}-{suffix}"
        suffix += 1

    now = utc_now()
    execute_db(
        """INSERT INTO locations
           (owner_id,name,slug,contact_email,industry,active,
            automation_enabled,chatbot_enabled,reporting_enabled,
            custom_integrations_enabled,priority_support_enabled,
            timezone,currency,language,daily_capacity,public_booking_enabled,
            created_at,updated_at)
           VALUES (%s,%s,%s,%s,%s,TRUE,FALSE,FALSE,FALSE,FALSE,FALSE,
                   'Africa/Johannesburg','ZAR','en',12,TRUE,%s,%s)""",
        (owner_id, name, slug, owner.get("email"), industry, now, now),
    )
    location = query_db(
        "SELECT id FROM locations WHERE owner_id=%s ORDER BY id DESC LIMIT 1",
        (owner_id,), one=True,
    )
    if not location:
        return {"ok": False, "error": "location creation could not be verified"}

    execute_db(
        "UPDATE users SET owner_id=%s, location_id=%s, updated_at=%s WHERE id=%s AND owner_id=%s",
        (owner_id, location["id"], now, owner.get("user_id"), owner_id),
    )
    return {"ok": True, "location_id": location["id"], "owner_id": owner_id, "industry": industry}
