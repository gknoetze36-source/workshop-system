"""Customer list, profile, and edit routes.

Rewritten 2026-08-25 after finding this file was reading/writing an
abandoned legacy column set (customers.surname/phone,
vehicles.license_plate/vehicle_vin/colour, bookings.scheduled_date/
booking_reference/work_to_be_done/internal_notes) that no live code path
in this app has written to in a long time -- create_customer() (the one
function that *did* dual-write both the legacy and current columns) is
itself dead code, never called from anywhere. Every customer created
through the actual live app (the Meta webhook handler creating a
first-time WhatsApp sender is the primary path) only ever populates
first_name/last_name/whatsapp_number, so this route was displaying blank
WhatsApp numbers, incomplete names, and blank vehicle registrations for
essentially every real customer.

A second, compounding bug: customers()'s output dict shape didn't match
what templates/customers.html actually reads (a flat {"customer_name":
..., "vehicle_make": ...} dict vs. the template's {"name": ..., "vehicle":
{"make": ...}} nested shape) -- so even with the columns fixed, nothing
would have rendered differently, since Jinja's default Undefined behavior
silently renders a missing attribute access as empty/falsy rather than
raising. Fixed to produce the shape the template already expects, rather
than change the template.

customer_profile()'s existing output shape did already match its own
template (templates/customer_profile.html) -- only the source columns
needed fixing there. customer_edit()'s template-facing keys are
deliberately left as `surname`/`phone`/`meta_notes` (matching
templates/customer_edit.html's field names exactly) even though the real
columns behind them are last_name/whatsapp_number/notes -- this decouples
"what the form field is called" from "which database column is actually
correct", so no template needed to change.
"""
from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from services.auth_service import active_location_required, login_required, current_user
from database import get_session
from helpers.permission import require_role, ADMIN_ROLES
from helpers.security_events import record_security_event
from repositories.audit_repo import AuditLogRepository
from services.data_lifecycle import DataLifecycleService
import logging

logger = logging.getLogger(__name__)
from database import query_db
from helpers.dates import utc_today

customer_bp = Blueprint("customer", __name__)


@customer_bp.route("/customers/search")
@login_required
def customer_vehicle_search():
    """Location-scoped global search across customers and vehicles.

    This endpoint returns only records owned by the authenticated workshop.
    It deliberately omits fields that are not present in the current schema.
    """
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    from flask import jsonify
    from sqlalchemy import or_, func
    from database import get_session
    from models.core import Customer, Vehicle
    from helpers.location import current_location_id

    try:
        location_id = current_location_id()
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401

    query = (request.args.get("q") or "").strip()
    if len(query) < 2:
        return jsonify({"query": query, "results": []}), 200

    pattern = f"%{query.lower()}%"
    session = get_session()
    try:
        rows = session.query(Customer, Vehicle).join(
            Vehicle, Vehicle.customer_id == Customer.id, isouter=True
        ).filter(
            Customer.location_id == location_id,
            or_(
                func.lower(Customer.first_name).like(pattern),
                func.lower(Customer.last_name).like(pattern),
                func.lower(Customer.whatsapp_number).like(pattern),
                func.lower(Customer.email).like(pattern),
                func.lower(Vehicle.make).like(pattern),
                func.lower(Vehicle.model).like(pattern),
                func.lower(Vehicle.registration).like(pattern),
                func.lower(Vehicle.vin).like(pattern),
            ),
        ).order_by(Customer.last_name.asc(), Customer.first_name.asc()).limit(25).all()

        results = []
        seen = set()
        for customer, vehicle in rows:
            key = (customer.id, vehicle.id if vehicle else None)
            if key in seen:
                continue
            seen.add(key)
            item = {
                "customer_id": customer.id,
                "customer_name": f"{customer.first_name} {customer.last_name}".strip(),
            }
            if customer.whatsapp_number:
                item["whatsapp_number"] = customer.whatsapp_number
            if customer.email:
                item["email"] = customer.email
            if vehicle:
                item["vehicle_id"] = vehicle.id
                item["vehicle"] = " ".join(
                    part for part in [vehicle.make, vehicle.model, str(vehicle.year) if vehicle.year else None] if part
                )
                if vehicle.registration:
                    item["registration"] = vehicle.registration
            results.append(item)

        return jsonify({"query": query, "results": results}), 200
    finally:
        session.close()


@customer_bp.route("/customers")
@login_required
def customers():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]
    today = utc_today()

    def get_latest_booking(vehicle_id):
        return query_db(
            """
            SELECT id, start_time, status, service_type, notes
            FROM bookings
            WHERE location_id = %s AND vehicle_id = %s
            ORDER BY start_time DESC
            LIMIT 1
            """,
            (location_id, vehicle_id), one=True,
        )

    def get_next_booking(vehicle_id):
        return query_db(
            """
            SELECT id, start_time, status, service_type, notes
            FROM bookings
            WHERE location_id = %s AND vehicle_id = %s
              AND start_time >= %s
              AND status NOT IN ('completed', 'cancelled')
            ORDER BY start_time ASC
            LIMIT 1
            """,
            (location_id, vehicle_id, today), one=True,
        )

    customers_raw = query_db(
        """
        SELECT id, first_name, last_name, whatsapp_number, email, notes, created_at
        FROM customers
        WHERE location_id = %s
        ORDER BY last_name, first_name
        """,
        (location_id,)
    ) or []

    customers_data = []
    for c in customers_raw:
        customer_name = f"{c['first_name'] or ''} {c['last_name'] or ''}".strip() or "Unknown"
        customer_whatsapp = c["whatsapp_number"] or ""
        customer_email = c["email"] or ""
        customer_notes = c["notes"] or ""
        customer_created = c["created_at"]

        vehicles_raw = query_db(
            """
            SELECT id, make, model, year, registration, vin
            FROM vehicles
            WHERE customer_id = %s AND location_id = %s
            ORDER BY updated_at DESC
            """,
            (c["id"], location_id)
        ) or []

        def build_row(vehicle=None):
            latest_booking = next_booking = None
            status = "-"
            last_visit = "-"
            next_booking_display = "-"
            next_booking_obj = None
            if vehicle:
                latest_booking = get_latest_booking(vehicle["id"])
                next_booking = get_next_booking(vehicle["id"])
                if latest_booking:
                    status = latest_booking["status"] or "-"
                    if latest_booking["start_time"]:
                        last_visit = latest_booking["start_time"]
                if next_booking and next_booking["start_time"]:
                    next_booking_display = next_booking["start_time"]
                    next_booking_obj = {
                        "date": next_booking["start_time"],
                        "service": next_booking["service_type"],
                    }
            return {
                # Matches templates/customers.html's actual accessors
                # (customer.name/.whatsapp/.vehicle.make/.registration/
                # .last_visit/.next_booking.date/.status) -- not a
                # renamed-for-clarity convenience, the template silently
                # rendered nothing for any of this before these keys
                # matched what it was actually reading.
                "id": c["id"],
                "name": customer_name,
                "whatsapp": customer_whatsapp,
                "email": customer_email,
                "notes": customer_notes,
                "created_at": customer_created,
                "vehicle": {
                    "id": vehicle["id"], "make": vehicle["make"] or "", "model": vehicle["model"] or "",
                    "year": vehicle["year"],
                } if vehicle else None,
                "registration": (vehicle["registration"] or "") if vehicle else "",
                "vin": (vehicle["vin"] or "") if vehicle else "",
                "last_visit": last_visit,
                "next_booking": next_booking_obj,
                "next_booking_display": next_booking_display,
                "status": status,
            }

        if vehicles_raw:
            for v in vehicles_raw:
                customers_data.append(build_row(v))
        else:
            customers_data.append(build_row(None))

    customers_data.sort(key=lambda x: x["name"].lower())
    return render_template("customers.html", customers=customers_data)


@customer_bp.route("/customers/<int:customer_id>")
@login_required
def customer_profile(customer_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    customer = query_db(
        """
        SELECT id, first_name, last_name, whatsapp_number, email, notes, created_at
        FROM customers
        WHERE id = %s AND location_id = %s
        """,
        (customer_id, location_id), one=True,
    )
    if not customer:
        abort(404)

    customer_name = f"{customer['first_name'] or ''} {customer['last_name'] or ''}".strip() or "Unknown"
    customer_whatsapp = customer["whatsapp_number"] or ""
    customer_email = customer["email"] or ""
    customer_notes = customer["notes"] or ""
    customer_created = customer["created_at"]

    vehicles = query_db(
        """
        SELECT id, make, model, year, registration, vin, mileage
        FROM vehicles
        WHERE customer_id = %s AND location_id = %s
        ORDER BY updated_at DESC
        """,
        (customer_id, location_id)
    ) or []

    vehicle_list = [{
        "id": v["id"], "make": v["make"] or "", "model": v["model"] or "", "year": v["year"],
        "registration": v["registration"] or "", "colour": "", "vin": v["vin"] or "",
        "current_mileage": v["mileage"], "notes": "",
    } for v in vehicles]

    bookings = query_db(
        """
        SELECT b.id, b.start_time, b.status, b.service_type, b.notes
        FROM bookings b
        JOIN vehicles v ON b.vehicle_id = v.id
        WHERE v.customer_id = %s AND v.location_id = %s
        ORDER BY b.start_time DESC
        """,
        (customer_id, location_id)
    ) or []

    booking_list = [{
        "reference": b["id"], "date": b["start_time"], "status": b["status"],
        "service": b["service_type"] or "-", "work_to_be_done": b["service_type"] or "",
        "internal_notes": b["notes"] or "", "mileage": None,
    } for b in bookings]

    vehicle_count = len(vehicle_list)
    booking_count = len(booking_list)

    last_visit = None
    if bookings:
        sorted_bookings = sorted(bookings, key=lambda x: x["start_time"], reverse=True)
        last_visit = sorted_bookings[0]["start_time"]

    next_booking = None
    today = utc_today()
    upcoming = [b for b in bookings if b["start_time"] and str(b["start_time"]) >= today and b["status"] not in ("completed", "cancelled")]
    if upcoming:
        upcoming_sorted = sorted(upcoming, key=lambda x: x["start_time"])
        next_booking = {
            "reference": upcoming_sorted[0]["id"],
            "date": upcoming_sorted[0]["start_time"],
            "service": upcoming_sorted[0]["service_type"],
        }

    customer_obj = {
        "id": customer["id"], "name": customer_name, "whatsapp": customer_whatsapp,
        "email": customer_email, "notes": customer_notes, "created_at": customer_created,
        "vehicle_count": vehicle_count, "booking_count": booking_count,
        "last_visit": last_visit, "next_booking": next_booking,
        "vehicles": vehicle_list, "bookings": booking_list,
    }
    return render_template("customer_profile.html", customer=customer_obj)


@customer_bp.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
@login_required
def customer_edit(customer_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        customer = query_db(
            """
            SELECT id, first_name, last_name, whatsapp_number, email, notes, created_at
            FROM customers
            WHERE id = %s AND location_id = %s
            """,
            (customer_id, location_id), one=True,
        )
        if not customer:
            abort(404)

        # Template-facing keys deliberately match templates/customer_edit
        # .html's field names (surname/phone/meta_notes) even though the
        # real columns are last_name/whatsapp_number/notes -- see module
        # docstring.
        customer_data = {
            "id": customer["id"],
            "first_name": customer["first_name"] or "",
            "surname": customer["last_name"] or "",
            "phone": customer["whatsapp_number"] or "",
            "email": customer["email"] or "",
            "meta_notes": customer["notes"] or "",
            "created_at": customer["created_at"],
        }
        return render_template("customer_edit.html", customer=customer_data)

    elif request.method == "POST":
        form_data = request.form
        from database import execute_db, utc_now

        whatsapp_number = form_data.get("phone", "").strip()
        email = (form_data.get("email") or "").strip().lower()
        first_name = form_data.get("first_name", "").strip()
        last_name = form_data.get("surname", "").strip()
        notes = form_data.get("notes", "").strip()
        now = utc_now()

        execute_db(
            "UPDATE customers SET first_name=%s, last_name=%s, whatsapp_number=%s, email=%s, notes=%s, updated_at=%s "
            "WHERE id=%s AND location_id=%s",
            (first_name, last_name, whatsapp_number, email, notes, now, customer_id, location_id),
        )

        flash('Customer updated successfully', 'success')
        return redirect(url_for('customer.customer_profile', customer_id=customer_id))


@customer_bp.route("/customers/<int:customer_id>/delete", methods=["POST"])
@login_required
@require_role(*ADMIN_ROLES)
def customer_delete(customer_id):
    """Execute a POPIA erasure request for one customer.

    Wires up services/data_lifecycle.DataLifecycleService, which was written
    and unit-tested but had no route -- its only callers were in tests/, so
    PHANTA had no reachable deletion capability at all.

    WHAT THIS DOES: an anonymising soft delete. Directly identifying fields
    are cleared, deleted_at is stamped, and relational history (bookings,
    invoices, service records) is preserved so the workshop's operational and
    financial records stay intact and foreign keys do not break.

    WHAT IT DELIBERATELY DOES NOT DO: hard deletion, cascading removal of
    vehicles/bookings/messages, or anything about backups. Those belong with
    retention (item 16 of the security plan) once the legal retention periods
    are confirmed, and are a different decision from honouring an erasure
    request against the live record.

    Restricted to owner/admin: erasure is irreversible through the UI.
    """
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]
    actor = user.get("email") or user.get("username") or str(user.get("id"))

    session = get_session()
    try:
        service = DataLifecycleService(session, AuditLogRepository(session))
        service.soft_delete_customer(location_id, customer_id, actor)
        session.commit()
    except LookupError:
        session.rollback()
        abort(404)
    except Exception:
        session.rollback()
        logger.exception(
            "customer_delete_failed customer_id=%s location_id=%s", customer_id, location_id
        )
        flash("The customer could not be deleted. Please try again.", "error")
        return redirect(url_for("customer.customer_profile", customer_id=customer_id))
    finally:
        session.close()

    record_security_event(
        "privacy.customer_erased",
        user_id=user.get("id"),
        location_id=location_id,
        details={"customer_id": customer_id},
    )
    flash("The customer's personal details have been erased.", "success")
    return redirect(url_for("customer.customers"))
