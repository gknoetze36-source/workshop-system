"""Vehicle profile and edit routes.

Rewritten 2026-08-25 -- the same legacy-column bug found in
routes/customer.py, but worse here: this file crashed outright (500 on
every single view) rather than silently showing wrong data, because
`v.colour` was queried and no column of that name exists anywhere in the
schema, legacy or current (only customers.surname/phone and
vehicles.license_plate/vehicle_vin exist as unused legacy remnants;
`colour` was never a real column at all). bookings.scheduled_date/
booking_reference/work_to_be_done referenced here have the same problem
as they did in customer.py -- see that file's module docstring for the
full explanation of why (create_customer()'s dual-write sibling for
vehicles is equally dead code; the live creation path only populates
make/model/year/registration/vin/mileage via the ORM).

vehicle_edit()'s POST handler additionally referenced a `vehicle` variable
that was only ever assigned inside the sibling `if request.method ==
"GET":` branch -- guaranteed NameError on every real edit submission,
independent of the column-name bug.

Template-facing output keys are kept exactly as templates/vehicle_profile
.html and templates/vehicle_edit.html already expect (including
inconsistent naming between the two -- vehicle_profile.html wants
`vehicle_vin` and a nested `customer.surname`/`.phone`;
vehicle_edit.html wants `vin` and no customer object at all) so neither
template needed to change; only the source columns and the route's
output shape (which didn't match either template before this fix) did.

`colour` and vehicle-level `notes` are shown as empty strings and
`mileage_history` as an empty list throughout -- there is no real column
or tracking mechanism for any of the three in the current schema. This is
a disclosed gap, not a new one introduced by this fix.
"""
from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from database import query_db, execute_db, utc_now
from helpers.dates import utc_today
from services.auth_service import login_required, active_location_required, current_user

vehicles_bp = Blueprint("vehicles", __name__)


@vehicles_bp.route("/vehicles/<int:vehicle_id>")
@login_required
def vehicle_profile(vehicle_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    vehicle = query_db(
        """
        SELECT v.id, v.make, v.model, v.year, v.registration, v.vin, v.mileage, v.created_at,
               c.id as customer_id, c.first_name, c.last_name, c.whatsapp_number, c.email
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.id = %s AND v.location_id = %s
        """,
        (vehicle_id, location_id), one=True,
    )
    if not vehicle:
        abort(404)

    service_history = query_db(
        """
        SELECT id, start_time, service_type, status
        FROM bookings
        WHERE vehicle_id = %s AND location_id = %s AND status IN ('completed', 'done')
        ORDER BY start_time DESC
        """,
        (vehicle_id, location_id)
    ) or []
    service_list = [{
        "date": s["start_time"], "service": s["service_type"] or "-", "status": s["status"], "mileage": None,
    } for s in service_history]

    booking_history = query_db(
        """
        SELECT id, start_time, service_type, status
        FROM bookings
        WHERE vehicle_id = %s AND location_id = %s
        ORDER BY start_time DESC
        """,
        (vehicle_id, location_id)
    ) or []
    # Matches templates/vehicle_profile.html's actual accessors
    # (booking.booking_reference/.scheduled_date/.current_mileage) --
    # the previous version built {"reference": ..., "date": ...} instead,
    # a shape the template never read, on top of sourcing from columns
    # (booking_reference, scheduled_date) that don't exist in the schema.
    booking_list = [{
        "booking_reference": b["id"], "scheduled_date": b["start_time"],
        "service": b["service_type"] or "-", "status": b["status"], "current_mileage": None,
    } for b in booking_history]

    vehicle_obj = {
        "id": vehicle["id"], "make": vehicle["make"] or "", "model": vehicle["model"] or "",
        "year": vehicle["year"], "registration": vehicle["registration"] or "", "colour": "",
        "vehicle_vin": vehicle["vin"] or "", "current_mileage": vehicle["mileage"], "notes": "",
        "customer": {
            "id": vehicle["customer_id"],
            "first_name": vehicle["first_name"] or "",
            "surname": vehicle["last_name"] or "",
            "phone": vehicle["whatsapp_number"] or "",
            "email": vehicle["email"] or "",
        },
        "service_history": service_list,
        "booking_history": booking_list,
        "mileage_history": [],
    }
    return render_template("vehicle_profile.html", vehicle=vehicle_obj)


@vehicles_bp.route("/vehicles/<int:vehicle_id>/edit", methods=["GET", "POST"])
@login_required
def vehicle_edit(vehicle_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        vehicle = query_db(
            "SELECT id, make, model, year, registration, vin, mileage, created_at FROM vehicles WHERE id = %s AND location_id = %s",
            (vehicle_id, location_id), one=True,
        )
        if not vehicle:
            abort(404)

        vehicle_data = {
            "id": vehicle["id"], "make": vehicle["make"] or "", "model": vehicle["model"] or "",
            "year": vehicle["year"] or "", "registration": vehicle["registration"] or "", "colour": "",
            "vin": vehicle["vin"] or "", "mileage": vehicle["mileage"] or "", "notes": "",
            "created_at": vehicle["created_at"],
        }
        return render_template("vehicle_edit.html", vehicle=vehicle_data)

    elif request.method == "POST":
        form_data = request.form

        existing = query_db(
            "SELECT id, customer_id, location_id FROM vehicles WHERE id = %s AND location_id = %s",
            (vehicle_id, location_id), one=True,
        )
        if not existing:
            abort(404)

        make = form_data.get("make", "").strip()
        model = form_data.get("model", "").strip()
        year_str = form_data.get("year", "").strip()
        registration = form_data.get("registration", "").strip()
        vin = form_data.get("vin", "").strip()
        mileage_str = form_data.get("mileage", "").strip()
        notes = form_data.get("notes", "").strip()
        # `colour` is intentionally accepted from the form and discarded --
        # there is no real column for it in the current schema (see module
        # docstring); it was never actually being persisted before this
        # fix either, since the previous code wrote to a column
        # (vehicles.colour) that doesn't exist and would have crashed
        # before reaching any UPDATE at all.

        year = int(year_str) if year_str.isdigit() else None
        mileage = int(mileage_str) if mileage_str.isdigit() else None
        now = utc_now()

        execute_db(
            "UPDATE vehicles SET make=%s, model=%s, year=%s, registration=%s, vin=%s, mileage=%s, updated_at=%s "
            "WHERE id=%s AND location_id=%s",
            (make, model, year, registration, vin, mileage, now, vehicle_id, location_id),
        )

        flash('Vehicle updated successfully', 'success')
        return redirect(url_for('vehicles.vehicle_profile', vehicle_id=vehicle_id))
