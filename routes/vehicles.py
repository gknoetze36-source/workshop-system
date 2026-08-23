from flask import Blueprint, abort, flash, jsonify, redirect, render_template, request, url_for
from database import query_db, execute_db, utc_now
from helpers.dates import utc_today
import json
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

    # Get vehicle with location scope
    vehicle = query_db(
        """
        SELECT v.id, v.make, v.model, v.year, v.license_plate, v.colour, v.vehicle_vin, v.current_mileage,
               v.metadata_json, v.created_at, c.id as customer_id, c.first_name, c.surname, c.phone, c.email, c.metadata_json as customer_metadata
        FROM vehicles v
        JOIN customers c ON v.customer_id = c.id
        WHERE v.id = %s AND v.location_id = %s
        """,
        (vehicle_id, location_id),
        one=True
    )
    if not vehicle:
        abort(404)

    # Parse vehicle metadata
    vehicle_meta = {}
    if vehicle['metadata_json']:
        try:
            vehicle_meta = json.loads(vehicle['metadata_json'])
        except (json.JSONDecodeError, TypeError):
            vehicle_meta = {}

    # Parse customer metadata
    customer_meta = {}
    if vehicle['customer_metadata']:
        try:
            customer_meta = json.loads(vehicle['customer_metadata'])
        except (json.JSONDecodeError, TypeError):
            customer_meta = {}

    # Vehicle details
    vehicle_make = vehicle['make'] or ""
    vehicle_model = vehicle['model'] or ""
    vehicle_year = vehicle['year']
    vehicle_registration = vehicle['license_plate'] or ""
    vehicle_colour = vehicle['colour'] or ""
    vehicle_vin = vehicle['vehicle_vin'] or ""
    vehicle_mileage = vehicle['current_mileage']
    vehicle_notes = vehicle_meta.get('notes', "")

    # Customer details
    customer_name = f"{vehicle['first_name'] or ''} {vehicle['surname'] or ''}".strip() or "Unknown"
    customer_whatsapp = vehicle['phone'] or ""
    customer_email = vehicle['email'] or ""
    customer_notes = customer_meta.get('notes', "")

    # Get service history for this vehicle
    service_history = query_db(
        """
        SELECT b.scheduled_date as date, b.work_to_be_done as service, b.status, b.current_mileage as mileage
        FROM bookings b
        WHERE b.vehicle_id = %s
          AND b.location_id = %s
          AND b.status IN ('Completed', 'Done')
        ORDER BY b.scheduled_date DESC
        """,
        (vehicle_id, location_id)
    ) or []

    # Process service history
    service_list = []
    for s in service_history:
        service_list.append({
            'date': s['date'],
            'service': s['service'] or '-',
            'status': s['status'],
            'mileage': s['mileage']
        })

    # Get booking history for this vehicle
    booking_history = query_db(
        """
        SELECT b.booking_reference, b.scheduled_date, b.status, b.work_to_be_done as service, b.current_mileage as mileage
        FROM bookings b
        WHERE b.vehicle_id = %s
          AND b.location_id = %s
        ORDER BY b.scheduled_date DESC
        """,
        (vehicle_id, location_id)
    ) or []

    # Process booking history
    booking_list = []
    for b in booking_history:
        booking_list.append({
            'reference': b['booking_reference'],
            'date': b['scaled_date'],
            'status': b['status'],
            'service': b['service'] or '-',
            'mileage': b['mileage']
        })

    # Get next booking (future)
    today = utc_today()
    next_booking = query_db(
        """
        SELECT b.booking_reference, b.scheduled_date, b.status, b.work_to_be_done as service
        FROM bookings b
        WHERE b.vehicle_id = %s
          AND b.location_id = %s
          AND b.scheduled_date >= %s
          AND b.status NOT IN ('Completed', 'Cancelled')
        ORDER BY b.scheduled_date ASC
        LIMIT 1
        """,
        (vehicle_id, location_id, today)
    )

    next_booking_obj = None
    if next_booking:
        next_booking_obj = {
            'reference': next_booking['booking_reference'],
            'date': next_booking['scheduled_date'],
            'service': next_booking['work_to_be_done'] or '-'
        }

    # Prepare vehicle object for template
    vehicle_obj = {
        'id': vehicle['id'],
        'make': vehicle_make,
        'model': vehicle_model,
        'year': vehicle_year,
        'registration': vehicle_registration,
        'colour': vehicle_colour,
        'vin': vehicle_vin,
        'current_mileage': vehicle_mileage,
        'notes': vehicle_notes,
        'customer': {
            'id': vehicle['customer_id'],
            'name': customer_name,
            'whatsapp': customer_whatsapp,
            'email': customer_email,
            'notes': customer_notes
        },
        'service_history': service_list,
        'booking_history': booking_list,
        'next_booking': next_booking_obj
    }

    return render_template("vehicle_profile.html", vehicle=vehicle_obj)

# Vehicle edit route
@vehicles_bp.route("/vehicles/<int:vehicle_id>/edit", methods=["GET", "POST"])
@login_required
def vehicle_edit(vehicle_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        # Get vehicle for editing
        vehicle = query_db(
            """
            SELECT id, make, model, year, license_plate, colour, vehicle_vin, current_mileage, metadata_json, created_at
            FROM vehicles
            WHERE id = %s AND location_id = %s
            """,
            (vehicle_id, location_id),
            one=True
        )
        if not vehicle:
            abort(404)

        # Parse vehicle metadata
        vehicle_meta = {}
        if vehicle['metadata_json']:
            try:
                vehicle_meta = json.loads(vehicle['metadata_json'])
            except (json.JSONDecodeError, TypeError):
                vehicle_meta = {}

        # Prepare vehicle data for form
        vehicle_data = {
            'id': vehicle['id'],
            'make': vehicle['make'] or '',
            'model': vehicle['model'] or '',
            'year': vehicle['year'] or '',
            'registration': vehicle['license_plate'] or '',
            'colour': vehicle['colour'] or '',
            'vin': vehicle['vehicle_vin'] or '',
            'mileage': vehicle['current_mileage'] or '',
            'notes': vehicle_meta.get('notes', ''),
            'created_at': vehicle['created_at']
        }

        return render_template("vehicle_edit.html", vehicle=vehicle_data)

    elif request.method == "POST":
        # Update vehicle
        form_data = request.form
        from database import execute_db, utc_now
        from services.vehicle_service import upsert_vehicle
        import json

        # Extract form data
        make = form_data.get("make", "").strip()
        model = form_data.get("model", "").strip()
        year_str = form_data.get("year", "").strip()
        registration = form_data.get("registration", "").strip()
        colour = form_data.get("colour", "").strip()
        vin = form_data.get("vin", "").strip()
        mileage_str = form_data.get("mileage", "").strip()
        notes = form_data.get("notes", "").strip()

        # Convert year and mileage to integers or None
        try:
            year = int(year_str) if year_str.isdigit() else None
        except ValueError:
            year = None
        try:
            mileage = int(mileage_str) if mileage_str.isdigit() else None
        except ValueError:
            mileage = None

        now = utc_now()

        # Get existing vehicle to preserve customer_id and location_id
        existing = query_db(
            "SELECT customer_id, location_id FROM vehicles WHERE id = %s AND location_id = %s",
            (vehicle_id, location_id),
            one=True
        )
        if not existing:
            abort(404)

        customer_id = existing['customer_id']
        # Verify location_id matches
        if existing['location_id'] != location_id:
            abort(403)  # Forbidden

        # Use upsert_vehicle from vehicle_service to update
        # Note: upsert_vehicle will update based on registration/vin, but we know the ID.
        # However, we want to update the specific vehicle by ID.
        # We'll do a direct update similar to customer_edit.

        # Prepare metadata
        metadata = {}
        if mileage is not None:
            # For update, we need to handle mileage history
            # We'll retrieve the existing metadata to preserve history
            existing_meta = {}
            if vehicle['metadata_json']:
                try:
                    existing_meta = json.loads(vehicle['metadata_json'])
                except (json.JSONDecodeError, TypeError):
                    existing_meta = {}
            # If mileage is increasing, add current mileage to history
            current_mileage = existing_meta.get('current_mileage') if 'current_mileage' in existing_meta else None
            if current_mileage is not None and mileage is not None and mileage > current_mileage:
                history = existing_meta.get('mileage_history', [])
                history.append(current_mileage)
                metadata['mileage_history'] = history
            # Update current mileage
            metadata['current_mileage'] = mileage
        else:
            # Keep existing mileage if not provided
            if vehicle['metadata_json']:
                try:
                    existing_meta = json.loads(vehicle['metadata_json'])
                except (json.JSONDecodeError, TypeError):
                    existing_meta = {}
                metadata = existing_meta

        if notes is not None:
            metadata['notes'] = notes
        metadata_json = json.dumps(metadata) if metadata else None

        # Update vehicle
        updates = {
            "make": make,
            "model": model,
            "year": year,
            "license_plate": registration,
            "colour": colour,
            "vehicle_vin": vin,
            "current_mileage": mileage,
            "metadata_json": metadata_json,
            "updated_at": now
        }

        # Remove None values to avoid setting columns to NULL unintentionally
        updates = {k: v for k, v in updates.items() if v is not None}

        if updates:
            set_clause = ", ".join([f"{key}=%s" for key in updates.keys()])
            query = f"UPDATE vehicles SET {set_clause} WHERE id=%s AND location_id=%s"
            params = list(updates.values()) + [vehicle_id, location_id]
            execute_db(query, tuple(params))

        flash('Vehicle updated successfully', 'success')
        return redirect(url_for('vehicles.vehicle_profile', vehicle_id=vehicle_id))
