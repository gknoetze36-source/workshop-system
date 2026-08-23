from flask import Blueprint, abort, flash, redirect, render_template, request, url_for
from services.auth_service import active_location_required, login_required, current_user
from database import query_db
from helpers.dates import utc_today
import json


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

    # Helper function to get the latest booking for a vehicle by VIN or plate
    def get_latest_booking(vin, plate):
        query = """
            SELECT b.booking_reference, b.scheduled_date, b.status, b.work_to_be_done, b.internal_notes
            FROM bookings b
            WHERE b.location_id = %s
              AND (b.vehicle_vin = %s OR b.registration_number = %s)
            ORDER BY b.scheduled_date DESC
            LIMIT 1
        """
        params = (location_id, vin, plate)
        result = query_db(query, params, one=True)
        return result

    # Helper function to get the next booking (future) for a vehicle by VIN or plate
    def get_next_booking(vin, plate):
        query = """
            SELECT b.booking_reference, b.scheduled_date, b.status, b.work_to_be_done, b.internal_notes
            FROM bookings b
            WHERE b.location_id = %s
              AND (b.vehicle_vin = %s OR b.registration_number = %s)
              AND b.scheduled_date >= %s
              AND b.status NOT IN ('Completed', 'Cancelled')
            ORDER BY b.scheduled_date ASC
            LIMIT 1
        """
        params = (location_id, vin, plate, today)
        result = query_db(query, params, one=True)
        return result

    # Get all customers for the location
    customers_raw = query_db(
        """
        SELECT id, first_name, surname, phone, email, metadata_json, created_at
        FROM customers
        WHERE location_id = %s
        ORDER BY surname, first_name
        """,
        (location_id,)
    ) or []

    # Build the customer-vehicle rows
    customers_data = []
    for c in customers_raw:
        # Parse customer metadata for notes
        customer_meta = {}
        if c['metadata_json']:
            try:
                customer_meta = json.loads(c['metadata_json'])
            except (json.JSONDecodeError, TypeError):
                customer_meta = {}
        customer_name = f"{c['first_name'] or ''} {c['surname'] or ''}".strip() or "Unknown"
        customer_whatsapp = c['phone'] or ""
        customer_email = c['email'] or ""
        customer_notes = customer_meta.get('notes', "")
        customer_created = c['created_at']

        # Get vehicles for this customer
        vehicles_raw = query_db(
            """
            SELECT id, make, model, year, license_plate, colour, vehicle_vin, metadata_json
            FROM vehicles
            WHERE customer_id = %s AND location_id = %s
            ORDER BY updated_at DESC
            """,
            (c['id'], location_id)
        ) or []

        if vehicles_raw:
            for v in vehicles_raw:
                # Parse vehicle metadata for notes and mileage history
                vehicle_meta = {}
                if v['metadata_json']:
                    try:
                        vehicle_meta = json.loads(v['metadata_json'])
                    except (json.JSONDecodeError, TypeError):
                        vehicle_meta = {}
                vehicle_make = v['make'] or ""
                vehicle_model = v['model'] or ""
                vehicle_year = v['year']
                vehicle_registration = v['license_plate'] or ""
                vehicle_colour = v['colour'] or ""
                vehicle_vin = v['vehicle_vin'] or ""
                vehicle_notes = vehicle_meta.get('notes', "")

                # Get latest booking for this vehicle
                latest_booking = get_latest_booking(vehicle_vin, vehicle_registration)
                # Get next booking for this vehicle
                next_booking = get_next_booking(vehicle_vin, vehicle_registration)

                # Determine status from latest booking (if any)
                status = "-"
                if latest_booking:
                    status = latest_booking['status'] or "-"

                # Format dates
                last_visit = "-"
                if latest_booking and latest_booking['scheduled_date']:
                    last_visit = latest_booking['scheduled_date']

                next_booking_display = "-"
                if next_booking and next_booking['scheduled_date']:
                    next_booking_display = next_booking['scheduled_date']

                # Build the row
                row = {
                    'customer_id': c['id'],
                    'customer_name': customer_name,
                    'customer_whatsapp': customer_whatsapp,
                    'customer_email': customer_email,
                    'customer_notes': customer_notes,
                    'customer_created': customer_created,
                    'vehicle_id': v['id'],
                    'vehicle_make': vehicle_make,
                    'vehicle_model': vehicle_model,
                    'vehicle_year': vehicle_year,
                    'vehicle_registration': vehicle_registration,
                    'vehicle_colour': vehicle_colour,
                    'vehicle_vin': vehicle_vin,
                    'vehicle_notes': vehicle_notes,
                    'latest_booking': latest_booking,
                    'next_booking': next_booking,
                    'status': status,
                    'last_visit': last_visit,
                    'next_booking_display': next_booking_display,
                }
                customers_data.append(row)
        else:
            # Customer has no vehicles, show a row with empty vehicle fields
            row = {
                'customer_id': c['id'],
                'customer_name': customer_name,
                'customer_whatsapp': customer_whatsapp,
                'customer_email': customer_email,
                'customer_notes': customer_notes,
                'customer_created': customer_created,
                'vehicle_id': None,
                'vehicle_make': '',
                'vehicle_model': '',
                'vehicle_year': None,
                'vehicle_registration': '',
                'vehicle_colour': '',
                'vehicle_vin': '',
                'vehicle_notes': '',
                'latest_booking': None,
                'next_booking': None,
                'status': '-',
                'last_visit': '-',
                'next_booking_display': '-',
            }
            customers_data.append(row)

    # Sort by customer name
    customers_data.sort(key=lambda x: x['customer_name'].lower())

    return render_template("customers.html", customers=customers_data)

# Customer profile route
@customer_bp.route("/customers/<int:customer_id>")
@login_required
def customer_profile(customer_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    # Get customer with location scope
    customer = query_db(
        """
        SELECT id, first_name, surname, phone, email, metadata_json, created_at
        FROM customers
        WHERE id = %s AND location_id = %s
        """,
        (customer_id, location_id),
        one=True
    )
    if not customer:
        abort(404)

    # Parse customer metadata
    customer_meta = {}
    if customer['metadata_json']:
        try:
            customer_meta = json.loads(customer['metadata_json'])
        except (json.JSONDecodeError, TypeError):
            customer_meta = {}

    # Customer details
    customer_name = f"{customer['first_name'] or ''} {customer['surname'] or ''}".strip() or "Unknown"
    customer_whatsapp = customer['phone'] or ""
    customer_email = customer['email'] or ""
    customer_notes = customer_meta.get('notes', "")
    customer_created = customer['created_at']

    # Get vehicles for this customer
    vehicles = query_db(
        """
        SELECT id, make, model, year, license_plate, colour, vehicle_vin, current_mileage, metadata_json
        FROM vehicles
        WHERE customer_id = %s AND location_id = %s
        ORDER BY updated_at DESC
        """,
        (customer_id, location_id)
    ) or []

    # Process vehicles for display
    vehicle_list = []
    for v in vehicles:
        vehicle_meta = {}
        if v['metadata_json']:
            try:
                vehicle_meta = json.loads(v['metadata_json'])
            except (json.JSONDecodeError, TypeError):
                vehicle_meta = {}
        vehicle_list.append({
            'id': v['id'],
            'make': v['make'] or "",
            'model': v['model'] or "",
            'year': v['year'],
            'registration': v['license_plate'] or "",
            'colour': v['colour'] or "",
            'vin': v['vehicle_vin'] or "",
            'current_mileage': v['current_mileage'],
            'notes': vehicle_meta.get('notes', "")
        })

    # Get bookings for this customer (via their vehicles)
    bookings = query_db(
        """
        SELECT b.booking_reference, b.scheduled_date, b.status, b.service, b.work_to_be_done, b.internal_notes, b.current_mileage
        FROM bookings b
        JOIN vehicles v ON b.vehicle_id = v.id
        WHERE v.customer_id = %s AND v.location_id = %s
        ORDER BY b.scheduled_date DESC
        """,
        (customer_id, location_id)
    ) or []

    # Process bookings for display
    booking_list = []
    for b in bookings:
        booking_list.append({
            'reference': b['booking_reference'],
            'date': b['scheduled_date'],
            'status': b['status'],
            'service': b['service'] or b['work_to_be_done'] or '-',
            'work_to_be_done': b['work_to_be_done'] or '',
            'internal_notes': b['internal_notes'] or '',
            'mileage': b['current_mileage']
        })

    # Calculate statistics
    vehicle_count = len(vehicle_list)
    booking_count = len(booking_list)

    # Last visit (most recent booking date)
    last_visit = None
    if bookings:
        # Sort by date descending and take the first
        sorted_bookings = sorted(bookings, key=lambda x: x['date'], reverse=True)
        last_visit = sorted_bookings[0]['date']

    # Next booking (earliest future booking that is not completed or cancelled)
    next_booking = None
    today = utc_today()
    upcoming = [b for b in bookings if b['date'] and b['date'] >= today and b['status'] not in ('Completed', 'Cancelled')]
    if upcoming:
        upcoming_sorted = sorted(upcoming, key=lambda x: x['date'])
        next_booking = {
            'reference': upcoming_sorted[0]['reference'],
            'date': upcoming_sorted[0]['date'],
            'service': upcoming_sorted[0]['service']
        }

    # Prepare customer object for template
    customer_obj = {
        'id': customer['id'],
        'name': customer_name,
        'whatsapp': customer_whatsapp,
        'email': customer_email,
        'notes': customer_notes,
        'created_at': customer_created,
        'vehicle_count': vehicle_count,
        'booking_count': booking_count,
        'last_visit': last_visit,
        'next_booking': next_booking,
        'vehicles': vehicle_list,
        'bookings': booking_list
    }

    return render_template("customer_profile.html", customer=customer_obj)

# Customer edit route
@customer_bp.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
@login_required
def customer_edit(customer_id):
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        # Get customer for editing
        customer = query_db(
            """
            SELECT id, first_name, surname, phone, email, metadata_json, created_at
            FROM customers
            WHERE id = %s AND location_id = %s
            """,
            (customer_id, location_id),
            one=True
        )
        if not customer:
            abort(404)

        # Parse customer metadata
        customer_meta = {}
        if customer['metadata_json']:
            try:
                customer_meta = json.loads(customer['metadata_json'])
            except (json.JSONDecodeError, TypeError):
                customer_meta = {}

        # Prepare customer data for form
        customer_data = {
            'id': customer['id'],
            'first_name': customer['first_name'] or '',
            'surname': customer['surname'] or '',
            'phone': customer['phone'] or '',
            'email': customer['email'] or '',
            'notes': customer_meta.get('notes', ''),
            'created_at': customer['created_at']
        }

        return render_template("customer_edit.html", customer=customer_data)

    elif request.method == "POST":
        # Update customer
        form_data = request.form
        # Update customer in database
        # Note: We reuse the upsert_customer function from customer_service
        # but we need to ensure we are updating the existing customer.
        # However, upsert_customer is designed to insert or update based on phone/email.
        # For editing, we want to update the specific customer by ID.
        # We'll do a direct update.

        from database import execute_db, utc_now

        # Normalize input
        phone = form_data.get("phone", "").strip()
        email = (form_data.get("email") or "").strip().lower()
        first_name = form_data.get("first_name", "").strip()
        surname = form_data.get("surname", "").strip()
        full_name = " ".join(part for part in [first_name, surname] if part).strip() or form_data.get("customer_name", "").strip()
        notes = form_data.get("notes", "").strip()

        now = utc_now()

        # Update customer
        updates = {
            "first_name": first_name,
            "surname": surname,
            "full_name": full_name,
            "phone": phone,
            "email": email,
            "updated_at": now
        }

        # Handle notes in metadata_json
        metadata = {}
        if notes:
            metadata["notes"] = notes
        metadata_json = json.dumps(metadata) if metadata else None

        if metadata_json:
            updates["metadata_json"] = metadata_json

        # Build update query
        set_clause = ", ".join([f"{key}=%s" for key in updates.keys()])
        query = f"UPDATE customers SET {set_clause} WHERE id=%s AND location_id=%s"
        params = list(updates.values()) + [customer_id, location_id]
        execute_db(query, tuple(params))

        flash('Customer updated successfully', 'success')
        return redirect(url_for('customer.customer_profile', customer_id=customer_id))
