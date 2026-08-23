import json

from database import execute_db, query_db, utc_now
from repositories.vehicle_repository import (
    get_vehicle_by_registration,
    get_vehicle_by_vin,
)

def upsert_vehicle(location_id, customer_id, make, model, year, registration, colour, vin, mileage, notes=None):
    """
    Insert or update a vehicle and return the vehicle ID.
    If a vehicle with the same registration or VIN exists in the location, update it.
    Otherwise, insert a new vehicle.
    Also handles mileage history: when updating mileage to a higher value, the old mileage is stored in a JSON array in metadata_json.
    Workshop notes are stored in the metadata_json under the key 'notes'.
    """
    existing_vehicle = None
    if registration:
        existing_vehicle = get_vehicle_by_registration(registration, location_id)
    if not existing_vehicle and vin:
        existing_vehicle = get_vehicle_by_vin(vin, location_id)

    now = utc_now()

    if existing_vehicle:
        # Update existing vehicle
        updates = {
            "make": make,
            "model": model,
            "year": year,
            "colour": colour,
            "updated_at": now
        }

        # Prepare metadata updates
        metadata = existing_vehicle.get("metadata_json")
        data = {}
        if metadata:
            try:
                data = json.loads(metadata)
                if not isinstance(data, dict):
                    data = {}
            except (json.JSONDecodeError, TypeError):
                data = {}
        # Update notes if provided
        if notes is not None:
            data["notes"] = notes

        # Handle mileage and history
        if mileage is not None:
            current_mileage = existing_vehicle["current_mileage"]
            if current_mileage is not None and mileage > current_mileage:
                # We need to add the current_mileage to the history and then update the current_mileage
                history = data.get("mileage_history", [])
                # Append the old mileage
                history.append(current_mileage)
                data["mileage_history"] = history
                # Update the current_mileage
                updates["current_mileage"] = mileage
            else:
                # Either setting for the first time or new mileage is not greater, just set the current_mileage
                updates["current_mileage"] = mileage

        # Update metadata_json if we have changes
        if data:
            updates["metadata_json"] = json.dumps(data)

        # Build the update query
        set_clause = ", ".join([f"{key}=%s" for key in updates.keys()])
        query = f"UPDATE vehicles SET {set_clause} WHERE id=%s AND location_id=%s"
        params = list(updates.values()) + [existing_vehicle["id"], location_id]
        execute_db(query, tuple(params))

        return existing_vehicle["id"]
    else:
        # Insert new vehicle only when the customer belongs to this location.
        customer = query_db("SELECT id FROM customers WHERE id=%s AND location_id=%s", (customer_id, location_id), one=True)
        if not customer:
            raise PermissionError("customer does not belong to location")
        # Prepare metadata_json for new vehicle
        metadata = {}
        if mileage is not None:
            # No history for a new vehicle
            metadata["mileage_history"] = []
        if notes is not None:
            metadata["notes"] = notes
        metadata_json = json.dumps(metadata) if metadata else None

        execute_db(
            """
            INSERT INTO vehicles (
                location_id, customer_id, make, model, year, vehicle_vin, vin, license_plate, registration,
                current_mileage, mileage, fuel_type, last_service_date, last_service_mileage,
                next_service_due_date, next_service_due_mileage, service_notes,
                metadata_json, created_at, updated_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                location_id, customer_id,
                make,
                model,
                year,
                vin,
                vin,
                registration,
                registration,
                mileage,
                mileage,
                None,  # fuel_type
                None,  # last_service_date
                None,  # last_service_mileage
                None,  # next_service_due_date
                None,  # next_service_due_mileage
                None,  # service_notes
                metadata_json,
                now,
                now
            ),
        )

        # Return the ID of the newly inserted vehicle
        row = get_vehicle_by_vin(vin, location_id) if vin else get_vehicle_by_registration(registration, location_id)
        return row["id"] if row else None