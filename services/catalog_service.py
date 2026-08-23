from database import execute_db, query_db, utc_now, fetch_one
from helpers.common import db_bool



def ensure_service(location_id, service_name):
    service_name = (service_name or "").strip()
    if not service_name:
        return None

    service = fetch_one(
        """
        SELECT id
        FROM services
        WHERE location_id=%s
          AND COALESCE(location_id,0)=COALESCE(%s,0)
          AND lower(name)=lower(%s)
        ORDER BY id DESC
        LIMIT 1
        """,
        (location_id, service_name),
    )

    if service:
        return service["id"]

    execute_db(
        """
        INSERT INTO services (
            location_id, name, category, active, created_at, updated_at
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        """,
        (location_id, service_name, None, db_bool(True), utc_now(), utc_now()),
    )

    row = fetch_one(
        """
        SELECT id
        FROM services
        WHERE location_id=%s
          AND lower(name)=lower(%s)
        ORDER BY id DESC
        LIMIT 1
        """,
        (location_id, service_name),
    )

    return row["id"] if row else None