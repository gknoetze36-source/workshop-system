from database.query import fetch_all


def _fetch_all(query, args=()):
    return fetch_all(query, args=args) or []


def get_location_report(location_id):
    return _fetch_all(
        """
        SELECT
            b.name,
            COUNT(*) AS bookings,
            SUM(price) AS revenue
        FROM bookings
        JOIN locations b ON b.id = bookings.location_id
        WHERE location_id=%s
        GROUP BY b.name
        """,
        (location_id,),
    )


def get_service_profit(location_id):
    return _fetch_all(
        """
        SELECT
            service,
            SUM(price) AS revenue
        FROM bookings
        WHERE location_id=%s
        GROUP BY service
        """,
        (location_id,),
    )