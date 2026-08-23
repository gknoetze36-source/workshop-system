from database import fetch_all

_fetch_all = fetch_all

def monthly_usage_summary(user=None):
    clauses = []
    args = []
    if user and user["role"] != "super_admin":
        clauses.append("cum.location_id=%s")
        args.append(user["location_id"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return _fetch_all(
        """
        SELECT cum.*, f.name AS location_name, f.monthly_base_price, f.monthly_message_limit, f.overage_price_per_message, f.active
        FROM chatbot_usage_monthly cum
        LEFT JOIN locations f ON f.id = cum.location_id
        """
        + where
        + " ORDER BY cum.usage_month DESC, f.name",
        tuple(args),
    )


def daily_usage_summary(user=None):
    clauses = []
    args = []
    if user and user["role"] != "super_admin":
        clauses.append("cud.location_id=%s")
        args.append(user["location_id"])
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    return _fetch_all(
        """
        SELECT cud.*, f.name AS location_name
        FROM usage_daily cud
        LEFT JOIN locations f ON f.id = cud.location_id
        """
        + where
        + " ORDER BY cud.usage_date DESC, f.name",
        tuple(args),
    )
