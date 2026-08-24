from database import execute_db, fetch_one
from helpers.dates import utc_today
from database import utc_now

_fetch_one = fetch_one

def track_message_usage(location_id, count=1):
    if not location_id or count <= 0:
        return
    today = utc_today()
    month_key = today[:7]
    location = _fetch_one("SELECT * FROM locations WHERE id=%s", (location_id,))
    if not location:
        return

    limit = int(location.get("monthly_message_limit") or 2000)
    overage_price = float(location.get("overage_price_per_message") or 0.5)
    base_price = float(location.get("monthly_base_price") or 0)

    daily = _fetch_one("SELECT * FROM usage_daily WHERE location_id=%s AND usage_date=%s", (location_id, today))
    if daily:
        messages_used = int(daily.get("messages_used") or 0) + count
        extra_messages = max(messages_used - limit, 0)
        execute_db(
            "UPDATE usage_daily SET messages_used=%s, extra_messages=%s, extra_cost=%s, updated_at=%s WHERE id=%s",
            (messages_used, extra_messages, round(extra_messages * overage_price, 2), utc_now(), daily["id"]),
        )
    else:
        execute_db(
            "INSERT INTO usage_daily (location_id, usage_date, messages_used, extra_messages, extra_cost, created_at, updated_at) VALUES (%s, %s, %s, 0, 0, %s, %s)",
            (location_id, today, count, utc_now(), utc_now()),
        )

    monthly = _fetch_one("SELECT * FROM chatbot_usage_monthly WHERE location_id=%s AND usage_month=%s", (location_id, month_key))
    if monthly:
        message_count = int(monthly.get("message_count") or 0) + count
        extra_messages = max(message_count - limit, 0)
        overage_cost = round(extra_messages * overage_price, 2)
        execute_db(
            "UPDATE chatbot_usage_monthly SET message_count=%s, message_limit=%s, extra_messages=%s, base_price=%s, overage_price=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s",
            (message_count, limit, extra_messages, base_price, overage_price, overage_cost, round(base_price + overage_cost, 2), utc_now(), monthly["id"]),
        )
    else:
        execute_db(
            "INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, message_limit, extra_messages, base_price, overage_price, overage_cost, total_due, created_at, updated_at) VALUES (%s, %s, %s, %s, 0, %s, %s, 0, %s, %s, %s)",
            (location_id, month_key, count, limit, base_price, overage_price, base_price, utc_now(), utc_now()),
        )

    execute_db(
        "UPDATE locations SET messages_used=COALESCE(messages_used, 0) + %s, updated_at=%s WHERE id=%s",
        (count, utc_now(), location_id),
    )
