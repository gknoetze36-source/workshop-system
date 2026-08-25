import os
from datetime import datetime, timedelta

from database import execute_db, transaction, utc_now, fetch_one, fetch_all
from helpers.dates import utc_today

_fetch_one = fetch_one
_fetch_all = fetch_all

def close_billing_period(usage_month=None, location_id=None):
    """
    Finalise a billing period and generate or update billing records.

    Previously anchored on chatbot_usage_monthly rows -- a location that
    had never sent a single WhatsApp message (a brand new signup that
    hasn't connected WhatsApp yet, or one that only uses bookings/
    dashboard features) had no usage row to join against, so this
    produced zero billing_records for them, silently, even though they
    still owe the flat monthly base fee. Found while end-to-end testing
    the payment wall: a genuinely fresh location with no usage never got
    billed at all via this function.

    Now anchored on locations (active=TRUE) instead, LEFT JOIN to
    chatbot_usage_monthly -- every active location gets a bill each
    period regardless of whether they've used any metered features yet.
    A missing usage row is created (zero usage) before the existing
    per-row update/insert logic below runs unchanged.
    """
    usage_month = (usage_month or utc_today()[:7]).strip()
    clauses = ["l.active=TRUE"]
    args = []
    if location_id:
        clauses.append("l.id=%s")
        args.append(location_id)
    locations = _fetch_all(
        """
        SELECT l.id AS location_id, l.monthly_base_price, l.monthly_message_limit, l.overage_price_per_message,
               cum.id AS usage_row_id, cum.message_count, cum.message_limit AS usage_message_limit,
               cum.base_price AS usage_base_price, cum.overage_price AS usage_overage_price
        FROM locations l
        LEFT JOIN chatbot_usage_monthly cum ON cum.location_id = l.id AND cum.usage_month = %s
        WHERE
        """
        + " AND ".join(clauses),
        tuple([usage_month] + args),
    )
    with transaction():
        closed = 0
        for row in locations:
            usage_row_id = row.get("usage_row_id")
            if usage_row_id is None:
                # No usage row for this location/period at all -- create a
                # zero-usage one so the update below has something to
                # update, matching what services/usage_service.py's
                # track_message_usage() creates on a real message send.
                execute_db(
                    "INSERT INTO chatbot_usage_monthly (location_id, usage_month, message_count, base_price, overage_price, extra_messages, overage_cost, total_due, created_at, updated_at) "
                    "VALUES (%s, %s, 0, %s, %s, 0, 0, %s, %s, %s)",
                    (row["location_id"], usage_month, row.get("monthly_base_price") or 0, row.get("overage_price_per_message") or 0.5,
                     row.get("monthly_base_price") or 0, utc_now(), utc_now()),
                )
                usage_row_id = _fetch_one(
                    "SELECT id FROM chatbot_usage_monthly WHERE location_id=%s AND usage_month=%s",
                    (row["location_id"], usage_month),
                )["id"]
                message_count = 0
            else:
                message_count = row.get("message_count") or 0

            limit = int(row.get("usage_message_limit") or row.get("monthly_message_limit") or 2000)
            overage_price = float(row.get("usage_overage_price") or row.get("overage_price_per_message") or 0.5)
            base_price = float(row.get("usage_base_price") or row.get("monthly_base_price") or 0)
            extra = max(int(message_count) - limit, 0)
            # round() here, not just at the Paystack charge boundary: plain
            # float multiplication produces artifacts like
            # 333 * 0.1 == 33.300000000000004. charge_overage() already
            # quantizes to cents before charging, so the customer was never
            # actually overcharged by this -- but the raw value was still
            # what got stored in billing_records and chatbot_usage_monthly,
            # so any dashboard, receipt email, or export reading those
            # columns directly would have shown the artifact.
            usage_amount = round(extra * overage_price, 2)
            total_due = round(base_price + usage_amount, 2)
            execute_db(
                "UPDATE chatbot_usage_monthly SET message_limit=%s, extra_messages=%s, base_price=%s, overage_price=%s, overage_cost=%s, total_due=%s, updated_at=%s WHERE id=%s",
                (limit, extra, base_price, overage_price, usage_amount, total_due, utc_now(), usage_row_id),
            )
            existing = _fetch_one("SELECT id FROM billing_records WHERE location_id=%s AND billing_period=%s", (row["location_id"], usage_month))
            if existing:
                execute_db("UPDATE billing_records SET amount=%s, base_amount=%s, usage_amount=%s, updated_at=%s WHERE id=%s", (total_due, base_price, usage_amount, utc_now(), existing["id"]))
            else:
                execute_db(
                    "INSERT INTO billing_records (location_id, amount, base_amount, usage_amount, status, billing_period, created_at, updated_at) VALUES (%s, %s, %s, %s, 'unpaid', %s, %s, %s)",
                    (row["location_id"], total_due, base_price, usage_amount, usage_month, utc_now(), utc_now()),
                )
            closed += 1
        return closed


def mark_billing_paid(location_id, billing_period, payment_reference=""):
    """
    Mark a billing period as paid and renew the subscription.
    """
    subscription_start = utc_today()
    subscription_end = (datetime.utcnow() + timedelta(days=30)).strftime("%Y-%m-%d")
    with transaction():
        execute_db(
            """
            UPDATE locations
            SET subscription_status='active', subscription_start=%s, subscription_end=%s,
                messages_used=0, updated_at=%s
            WHERE id=%s
            """,
            (subscription_start, subscription_end, utc_now(), location_id),
        )
        execute_db(
            "UPDATE chatbot_usage_monthly SET payment_status='Paid', paid_at=%s, payment_reference=%s, updated_at=%s WHERE location_id=%s AND usage_month=%s",
            (utc_now(), payment_reference, utc_now(), location_id, billing_period),
        )
        execute_db(
            "UPDATE billing_records SET status='paid', payment_reference_id=%s, paid_at=%s, updated_at=%s WHERE location_id=%s AND billing_period=%s",
            (payment_reference, utc_now(), utc_now(), location_id, billing_period),
        )


def create_payment_link(billing_id):
    billing = _fetch_one("SELECT br.*, l.name AS location_name, l.contact_email AS location_email FROM billing_records br LEFT JOIN locations l ON l.id=br.location_id WHERE br.id=%s AND l.active=TRUE", (billing_id,))
    if not billing:
        return None
    base_url = os.environ.get("PUBLIC_BASE_URL", "").rstrip("/")
    reference = f"billing-{billing['id']}-{billing['billing_period']}".replace(" ", "-")
    callback_url = f"{base_url}/manage/locations" if base_url else ""
    email = billing.get("location_email") or os.environ.get("BILLING_EMAIL") or os.environ.get("ADMIN_EMAIL")
    metadata = {"location_id": billing["location_id"], "billing_period": billing["billing_period"], "billing_record_id": billing["id"]}
    from services.paystack_service import initialize_transaction

    result = initialize_transaction(email, billing.get("amount") or 0, reference, callback_url=callback_url, metadata=metadata)
    payment_link = ((result.get("data") or {}).get("authorization_url")) or callback_url
    execute_db("UPDATE billing_records SET payment_link=%s, updated_at=%s WHERE id=%s", (payment_link, utc_now(), billing_id))
    return payment_link


def expire_due_subscriptions():
    today = utc_today()
    execute_db(
        "UPDATE locations SET subscription_status='inactive', updated_at=%s WHERE active=TRUE AND subscription_end IS NOT NULL AND subscription_end<>'' AND subscription_end < %s AND subscription_status NOT IN ('inactive','cancelled')",
        (utc_now(), today),
    )


