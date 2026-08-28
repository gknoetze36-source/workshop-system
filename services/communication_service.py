from database import execute_db, fetch_all, fetch_one, utc_now
from helpers.common import scope_clause

def log_communication(booking, reminder, channel, recipient, subject, body, status, user_id=None, external_target=""):
    """Record that an outbound message was attempted.

    Two defects were fixed here:

    1. The INSERT declared 12 columns but supplied 13 placeholders and 13
       values -- booking["location_id"] was passed twice, once for
       location_id and once in the position belonging to user_id. Every call
       therefore raised, so communication_logs was never written. That also
       silently disabled the duplicate-send guard in
       services/messaging_service.py::can_send_outbound(), which suppresses a
       repeat of the same subject to the same recipient within 12 hours by
       looking for a prior row in this table. With no rows ever written, that
       guard could never fire.

    2. The rendered message body is no longer persisted. Outbound text is
       generated from templates and can be re-derived, so storing it turned
       this table into a store of personal information for no operational
       benefit. `subject` is still stored because can_send_outbound() matches
       on it, and the delivery metadata (channel, recipient, status, times)
       is what the operational dashboards actually read.

    The `body` parameter is retained in the signature so the four existing
    call sites do not need to change; it is deliberately not written.
    """
    sent = str(status or "").startswith("sent")
    execute_db(
        """
        INSERT INTO communication_logs (
            booking_id, reminder_id, location_id, user_id, channel,
            recipient, subject, status, external_target, created_at, sent_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            booking["id"],
            reminder["id"] if reminder else None,
            booking["location_id"],
            user_id,
            channel,
            recipient,
            subject,
            status,
            external_target,
            utc_now(),
            utc_now() if sent else None,
        ),
    )


def update_reminder_status(reminder_id, status, channel="", count_as_send=False):
    reminder = fetch_one("SELECT * FROM reminder_campaigns WHERE id=%s", (reminder_id,))
    if not reminder:
        return

    send_count = int(reminder.get("send_count") or 0) + (1 if count_as_send else 0)
    sent_at = utc_now() if count_as_send else reminder.get("sent_at")
    execute_db(
        """
        UPDATE reminder_campaigns
        SET status=%s,
            last_channel_used=%s,
            send_count=%s,
            updated_at=%s,
            sent_at=%s
        WHERE id=%s
        """,
        (status, channel or reminder.get("last_channel_used"), send_count, utc_now(), sent_at, reminder_id),
    )


def fetch_reminders_for_user(user):
    clause, args = scope_clause(user, alias="rc")
    return fetch_all(
        """
        SELECT
            rc.*,
            b.booking_reference,
            b.first_name,
            b.surname,
            b.phone,
            b.customer_email,
            b.service,
            b.service_level,
            b.work_to_be_done,
            b.preferred_contact_method,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone,
            f.name AS location_name,
            f.slug AS location_slug
        FROM reminder_campaigns rc
        JOIN bookings b ON b.id = rc.booking_id
        LEFT JOIN locations br ON br.id = rc.location_id
        LEFT JOIN locations f ON f.id = rc.location_id
        WHERE
        """
        + clause
        + " ORDER BY rc.scheduled_for DESC, rc.created_at DESC",
        tuple(args),
    )


def fetch_reminder(reminder_id):
    return fetch_one(
        """
        SELECT
            rc.*,
            b.booking_reference,
            b.first_name,
            b.surname,
            b.phone,
            b.customer_email,
            b.service,
            b.service_level,
            b.work_to_be_done,
            b.preferred_contact_method,
            br.name AS location_name,
            br.slug AS location_slug,
            br.contact_email AS location_contact_email,
            br.contact_phone AS location_contact_phone,
            f.name AS location_name,
            f.slug AS location_slug
        FROM reminder_campaigns rc
        JOIN bookings b ON b.id = rc.booking_id
        LEFT JOIN locations br ON br.id = rc.location_id
        LEFT JOIN locations f ON f.id = rc.location_id
        WHERE rc.id=%s
        """,
        (reminder_id,),
    )


def reminder_in_scope(reminder, user):
    if not reminder:
        return False
    if user["role"] == "super_admin":
        return True
    if user["role"] == "location_admin":
        return reminder.get("location_id") == user.get("location_id")
    return reminder.get("location_id") == user.get("location_id")
