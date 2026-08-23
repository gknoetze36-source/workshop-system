"""
Communication Repository for Workshop System Version 2.

This repository handles database operations for the Communication entity.
It interacts with the existing tables:

- communication_logs
- reminder_campaigns
- messaging_accounts
- chatbot_messages
- chatbot_usage_daily
- chatbot_usage_monthly

Repository Responsibilities
---------------------------
- Communication log retrieval
- Reminder retrieval
- Messaging account queries
- Chatbot usage queries
- Communication dashboard support

Notes
-----
- All SQL is parameterized.
- Location isolation is handled through _scope_clause().
- Business validation belongs in the service layer.
"""

from database import query_db


# ============================================================================
# Internal Helpers
# ============================================================================

def _scope_clause(user, alias="cm"):
    """
    Build the SQL scope clause for the supplied user.

    Super Admin:
        Full access.

    Location Admin:
        Limited to location.

    Location Users:
        Limited to location.
    """

    role = user.get("role")

    if role == "super_admin":
        return "1=1", []

    if role == "location_admin":
        return f"{alias}.location_id = %s", [user["location_id"]]

    return f"{alias}.location_id = %s", [user["location_id"]]


# ============================================================================
# Communication Log Queries
# ============================================================================

def get_communication_logs_count_by_date(date):
    """
    Return the number of communication logs created on a specific date.

    Args:
        date (str): YYYY-MM-DD

    Returns:
        int
    """

    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE substr(created_at, 1, 10) = %s
    """

    result = query_db(sql, (date,), one=True)

    return result["total"] if result else 0


def get_communication_logs_count_by_location(location_id):
    """
    Return the total number of communication logs
    for a location.
    """

    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE location_id = %s
    """

    result = query_db(sql, (location_id,), one=True)

    return result["total"] if result else 0


def get_communication_logs_failed_count_by_location(location_id):
    """
    Return the number of failed communication logs
    for a location.
    """

    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE location_id = %s
          AND status LIKE 'failed%%'
    """

    result = query_db(sql, (location_id,), one=True)

    return result["total"] if result else 0


def get_communication_logs_by_booking_id(booking_id):
    """
    Return all communication logs linked to a booking.
    """

    sql = """
        SELECT *
        FROM communication_logs
        WHERE booking_id = %s
        ORDER BY created_at DESC
    """

    return query_db(sql, (booking_id,))


# ============================================================================
# Messaging Accounts
# ============================================================================

def get_messaging_accounts_whatsapp_connected():
    """
    Return the number of active Meta WhatsApp accounts.
    """

    sql = """
        SELECT COUNT(*) AS total
        FROM messaging_accounts
        WHERE provider = 'meta'
          AND is_active = TRUE
    """

    result = query_db(sql, (), one=True)

    return result["total"] if result else 0


# ============================================================================
# Reminder Queries
# ============================================================================

_BASE_REMINDER_SELECT = """
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
JOIN bookings b
    ON b.id = rc.booking_id
LEFT JOIN locations br
    ON br.id = rc.location_id
LEFT JOIN locations f
    ON f.id = rc.location_id
"""


def get_reminders_for_user(user):
    """
    Return all reminders visible to the supplied user.
    """

    clause, args = _scope_clause(user, alias="rc")

    sql = f"""
        {_BASE_REMINDER_SELECT}
        WHERE {clause}
        ORDER BY
            rc.scheduled_for DESC,
            rc.created_at DESC
    """

    return query_db(sql, tuple(args))


def get_reminder_by_id(reminder_id):
    """
    Return a single reminder by its ID.
    """

    sql = f"""
        {_BASE_REMINDER_SELECT}
        WHERE rc.id = %s
    """

    return query_db(sql, (reminder_id,), one=True)

# ============================================================================
# Communication Message Queries
# ============================================================================

def _get_messages(
    location_id,
    *,
    channel=None,
    status_like=None,
    limit=None,
    offset=None,
):
    """
    Internal helper for retrieving communication logs.

    Public functions (SMS, Email, WhatsApp, Failed, etc.)
    call this helper to avoid duplicated SQL.
    """

    sql = """
        SELECT *
        FROM communication_logs
        WHERE location_id = %s
    """

    params = [location_id]

    if channel is not None:
        sql += " AND channel = %s"
        params.append(channel)

    if status_like is not None:
        sql += " AND status LIKE %s"
        params.append(status_like)

    sql += " ORDER BY created_at DESC"

    if limit is not None:
        sql += " LIMIT %s"
        params.append(limit)

        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)

    return query_db(sql, tuple(params))


def get_whatsapp_messages(location_id, limit=None, offset=None):
    """Return WhatsApp communication logs."""
    return _get_messages(
        location_id,
        channel="whatsapp",
        limit=limit,
        offset=offset,
    )


def get_email_messages(location_id, limit=None, offset=None):
    """Return Email communication logs."""
    return _get_messages(
        location_id,
        channel="email",
        limit=limit,
        offset=offset,
    )


def get_sms_messages(location_id, limit=None, offset=None):
    """Return SMS communication logs."""
    return _get_messages(
        location_id,
        channel="sms",
        limit=limit,
        offset=offset,
    )


def get_failed_messages(location_id, limit=None, offset=None):
    """Return failed communication logs."""
    return _get_messages(
        location_id,
        status_like="failed%",
        limit=limit,
        offset=offset,
    )


# ============================================================================
# Template / Conversation Stubs
# ============================================================================

def get_templates():
    """
    Placeholder for future template repository.
    """
    return []


def get_template_by_id(template_id):
    """
    Placeholder for future template repository.
    """
    return None


def get_delivery_status(message_id):
    """
    Placeholder for delivery tracking.
    """
    return None


def get_conversation(conversation_id):
    """
    Placeholder for conversation support.
    """
    return None


def get_conversations(user, limit=None, offset=None):
    """
    Placeholder for future conversation support.
    """
    return []


def get_chat_history(user, limit=None, offset=None):
    """
    Placeholder for future chat history.
    """
    return []


# ============================================================================
# Convenience Wrappers
# ============================================================================

def get_messages_for_booking(booking_id):
    """
    Alias for booking communication history.
    """
    return get_communication_logs_by_booking_id(booking_id)


def get_messages_for_customer(location_id, phone, limit=None, offset=None):
    """
    Placeholder for future customer messaging lookup.
    """
    return []


def get_messages_for_vehicle(vin, location_id):
    """
    Placeholder for vehicle communication history.
    """
    return []


# ============================================================================
# Retry Queue / Reminder Helpers
# ============================================================================

def get_retry_queue(location_id, limit=None, offset=None):
    """
    Placeholder for retry queue implementation.

    NOTE:
        Parameter renamed from 'franchid_id'
        to 'location_id'.
    """
    return []


def get_sent_reminders(user):
    """
    Placeholder for sent reminders.
    """
    return []


def get_pending_reminders(user):
    """
    Return pending reminders only.
    """

    reminders = get_reminders_for_user(user)

    return [
        reminder
        for reminder in reminders
        if reminder.get("status") == "Pending"
    ]


# ============================================================================
# Dashboard
# ============================================================================

def get_communication_dashboard(user):
    """
    Return communication dashboard statistics.
    """

    if not user:
        return {
            "messages_today": 0,
            "failed_messages": 0,
            "whatsapp_connected": 0,
            "reminders_pending": 0,
        }

    messages_today = get_communication_logs_count_by_date(
        user.get("utc_today", "")
    )

    failed_messages = get_communication_logs_failed_count_by_location(
        user.get("location_id")
    )

    whatsapp_connected = get_messaging_accounts_whatsapp_connected()

    reminders_pending = len(
        get_pending_reminders(user)
    )

    return {
        "messages_today": messages_today,
        "failed_messages": failed_messages,
        "whatsapp_connected": whatsapp_connected,
        "reminders_pending": reminders_pending,
    }

# ============================================================================
# Chatbot Usage Queries
# ============================================================================

def get_chatbot_messages_daily(location_id, date):
    """
    Return chatbot daily usage for a location.
    """

    sql = """
        SELECT *
        FROM chatbot_usage_daily
        WHERE location_id = %s
          AND usage_date = %s
    """

    return query_db(sql, (location_id, date), one=True)


def get_chatbot_messages_monthly(location_id, year_month):
    """
    Return chatbot monthly usage.
    """

    sql = """
        SELECT *
        FROM chatbot_usage_monthly
        WHERE location_id = %s
          AND usage_month = %s
    """

    return query_db(sql, (location_id, year_month), one=True)


def get_chatbot_message_by_id(usage_monthly_id):
    """
    Return a chatbot monthly usage record.
    """

    sql = """
        SELECT *
        FROM chatbot_usage_monthly
        WHERE id = %s
    """

    return query_db(sql, (usage_monthly_id,), one=True)


# ============================================================================
# Chatbot Inbox
# ============================================================================

def get_chatbot_messages_inbox(user):
    """
    Return chatbot inbox messages visible to the supplied user.

    Visibility Rules
    ----------------
    Super Admin:
        All messages.

    Location Admin:
        Messages within their location.

    Location Users:
        Messages within their location.
    """

    clause, args = _scope_clause(user, alias="cm")

    sql = f"""
        SELECT
            cm.*,
            f.name AS location_name,
            b.name AS location_name,
            u.username AS actor_name
        FROM chatbot_messages cm
        LEFT JOIN locations f
            ON f.id = cm.location_id
        LEFT JOIN locations b
            ON b.id = cm.location_id
        LEFT JOIN users u
            ON u.id = cm.user_id
        WHERE {clause}
        ORDER BY
            cm.created_at DESC
    """

    return query_db(sql, tuple(args))