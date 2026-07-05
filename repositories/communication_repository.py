"""
Communication Repository for Workshop System Version 2.

This repository handles all database operations for the Communication entity.
It interacts with the existing tables: communication_logs, reminder_campaigns,
messaging_accounts, chatbot_messages, chatbot_usage_daily, chatbot_usage_monthly, etc.
"""

from database import query_db


def _scope_clause(user, alias="cm"):
    """Replicate the scope_clause logic from platform_helpers for a given alias."""
    if user["role"] == "super_admin":
        return "1=1", []
    if user["role"] == "franchise_admin":
        return f"{alias}.franchise_id = %s", [user["franchise_id"]]
    return f"{alias}.branch_id = %s", [user["branch_id"]]


def get_communication_logs_count_by_date(date):
    """Get count of communication logs for a specific date (YYYY-MM-DD)."""
    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE substr(created_at, 1, 10) = %s
    """
    result = query_db(sql, (date,), one=True)
    return result['total'] if result else 0


def get_communication_logs_count_by_franchise(franchise_id):
    """Get total count of communication logs for a franchise."""
    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE franchise_id = %s
    """
    result = query_db(sql, (franchise_id,), one=True)
    return result['total'] if result else 0


def get_communication_logs_failed_count_by_franchise(franchise_id):
    """Get count of failed communication logs for a franchise."""
    sql = """
        SELECT COUNT(*) AS total
        FROM communication_logs
        WHERE franchise_id = %s AND status LIKE 'failed%%'
    """
    result = query_db(sql, (franchise_id,), one=True)
    return result['total'] if result else 0


def get_communication_logs_by_booking_id(booking_id):
    """Get all communication logs for a specific booking."""
    sql = """
        SELECT *
        FROM communication_logs
        WHERE booking_id = %s_filter = %s
        ORDER BY created_at DESC
    """
    return query_db(sql, (booking_id,))


def get_messaging_accounts_whatsapp_connected():
    """Get count of active WhatsApp messaging accounts (provider=meta)."""
    sql = """
        SELECT COUNT(*) AS total
        FROM messaging_accounts
        WHERE provider = 'meta' AND is_active = TRUE
    """
    result = query_db(sql, (), one=True)
    return result['total'] if result else 0


def get_reminders_for_user(user):
    """Get reminders for a user with joins (replicates platform_helpers.fetch_reminders_for_user)."""
    clause, args = _scope_clause(user, alias="rc")
    sql = f"""
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
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM reminder_campaigns rc
        JOIN bookings b ON b.id = rc.booking_id
        LEFT JOIN branches br ON br.id = rc.branch_id
        LEFT JOIN franchises f ON f.id = rc.franchise_id
        WHERE {clause}
        ORDER BY rc.scheduled_for DESC, rc.created_at DESC
    """
    return query_db(sql, tuple(args))


def get_reminder_by_id(reminder_id):
    """Get a single reminder by ID with joins (replicates platform_helpers.fetch_reminder)."""
    sql = """
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
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone,
            f.name AS fname,
            f.slug AS fs_slug
        FROM reminder_campaigns rc
        JOIN bookings b ON b.id = rc.booking_id
        LEFT JOIN branches br ON br.id = rc.branch_id
        LEFT JOIN franchises f ON f.id = rc.franchise_id
        WHERE rc.id = %s
    """
    return query_db(sql, (reminder_id,), one=True)


def get_whatsapp_messages(franchise_id, limit=None, offset=None):
    """Get WhatsApp messages for a franchise (channel='whatsapp')."""
    sql = """
        SELECT *
        FROM communication_logs
        WHERE franchise_id = %s AND channel = 'whatsapp'
        ORDER BY created_at DESC
    """
    if limit is not None:
        sql += " LIMIT %s"
        params = [franchise_id, limit]
        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)
        return query_db(sql, tuple(params))
    else:
        return query_db(sql, (franchise_id,))


def get_email_messages(franchise_id, limit=None, offset=None):
    """Get email messages for a franchise (channel='email')."""
    sql = """
        SELECT *
        FROM communication_logs
        WHERE franchise_id = %s AND channel = 'email'
        ORDER BY created_at DESC
    """
    if limit is not None:
        sql += " LIMIT %s"
        params = [franchise_id, limit]
        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)
        return query_db(sql, tuple(params))
    else:
        return query_db(sql, (franchise_id,))


def get_sms_messages(franchise_id, limit=None, offset=None):
    """Get SMS messages for a franchise (channel='sms')."""
    sql = """
        SELECT *
        FROM communication_logs
        WHERE franchise_id = %s AND channel = 'sms'
        ORDER BY created_at DESC
    """
    if limit is not None:
        sql += " LIMIT %s"
        params = [franchise_id, limit]
        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)
        return query_db(sql, tuple(params))
    else:
        return query_db(sql, (franchise_id,))


def get_templates():
    """Get message templates (stub for future implementation)."""
    return []


def get_template_by_id(template_id):
    """Get a template by ID (stub for future implementation)."""
    return None


def get_delivery_status(message_id):
    """Get delivery status for a message (stub for future implementation)."""
    return None


def get_conversation(conversation_id):
    """Get a conversation by ID (stub for future implementation)."""
    return None


def get_conversations(user, limit=None, offset=None):
    """Get conversations for a user (stub for future implementation)."""
    return []


def get_chat_history(user, limit=None, offset=None):
    """Get chat history for a user (stub for future implementation)."""
    return []


def get_messages_for_booking(booking_id):
    """Get messages for a booking (alias to communication logs)."""
    return get_communication_logs_by_booking_id(booking_id)


def get_messages_for_customer(franchise_id, phone, limit=None, offset=None):
    """Get messages for a customer (stub for future implementation)."""
    return []


def get_messages_for_vehicle(vin, franchise_id):
    """Get messages for a vehicle (stub for future implementation)."""
    return []


def get_failed_messages(franchise_id, limit=None, offset=None):
    """Get failed messages for a franchise."""
    sql = """
        SELECT *
        FROM communication_logs
        WHERE franchise_id = %s AND status LIKE 'failed%%'
        ORDER BY created_at DESC
    """
    if limit is not None:
        sql += " LIMIT %s"
        params = [franchise_id, limit]
        if offset is not None:
            sql += " OFFSET %s"
            params.append(offset)
        return query_db(sql, tuple(params))
    else:
        return query_db(sql, (franchise_id,))


def get_retry_queue(franchid_id, limit=None, offset=None):
    """Get retry queue (stub for future implementation)."""
    return []


def get_sent_reminders(user):
    """Get sent reminders for a user (stub for future implementation)."""
    return []


def get_pending_reminders(user):
    """Get pending reminders for a user."""
    reminders = get_reminders_for_user(user)
    return [r for r in reminders if r.get("status") == "Pending"]


def get_communication_dashboard(user):
    """Get communication dashboard data for a user."""
    # This can be expanded later; for now, return basic counts from available methods.
    # Note: user may be a dict; if not, default values.
    messages_today = get_communication_logs_count_by_date(
        user.get("utc_today", "") if hasattr(user, "get") and isinstance(user, dict) else ""
    ) if user else 0
    failed_messages = get_communication_logs_failed_count_by_franchise(
        user.get("franchise_id") if hasattr(user, "get") and isinstance(user, dict) else None
    ) if user else 0
    whatsapp_connected = get_messaging_accounts_whatsapp_connected()
    pending_reminders = len(get_pending_reminders(user)) if user else 0
    return {
        "messages_today": messages_today,
        "failed_messages": failed_messages,
        "whatsapp_connected": whatsapp_connected,
        "reminders_pending": pending_reminders,
    }


def get_chatbot_messages_daily(franchise_id, date):
    """Get chatbot usage daily for a franchise and date."""
    sql = """
        SELECT *
        FROM chatbot_usage_daily
        WHERE franchise_id = %s AND usage_date = %s
    """
    return query_db(sql, (franchise_id, date), one=True)


def get_chatbot_messages_monthly(franchise_id, year_month):
    """Get chatbot usage monthly for a franchise and year-month."""
    sql = """
        SELECT *
        FROM chatbot_usage_monthly
        WHERE franchise_id = %s AND usage_month = %s
    """
    return query_db(sql, (franchise_id, year_month), one=True)


def get_chatbot_message_by_id(usage_monthly_id):
    """Get a specific chatbot usage monthly record by id."""
    sql = """
        SELECT *
        FROM chatbot_usage_monthly
        WHERE id = %s
    """
    return query_db(sql, (usage_monthly_id,), one=True)


def get_chatbot_messages_inbox(user):
    """Get chatbot messages for inbox with appropriate filtering based on user's role."""
    clause, args = _scope_clause(user, alias="cm")
    sql = f"""
        SELECT
            cm.*,
            f.name AS franchise_name,
            b.name AS branch_name,
            u.username AS actor_name
        FROM chatbot_messages cm
        LEFT JOIN franchises f ON f.id = cm.franchise_id
        LEFT JOIN branches b ON br.id = cm.branch_id
        LEFT JOIN users u ON u.id = cm.user_id
        WHERE {clause}
        ORDER BY cm.created_at DESC
    """
    return query_db(sql, tuple(args))