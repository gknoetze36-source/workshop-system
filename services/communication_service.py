"""
Communication Service for Workshop System Version 2.

This service contains all business logic for the Communication entity.
It depends only delegates exactly once to the Communication Repository.
"""

from repositories.communication_repository import (
    get_communication_logs_count_by_date as _get_communication_logs_count_by_date,
    get_communication_logs_count_by_franchise as _get_communication_logs_count_by_franchise,
    get_communication_logs_failed_count_by_franchise as _get_communication_logs_failed_count_by_franchise,
    get_communication_logs_by_booking_id as _get_communication_logs_by_booking_id,
    get_messaging_accounts_whatsapp_connected as _get_messaging_accounts_whatsapp_connected,
    get_reminders_for_user as _get_reminders_for_user,
    get_reminder_by_id as _get_reminder_by_id,
    get_whatsapp_messages as _get_whatsapp_messages,
    get_email_messages as _get_email_messages,
    get_sms_messages as _get_sms_messages,
    get_templates as _get_templates,
    get_template_by_id as _get_template_by_id,
    get_delivery_status as _get_delivery_status,
    get_conversation as _get_conversation,
    get_conversations as _get_conversations,
    get_chat_history as _get_chat_history,
    get_messages_for_booking as _get_messages_for_booking,
    get_messages_for_customer as _get_messages_for_customer,
    get_messages_for_vehicle as _get_messages_for_vehicle,
    get_failed_messages as _get_failed_messages,
    get_retry_queue as _get_retry_queue,
    get_sent_reminders as _get_sent_reminders,
    get_pending_reminders as _get_pending_reminders,
    get_communication_dashboard as _get_communication_dashboard,
    get_chatbot_messages_daily as _get_chatbot_messages_daily,
    get_chatbot_messages_monthly as _get_chatbot_messages_monthly,
    get_chatbot_message_by_id as _get_chatbot_message_by_id,
    get_chatbot_messages_inbox as _get_chatbot_messages_inbox,
)


def get_communication_logs_count_by_date(date):
    """Get count of communication logs for a specific date (YYYY-MM-DD)."""
    return _get_communication_logs_count_by_date(date)


def get_communication_logs_count_by_franchise(franchise_id):
    """Get total count of communication logs for a franchise."""
    return _get_communication_logs_count_by_franchise(franchise_id)


def get_communication_logs_failed_count_by_franchise(franchise_id):
    """Get count of failed communication logs for a franchise."""
    return _get_communication_logs_failed_count_by_franchise(franchise_id)


def get_communication_logs_by_booking_id(booking_id):
    """Get all communication logs for a specific booking."""
    return _get_communication_logs_by_booking_id(booking_id)


def get_messaging_accounts_whatsapp_connected():
    """Get count of active WhatsApp messaging accounts (provider=meta)."""
    return _get_messaging_accounts_whatsapp_connected()


def get_reminders_for_user(user):
    """Get reminders for a user."""
    return _get_reminders_for_user(user)


def get_reminder_by_id(reminder_id):
    """Get a single reminder by ID."""
    return _get_reminder_by_id(reminder_id)


def get_whatsapp_messages(franchise_id, limit=None, offset=None):
    """Get WhatsApp messages for a franchise."""
    return _get_whatsapp_messages(franchise_id, limit, offset)


def get_email_messages(franchise_id, limit=None, offset=None):
    """Get email messages for a franchise."""
    return _get_email_messages(franchise_id, limit, offset)


def get_sms_messages(franchise_id, limit=None, offset=None):
    """Get SMS messages for a franchise."""
    return _get_sms_messages(franchise_id, limit, offset)


def get_templates():
    """Get message templates."""
    return _get_templates()


def get_template_by_id(template_id):
    """Get a template by ID."""
    return _get_template_by_id(template_id)


def get_delivery_status(message_id):
    """Get delivery status for a message."""
    return _get_delivery_status(message_id)


def get_conversation(conversation_id):
    """Get a conversation by ID."""
    return _get_conversation(conversation_id)


def get_conversations(user, limit=None, offset=None):
    """Get conversations for a user."""
    return _get_conversations(user, limit, offset)


def get_chat_history(user, limit=None, offset=None):
    """Get chat history for a user."""
    return _get_chat_history(user, limit, offset)


def get_messages_for_booking(booking_id):
    """Get messages for a booking."""
    return _get_messages_for_booking(booking_id)


def get_messages_for_customer(franchise_id, phone, limit=None, offset=None):
    """Get messages for a customer."""
    return _get_messages_for_customer(franchise_id, phone, limit, offset)


def get_messages_for_vehicle(vin, franchise_id):
    """Get messages for a vehicle."""
    return _get_messages_for_vehicle(vin, franchise_id)


def get_failed_messages(franchise_id, limit=None, offset=None):
    """Get failed messages for a franchise."""
    return _get_failed_messages(franchise_id, limit, offset)


def get_retry_queue(franchise_id, limit=None, offset=None):
    """Get retry queue."""
    return _get_retry_queue(franchise_id, limit, offset)


def get_sent_reminders(user):
    """Get sent reminders for a user."""
    return _get_sent_reminders(user)


def get_pending_reminders(user):
    """Get pending reminders for a user."""
    return _get_pending_reminders(user)


def get_communication_dashboard(user):
    """Get communication dashboard data for a user."""
    return _get_communication_dashboard(user)


def get_chatbot_messages_daily(franchise_id, date):
    """Get chatbot usage daily for a franchise and date."""
    return _get_chatbot_messages_daily(franchise_id, date)


def get_chatbot_messages_monthly(franchise_id, year_month):
    """Get chatbot usage monthly for a franchise and year-month."""
    return _get_chatbot_messages_monthly(franchise_id, year_month)


def get_chatbot_message_by_id(usage_monthly_id):
    """Get a specific chatbot usage monthly record by id."""
    return _get_chatbot_message_by_id(usage_monthly_id)


def get_chatbot_messages_inbox(user):
    """Get chatbot messages for inbox with appropriate filtering."""
    return _get_chatbot_messages_inbox(user)