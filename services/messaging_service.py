import re
from urllib.parse import quote
from datetime import datetime, timedelta

from database import fetch_one
from helpers.common import boolish
from services.financial_service import can_send_messages
from services.messaging_provider import active_messaging_account, send_provider_message
from services.communication_service import log_communication
from services.usage_service import track_message_usage
from validators.phone_validator import normalize_phone

def manual_channel_link(channel, recipient, subject, body):
    if channel == "sms":
        return f"sms:{recipient}?body={quote(body)}"
    return f"https://wa.me/{normalize_phone(recipient)}?text={quote(body)}"


def preferred_channels(booking):
    if not booking.get("phone"):
        return []
    method = (booking.get("preferred_contact_method") or "").lower()
    return ["sms", "whatsapp"] if "sms" in method or "text" in method else ["whatsapp", "sms"]


def lowest_cost_channels(booking):
    channels = []
    if booking.get("phone"):
        channels.append("whatsapp")
        channels.append("sms")
    seen = []
    for item in channels:
        if item not in seen:
            seen.append(item)
    return seen


def send_cheapest_message(booking, subject, body, actor_user_id=None, reminder=None):
    if not can_send_outbound(booking, subject, body):
        return False, "suppressed"
    recipient_phone = booking.get("phone")
    if recipient_phone and boolish(booking.get("whatsapp_opt_in", 0)):
        try:
            account = active_messaging_account(booking)
            if account:
                send_provider_message(recipient_phone, body, booking, account=account)
                log_communication(booking, reminder, "whatsapp", recipient_phone, subject, body, f"sent:{account.get('provider')}", actor_user_id)
                track_message_usage(booking.get("location_id"))
                return True, "whatsapp"
        except Exception as exc:
            log_communication(booking, reminder, "whatsapp", recipient_phone, subject, body, f"failed: {exc}", actor_user_id)
    return False, "manual"



def can_send_outbound(booking, subject, body):
    if not booking:
        return False
    location = fetch_one("SELECT * FROM locations WHERE id=%s", (booking.get("location_id"),))
    if not can_send_messages(location):
        return False
    if not boolish(booking.get("reminder_opt_in", 1)) and "reminder" in (subject or "").lower():
        return False
    recipient = booking.get("phone") or ""
    if not recipient:
        return False
    threshold = (datetime.utcnow() - timedelta(hours=12)).replace(microsecond=0).isoformat()
    recent = fetch_one(
        """
        SELECT id
        FROM communication_logs
        WHERE recipient=%s
          AND subject=%s
          AND created_at >= %s
        ORDER BY created_at DESC
        LIMIT 1
        """,
        (recipient, subject, threshold),
    )
    return recent is None


