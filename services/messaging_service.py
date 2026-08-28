import re
from urllib.parse import quote
import logging
from constants.message_categories import (
    normalise_category, is_marketing, is_reminder, UNCATEGORISED,
)
from services.consent_service import may_send_marketing
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


def send_cheapest_message(booking, subject, body, actor_user_id=None, reminder=None, category=None):
    """Send one message on the cheapest available channel.

    `category` must be a value from constants/message_categories.py. It is
    what allows marketing suppression to block promotional sends without
    blocking operational ones, so every caller should declare it.
    """
    if not can_send_outbound(booking, subject, body, category=category):
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



logger = logging.getLogger(__name__)


def can_send_outbound(booking, subject, body, category=None):
    """Decide whether one outbound message may be sent.

    This is the single chokepoint every send path passes through, including
    the cron-driven reminder jobs, so suppression applied here is inherited by
    background work automatically.

    ORDER OF CHECKS
    ---------------
    1. Location may send at all (billing/access state).
    2. MARKETING requires affirmative customer-level consent. Operational
       messages about a booking the customer actually made are never blocked
       by a marketing opt-out -- that is the whole point of categorising.
    3. Reminders honour the pre-existing per-booking reminder opt-out.
    4. Duplicate suppression: same recipient and subject within 12 hours.

    `category` is optional so existing call sites keep working; an unlabelled
    message is treated as operational and logged, so remaining call sites can
    be found and labelled rather than silently dropped.
    """
    if not booking:
        return False
    location = fetch_one("SELECT * FROM locations WHERE id=%s", (booking.get("location_id"),))
    if not can_send_messages(location):
        return False

    resolved = normalise_category(category)
    if resolved == UNCATEGORISED and category is None:
        logger.info(
            "outbound_message_uncategorised subject=%r booking_id=%s",
            (subject or "")[:40], booking.get("id"),
        )

    # Marketing requires an affirmative opt-in. Absence of an opt-out is not
    # consent, so an unknown state suppresses.
    if is_marketing(resolved):
        customer_id = booking.get("customer_id")
        location_id = booking.get("location_id")
        if not customer_id or not may_send_marketing(customer_id, location_id):
            logger.info(
                "marketing_suppressed customer_id=%s location_id=%s category=%s",
                customer_id, location_id, resolved,
            )
            return False

    # Pre-existing per-booking reminder opt-out. Retained, but now keyed on the
    # declared category rather than a substring match on the subject line.
    if not boolish(booking.get("reminder_opt_in", 1)):
        if is_reminder(resolved) or (category is None and "reminder" in (subject or "").lower()):
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


