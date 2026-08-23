from datetime import datetime, timedelta

from database import execute_db, utc_now
from helpers.common import fetch_all, fetch_one
from helpers.dates import parse_date, sast_now, sast_today
from repositories.inquiry_repository import (
    find_active_inquiry as _find_active_inquiry,
    fetch_inquiries_for_user as _fetch_inquiries_for_user,
    inquiry_metrics as _inquiry_metrics,
)

FOLLOWUP_DELAYS_MINUTES = {1: 7, 2: 90, 4: 60 * 24 * 2}


def _inquiry_stage_time(stage, reference_time):
    base = reference_time if isinstance(reference_time, datetime) else parse_date(reference_time) or datetime.utcnow()
    if stage == 1:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[1])).replace(microsecond=0)
    if stage == 2:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[2])).replace(microsecond=0)
    if stage == 3:
        return (base + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
    if stage == 4:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[4])).replace(hour=9, minute=30, second=0, microsecond=0)
    return None


def find_active_inquiry(location_id, phone="", email=""):
    return _find_active_inquiry(location_id, phone=phone, email=email)


def fetch_inquiries_for_user(user, limit=30):
    return _fetch_inquiries_for_user(user, limit=limit)


def inquiry_metrics(user):
    return _inquiry_metrics(user)




DECLINE_PATTERNS = (
    "no",
    "not now",
    "stop",
    "cancel",
    "don't",
    "do not",
    "no thanks",
    "not interested",
    "leave me",
)


def _iso_now(as_of=None):
    moment = as_of or datetime.utcnow()
    return moment.replace(microsecond=0).isoformat()


def _parse_timestamp(value):
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", ""))
    except ValueError:
        return parse_date(text)



def _decline_detected(message):
    text = (message or "").strip().lower()
    return any(pattern in text for pattern in DECLINE_PATTERNS)


def _inquiry_state_for_message(message, service_type="", existing_state=""):
    text = (message or "").strip().lower()
    if _decline_detected(text):
        return "LOST"
    if any(keyword in text for keyword in ("book", "booking", "appointment", "come in", "available", "time", "tomorrow", "today")):
        return "BOOKING_PENDING"
    if service_type or any(keyword in text for keyword in ("price", "quote", "cost", "repair", "service", "help", "?")):
        return "ENGAGED"
    return existing_state or "NEW_INQUIRY"


def ensure_inquiry(location, phone="", email="", customer_name="", channel="WhatsApp", message="", service_type="", interested=False):
    phone = (phone or "").strip()
    email = (email or "").strip()
    if not phone and not email:
        return None
    inquiry = find_active_inquiry(location["location_id"], phone=phone, email=email)
    state = _inquiry_state_for_message(message, service_type=service_type, existing_state=(inquiry or {}).get("user_state"))
    now = utc_now()
    next_followup = _inquiry_stage_time(1, datetime.utcnow()).isoformat() if interested and state in {"ENGAGED", "BOOKING_PENDING", "NEW_INQUIRY"} else None
    if inquiry:
        execute_db(
            """
            UPDATE booking_inquiries
            SET customer_name=COALESCE(NULLIF(%s, ''), customer_name),
                customer_email=COALESCE(NULLIF(%s, ''), customer_email),
                source_channel=%s,
                user_state=%s,
                service_type=COALESCE(NULLIF(%s, ''), service_type),
                last_message_text=%s,
                last_user_interaction_at=%s,
                next_followup_at=CASE
                    WHEN booking_id IS NOT NULL OR %s='LOST' THEN NULL
                    WHEN COALESCE(followup_stage, 0)=0 AND %s THEN COALESCE(next_followup_at, %s)
                    ELSE next_followup_at
                END,
                declined=%s,
                stop_reason=CASE WHEN %s='LOST' THEN 'declined' ELSE NULL END,
                closed_at=CASE WHEN %s='LOST' THEN %s ELSE NULL END,
                updated_at=%s
            WHERE id=%s AND location_id=%s
            """,
            (
                customer_name,
                email,
                channel,
                state,
                service_type,
                message,
                now,
                state,
                1 if interested else 0,
                next_followup,
                1 if state == "LOST" else 0,
                state,
                state,
                now,
                now,
                inquiry["id"],
                location["location_id"],
            ),
        )
        return find_active_inquiry(location["location_id"], phone=phone, email=email)

    execute_db(
        """
        INSERT INTO booking_inquiries (
            location_id, customer_name, customer_phone, customer_email,
            source_channel, user_state, service_type, last_message_text, last_user_interaction_at,
            next_followup_at, declined, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            location["location_id"],
            customer_name,
            phone,
            email,
            channel,
            state,
            service_type,
            message,
            now,
            next_followup,
            1 if state == "LOST" else 0,
            now,
            now,
        ),
    )
    return find_active_inquiry(location["location_id"], phone=phone, email=email)


def stop_inquiry_for_reply(location, phone="", email="", message="", customer_name="", channel="WhatsApp"):
    inquiry = ensure_inquiry(location, phone=phone, email=email, customer_name=customer_name, channel=channel, message=message, interested=True)
    if not inquiry:
        return None
    now = utc_now()
    new_state = "LOST" if _decline_detected(message) else _inquiry_state_for_message(message, service_type=inquiry.get("service_type"), existing_state=inquiry.get("user_state"))
    prior_followups = int(inquiry.get("followups_sent_count") or 0)
    replies_after = int(inquiry.get("replies_after_followup_count") or 0) + (1 if prior_followups > 0 else 0)
    if new_state == "LOST":
        next_followup = None
    elif prior_followups > 0:
        next_followup = None
    else:
        next_followup = inquiry.get("next_followup_at") or _inquiry_stage_time(1, datetime.utcnow()).isoformat()
    execute_db(
        """
        UPDATE booking_inquiries
        SET user_state=%s,
            last_user_interaction_at=%s,
            last_message_text=%s,
            customer_name=COALESCE(NULLIF(%s, ''), customer_name),
            source_channel=%s,
            replies_after_followup_count=%s,
            stop_reason=%s,
            declined=%s,
            closed_at=%s,
            next_followup_at=%s,
            updated_at=%s
        WHERE id=%s AND location_id=%s
        """,
        (
            new_state,
            now,
            message,
            customer_name,
            channel,
            replies_after,
            "declined" if new_state == "LOST" else None,
            1 if new_state == "LOST" else 0,
            now if new_state == "LOST" else None,
            next_followup,
            now,
            inquiry["id"],
            location["location_id"],
        ),
    )
    return find_active_inquiry(location["location_id"], phone=phone, email=email)


