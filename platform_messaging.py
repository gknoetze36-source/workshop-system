import re
from datetime import datetime, timedelta
from urllib.parse import quote

from database import execute_db, utc_now
from platform_helpers import (
    INQUIRY_STATES,
    boolish,
    fetch_all,
    fetch_one,
    find_active_inquiry,
    human_date,
    month_end,
    parse_date,
    public_booking_url,
    role_label,
    scope_clause,
    utc_today,
)


def normalize_phone(phone):
    digits = re.sub(r"\D", "", str(phone or ""))
    if digits.startswith("0") and len(digits) == 10:
        return f"27{digits[1:]}"
    if digits.startswith("27"):
        return digits
    if len(digits) == 9:
        return f"27{digits}"
    return digits


FOLLOWUP_DELAYS_MINUTES = {
    1: 7,
    2: 90,
    4: 60 * 24 * 2,
}


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


def manual_channel_link(channel, recipient, subject, body):
    if channel == "sms":
        return f"sms:{recipient}?body={quote(body)}"
    return f"https://wa.me/{normalize_phone(recipient)}?text={quote(body)}"


def twilio_configured(channel):
    import os

    if not all(os.environ.get(key) for key in ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN")):
        return False
    if channel == "sms":
        return bool(os.environ.get("TWILIO_SMS_FROM"))
    if channel == "whatsapp":
        return bool(os.environ.get("TWILIO_WHATSAPP_FROM"))
    return False


def send_twilio_message(channel, recipient, body):
    import os
    import requests

    account_sid = os.environ.get("TWILIO_ACCOUNT_SID")
    auth_token = os.environ.get("TWILIO_AUTH_TOKEN")
    from_number = os.environ.get("TWILIO_SMS_FROM" if channel == "sms" else "TWILIO_WHATSAPP_FROM")
    target = normalize_phone(recipient)
    if channel == "whatsapp":
        from_number = from_number if str(from_number).startswith("whatsapp:") else f"whatsapp:{from_number}"
        target = f"whatsapp:+{target}" if not str(target).startswith("whatsapp:") else target
    else:
        target = f"+{target}" if not str(target).startswith("+") else target

    response = requests.post(
        f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json",
        auth=(account_sid, auth_token),
        data={"From": from_number, "To": target, "Body": body},
        timeout=20,
    )
    response.raise_for_status()
    return response.json()


def build_booking_message(booking, reminder=None):
    service_label = (booking.get("service_level") or "General").lower()
    due_date = human_date(reminder.get("due_date") if reminder else booking.get("service_due_date"))
    branch_name = booking.get("branch_name") or booking.get("branch") or "your workshop"
    vehicle = " ".join(part for part in [booking.get("make"), booking.get("model")] if part).strip() or "your vehicle"
    branch_phone = booking.get("branch_contact_phone") or "the branch"
    booking_link = public_booking_url(
        {
            "franchise_slug": booking.get("franchise_slug"),
            "slug": booking.get("branch_slug"),
        }
    )

    lines = [
        f"Hello {booking.get('first_name', '').strip() or 'Customer'},",
        f"This is a reminder from {branch_name}.",
        f"Your annual {service_label} service for {vehicle} is due around {due_date}.",
    ]
    if booking.get("work_to_be_done"):
        lines.append(f"Workshop notes: {booking['work_to_be_done']}")
    lines.append(f"Book your next visit here: {booking_link}")
    lines.append(f"Need help? Contact {branch_phone}.")
    body = "\n".join(lines)
    subject = f"{branch_name}: {service_label.title()} service reminder"
    return subject, body


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


def _inquiry_stage_time(stage, reference_time):
    base = reference_time if isinstance(reference_time, datetime) else parse_date(reference_time) or datetime.utcnow()
    if stage == 1:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[1])).replace(microsecond=0)
    if stage == 2:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[2])).replace(microsecond=0)
    if stage == 3:
        next_day = (base + timedelta(days=1)).replace(hour=8, minute=30, second=0, microsecond=0)
        return next_day
    if stage == 4:
        return (base + timedelta(minutes=FOLLOWUP_DELAYS_MINUTES[4])).replace(hour=9, minute=30, second=0, microsecond=0)
    return None


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


def _available_slot_summary(branch_id, days=(0, 1)):
    branch = fetch_one("SELECT daily_capacity, name FROM branches WHERE id=%s", (branch_id,))
    if not branch:
        return ""
    capacity = int(branch.get("daily_capacity") or 0)
    labels = []
    for offset in days:
        day = datetime.utcnow().date() + timedelta(days=offset)
        date_key = day.strftime("%Y-%m-%d")
        booked = fetch_one("SELECT COUNT(*) AS total FROM bookings WHERE branch_id=%s AND scheduled_date=%s", (branch_id, date_key))
        total = int((booked or {}).get("total") or 0)
        remaining = max(capacity - total, 0)
        if remaining > 0:
            label = "today" if offset == 0 else ("tomorrow" if offset == 1 else day.strftime("%a"))
            if remaining >= 3:
                labels.append(f"{label} morning or afternoon")
            else:
                labels.append(f"{label} limited availability")
    return ", ".join(labels[:2])


def _followup_message(inquiry, branch, stage):
    service_type = (inquiry.get("service_type") or "your service").strip()
    branch_name = branch.get("name") or "the workshop"
    slot_summary = _available_slot_summary(branch["id"])
    slot_text = f" Available times: {slot_summary}." if slot_summary else ""
    if stage == 1:
        return (
            f"{branch_name}: just checking in about {service_type}. "
            f"Would you like me to book you in for today or tomorrow?{slot_text}"
        )
    if stage == 2:
        return (
            f"{branch_name}: we still have a few open spots for {service_type}. "
            f"I can quickly secure one for you. What time works best?{slot_text}"
        )
    if stage == 3:
        return (
            f"{branch_name}: just following up on {service_type}. "
            f"Did you still want to come in? I can book you for today or later this week.{slot_text}"
        )
    return (
        f"{branch_name}: one last check-in about {service_type}. "
        f"Let me know if you'd like me to book something for you."
    )


def _followup_subject(inquiry, branch, stage):
    service_type = (inquiry.get("service_type") or "booking").strip()
    return f"{branch.get('name')}: inquiry follow-up {stage} for {service_type}"


def ensure_inquiry(branch, phone="", email="", customer_name="", channel="WhatsApp", message="", service_type="", interested=False):
    phone = (phone or "").strip()
    email = (email or "").strip()
    if not phone and not email:
        return None
    inquiry = find_active_inquiry(branch["franchise_id"], branch["id"], phone=phone, email=email)
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
            WHERE id=%s
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
            ),
        )
        return find_active_inquiry(branch["franchise_id"], branch["id"], phone=phone, email=email)

    execute_db(
        """
        INSERT INTO booking_inquiries (
            franchise_id, branch_id, customer_name, customer_phone, customer_email,
            source_channel, user_state, service_type, last_message_text, last_user_interaction_at,
            next_followup_at, declined, created_at, updated_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            branch["franchise_id"],
            branch["id"],
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
    return find_active_inquiry(branch["franchise_id"], branch["id"], phone=phone, email=email)


def stop_inquiry_for_reply(branch, phone="", email="", message="", customer_name="", channel="WhatsApp"):
    inquiry = ensure_inquiry(branch, phone=phone, email=email, customer_name=customer_name, channel=channel, message=message, interested=True)
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
        WHERE id=%s
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
        ),
    )
    return find_active_inquiry(branch["franchise_id"], branch["id"], phone=phone, email=email)


def preferred_channels(booking):
    method = (booking.get("preferred_contact_method") or "").lower()
    available = []
    if booking.get("phone"):
        available.extend(["whatsapp", "sms"])

    if "sms" in method or "message" in method or "text" in method:
        ordered = ["sms", "whatsapp"]
    else:
        ordered = ["whatsapp", "sms"]

    return [channel for channel in ordered if channel in available]


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


def log_communication(booking, reminder, channel, recipient, subject, body, status, user_id=None, external_target=""):
    execute_db(
        """
        INSERT INTO communication_logs (
            booking_id, reminder_id, franchise_id, branch_id, user_id, channel,
            recipient, subject, body, status, external_target, created_at, sent_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (
            booking["id"],
            reminder["id"] if reminder else None,
            booking["franchise_id"],
            booking["branch_id"],
            user_id,
            channel,
            recipient,
            subject,
            body,
            status,
            external_target,
            utc_now(),
            utc_now() if status == "sent" else None,
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
        WHERE rc.id=%s
        """,
        (reminder_id,),
    )


def reminder_in_scope(reminder, user):
    if not reminder:
        return False
    if user["role"] == "super_admin":
        return True
    if user["role"] == "franchise_admin":
        return reminder.get("franchise_id") == user.get("franchise_id")
    return reminder.get("branch_id") == user.get("branch_id")


def generate_due_reminders(user_scope=None, as_of=None, force=False):
    as_of = as_of or datetime.utcnow()
    clause = "1=1"
    args = []
    if user_scope:
        clause, args = scope_clause(user_scope)

    bookings = fetch_all(
        f"""
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE {clause}
          AND b.status IN ('Done', 'Collected')
          AND COALESCE(b.reminder_opt_in, 1) = 1
          AND (
              (b.service_level IN ('Major', 'Minor') AND COALESCE(b.service_due_date, '') <> '')
              OR COALESCE(b.work_to_be_done, '') <> ''
          )
        ORDER BY b.service_due_date ASC
        """,
        tuple(args),
    )

    created = 0
    for booking in bookings:
        due_date = parse_date(booking.get("service_due_date"))
        if not due_date:
            continue

        reminder_types = []
        if booking.get("service_level") in {"Major", "Minor"} and due_date:
            reminder_types.append((f"{booking['service_level'].lower()}_service", [month_end(due_date), month_end(month_end(due_date) + timedelta(days=1))]))
        if booking.get("work_to_be_done"):
            work_due = due_date or parse_date(booking.get("scheduled_date")) or as_of
            reminder_types.append(("work_to_be_done", [month_end(work_due), month_end(month_end(work_due) + timedelta(days=1))]))

        for reminder_kind, campaign_dates in reminder_types:
            for round_number, campaign_date in enumerate(campaign_dates, start=1):
                if not campaign_date:
                    continue
                window_end = campaign_date + timedelta(days=31)
                if not force and not (campaign_date <= as_of <= window_end):
                    continue

                existing = fetch_one(
                    """
                    SELECT id
                    FROM reminder_campaigns
                    WHERE booking_id=%s AND reminder_kind=%s AND campaign_round=%s
                    """,
                    (booking["id"], reminder_kind, round_number),
                )
                if existing:
                    continue

                subject, body = build_booking_message(booking, {"due_date": booking.get("service_due_date") or booking.get("scheduled_date")})
                execute_db(
                    """
                    INSERT INTO reminder_campaigns (
                        booking_id, franchise_id, branch_id, reminder_kind, due_date,
                        campaign_round, scheduled_for, status, message_subject,
                        message_body, send_count, created_at, updated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, 'Pending', %s, %s, 0, %s, %s)
                    """,
                    (
                        booking["id"],
                        booking["franchise_id"],
                        booking["branch_id"],
                        reminder_kind,
                        booking.get("service_due_date") or booking.get("scheduled_date"),
                        round_number,
                        campaign_date.strftime("%Y-%m-%d"),
                        subject,
                        body,
                        utc_now(),
                        utc_now(),
                    ),
                )
                created += 1

    return created


def auto_send_reminder(reminder, actor_user=None):
    booking = fetch_one(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.id=%s
        """,
        (reminder["booking_id"],),
    )
    if not booking:
        return False, "Booking not found."

    subject, body = build_booking_message(booking, reminder)
    for channel in lowest_cost_channels(booking):
        try:
            if channel in {"sms", "whatsapp"} and twilio_configured(channel) and booking.get("phone"):
                send_twilio_message(channel, booking["phone"], body)
                log_communication(booking, reminder, channel, booking["phone"], subject, body, "sent", actor_user["id"] if actor_user else None)
                update_reminder_status(reminder["id"], "Sent", channel, count_as_send=True)
                return True, f"{channel.title()} message sent."
        except Exception as exc:
            log_communication(
                booking,
                reminder,
                channel,
                booking.get("phone", ""),
                subject,
                body,
                f"failed: {exc}",
                actor_user["id"] if actor_user else None,
            )
            return False, str(exc)

    return False, "No direct provider is configured for this customer."


def send_cheapest_message(booking, subject, body, actor_user_id=None, reminder=None):
    if not can_send_outbound(booking, subject, body):
        return False, "suppressed"
    recipient_phone = booking.get("phone")
    for channel in ["whatsapp", "sms"]:
        try:
            if channel == "whatsapp" and recipient_phone and boolish(booking.get("whatsapp_opt_in", 0)) and twilio_configured("whatsapp"):
                send_twilio_message("whatsapp", recipient_phone, body)
                log_communication(booking, reminder, "whatsapp", recipient_phone, subject, body, "sent", actor_user_id)
                return True, "whatsapp"
            if channel == "sms" and recipient_phone and twilio_configured("sms"):
                send_twilio_message("sms", recipient_phone, body)
                log_communication(booking, reminder, "sms", recipient_phone, subject, body, "sent", actor_user_id)
                return True, "sms"
        except Exception as exc:
            log_communication(booking, reminder, channel, recipient_phone, subject, body, f"failed: {exc}", actor_user_id)
            continue
    return False, "manual"


def can_send_outbound(booking, subject, body):
    if not booking:
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


def send_missed_booking_followups():
    today = utc_today()
    bookings = fetch_all(
        """
        SELECT
            b.*,
            f.name AS franchise_name,
            f.slug AS franchise_slug,
            br.name AS branch_name,
            br.slug AS branch_slug,
            br.contact_email AS branch_contact_email,
            br.contact_phone AS branch_contact_phone
        FROM bookings b
        LEFT JOIN franchises f ON f.id = b.franchise_id
        LEFT JOIN branches br ON br.id = b.branch_id
        WHERE b.scheduled_date < %s
          AND b.status IN ('Pending', 'Confirmed', 'In Progress')
          AND COALESCE(b.phone, '') <> ''
          AND COALESCE(b.missed_followup_count, 0) < 2
        ORDER BY b.scheduled_date ASC
        """,
        (today,),
    )
    sent = 0
    for booking in bookings:
        recent_reply = fetch_one(
            """
            SELECT id
            FROM chatbot_messages
            WHERE franchise_id=%s
              AND customer_phone=%s
              AND direction='inbound'
              AND created_at > COALESCE(%s, '1900-01-01T00:00:00')
            ORDER BY created_at DESC
            LIMIT 1
            """,
            (booking["franchise_id"], booking["phone"], booking.get("last_missed_followup_at")),
        )
        if recent_reply:
            execute_db("UPDATE bookings SET last_customer_reply_at=%s, updated_at=%s WHERE id=%s", (utc_now(), utc_now(), booking["id"]))
            continue
        subject = f"{booking.get('branch_name')}: missed booking follow-up"
        body = (
            f"Hello {booking.get('first_name') or 'Customer'}, we missed you for your booking on "
            f"{human_date(booking.get('scheduled_date'))}. Reply here if you would like us to reschedule."
        )
        success, channel = send_cheapest_message(booking, subject, body)
        if success:
            execute_db(
                "UPDATE bookings SET missed_followup_count=%s, last_missed_followup_at=%s, updated_at=%s WHERE id=%s",
                (int(booking.get("missed_followup_count") or 0) + 1, utc_now(), utc_now(), booking["id"]),
            )
            sent += 1
    return sent


def send_inquiry_followups(as_of=None):
    now = as_of or datetime.utcnow()
    now_iso = _iso_now(now)
    inquiries = fetch_all(
        """
        SELECT
            bi.*,
            br.name AS branch_name,
            br.slug AS branch_slug,
            f.name AS franchise_name,
            f.slug AS franchise_slug
        FROM booking_inquiries bi
        LEFT JOIN branches br ON br.id = bi.branch_id
        LEFT JOIN franchises f ON f.id = bi.franchise_id
        WHERE bi.booking_id IS NULL
          AND bi.user_state IN ('NEW_INQUIRY', 'ENGAGED', 'BOOKING_PENDING')
          AND COALESCE(bi.declined, 0) = 0
          AND bi.next_followup_at IS NOT NULL
          AND bi.next_followup_at <= %s
        ORDER BY bi.next_followup_at ASC
        """,
        (now_iso,),
    )
    sent = 0
    for inquiry in inquiries:
        stage = int(inquiry.get("followup_stage") or 0) + 1
        if stage > 4:
            execute_db(
                "UPDATE booking_inquiries SET user_state='LOST', stop_reason='sequence_completed', closed_at=%s, next_followup_at=NULL, updated_at=%s WHERE id=%s",
                (now_iso, now_iso, inquiry["id"]),
            )
            continue
        existing_event = fetch_one(
            "SELECT id FROM inquiry_followup_events WHERE inquiry_id=%s AND followup_stage=%s",
            (inquiry["id"], stage),
        )
        if existing_event:
            continue
        last_interaction = _parse_timestamp(inquiry.get("last_user_interaction_at"))
        last_followup = _parse_timestamp(inquiry.get("last_followup_at")) or datetime(1900, 1, 1)
        if int(inquiry.get("followups_sent_count") or 0) > 0 and last_interaction and last_interaction > last_followup:
            continue
        branch = {
            "id": inquiry["branch_id"],
            "name": inquiry.get("branch_name"),
            "slug": inquiry.get("branch_slug"),
            "franchise_id": inquiry["franchise_id"],
            "franchise_slug": inquiry.get("franchise_slug"),
        }
        booking_stub = {
            "id": None,
            "franchise_id": inquiry["franchise_id"],
            "branch_id": inquiry["branch_id"],
            "phone": inquiry.get("customer_phone"),
            "customer_email": inquiry.get("customer_email"),
            "whatsapp_opt_in": 1,
            "reminder_opt_in": 1,
        }
        subject = _followup_subject(inquiry, branch, stage)
        body = _followup_message(inquiry, branch, stage)
        success, channel = send_cheapest_message(booking_stub, subject, body)
        status = "sent" if success else f"failed:{channel}"
        execute_db(
            """
            INSERT INTO inquiry_followup_events (
                inquiry_id, followup_stage, channel, message_subject, message_body,
                status, sent_at, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (inquiry["id"], stage, channel, subject, body, status, now_iso if success else None, now_iso),
        )
        if not success:
            execute_db(
                "UPDATE booking_inquiries SET updated_at=%s WHERE id=%s",
                (now_iso, inquiry["id"]),
            )
            continue
        next_stage_time = _inquiry_stage_time(stage + 1, now)
        execute_db(
            """
            UPDATE booking_inquiries
            SET followup_stage=%s,
                last_followup_at=%s,
                next_followup_at=%s,
                followups_sent_count=COALESCE(followups_sent_count, 0) + 1,
                updated_at=%s
            WHERE id=%s
            """,
            (
                stage,
                now_iso,
                next_stage_time.isoformat() if next_stage_time and stage < 4 else None,
                now_iso,
                inquiry["id"],
            ),
        )
        if stage >= 4:
            execute_db(
                "UPDATE booking_inquiries SET user_state='LOST', stop_reason='sequence_completed', closed_at=%s, updated_at=%s WHERE id=%s",
                (now_iso, now_iso, inquiry["id"]),
            )
        sent += 1
    return sent
