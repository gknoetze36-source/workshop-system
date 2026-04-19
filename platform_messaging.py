import re
import smtplib
from datetime import datetime, timedelta
from email.message import EmailMessage
from urllib.parse import quote

from database import execute_db, utc_now
from platform_helpers import (
    boolish,
    fetch_all,
    fetch_one,
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


def manual_channel_link(channel, recipient, subject, body):
    if channel == "email":
        return f"mailto:{recipient}?subject={quote(subject)}&body={quote(body)}"
    if channel == "sms":
        return f"sms:{recipient}?body={quote(body)}"
    return f"https://wa.me/{normalize_phone(recipient)}?text={quote(body)}"


def smtp_configured():
    return all(
        __import__("os").environ.get(key)
        for key in ("SMTP_HOST", "SMTP_PORT", "SMTP_FROM_EMAIL")
    )


def twilio_configured(channel):
    import os

    if not all(os.environ.get(key) for key in ("TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN")):
        return False
    if channel == "sms":
        return bool(os.environ.get("TWILIO_SMS_FROM"))
    if channel == "whatsapp":
        return bool(os.environ.get("TWILIO_WHATSAPP_FROM"))
    return False


def send_email_message(recipient, subject, body):
    import os

    message = EmailMessage()
    message["Subject"] = subject
    message["From"] = os.environ.get("SMTP_FROM_EMAIL")
    message["To"] = recipient
    message.set_content(body)

    with smtplib.SMTP(os.environ.get("SMTP_HOST"), int(os.environ.get("SMTP_PORT", "587")), timeout=20) as smtp:
        if os.environ.get("SMTP_USE_TLS", "true").lower() != "false":
            smtp.starttls()
        if os.environ.get("SMTP_USERNAME") and os.environ.get("SMTP_PASSWORD"):
            smtp.login(os.environ.get("SMTP_USERNAME"), os.environ.get("SMTP_PASSWORD"))
        smtp.send_message(message)


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


def preferred_channels(booking):
    method = (booking.get("preferred_contact_method") or "").lower()
    available = []
    if booking.get("phone"):
        available.extend(["whatsapp", "sms"])
    if booking.get("customer_email"):
        available.append("email")

    if "email" in method:
        ordered = ["email", "whatsapp", "sms"]
    elif "sms" in method or "message" in method or "text" in method:
        ordered = ["sms", "whatsapp", "email"]
    else:
        ordered = ["whatsapp", "sms", "email"]

    return [channel for channel in ordered if channel in available]


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
          AND b.service_level IN ('Major', 'Minor')
          AND COALESCE(b.service_due_date, '') <> ''
        ORDER BY b.service_due_date ASC
        """,
        tuple(args),
    )

    created = 0
    for booking in bookings:
        due_date = parse_date(booking.get("service_due_date"))
        if not due_date:
            continue

        campaign_dates = [month_end(due_date), month_end(month_end(due_date) + timedelta(days=1))]
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
                (booking["id"], f"{booking['service_level'].lower()}_service", round_number),
            )
            if existing:
                continue

            subject, body = build_booking_message(booking, {"due_date": booking["service_due_date"]})
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
                    f"{booking['service_level'].lower()}_service",
                    booking["service_due_date"],
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
    for channel in preferred_channels(booking):
        try:
            if channel == "email" and smtp_configured() and booking.get("customer_email"):
                send_email_message(booking["customer_email"], subject, body)
                log_communication(booking, reminder, channel, booking["customer_email"], subject, body, "sent", actor_user["id"] if actor_user else None)
                update_reminder_status(reminder["id"], "Sent", channel, count_as_send=True)
                return True, f"Email sent to {booking['customer_email']}."

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
                booking.get("customer_email") if channel == "email" else booking.get("phone", ""),
                subject,
                body,
                f"failed: {exc}",
                actor_user["id"] if actor_user else None,
            )
            return False, str(exc)

    return False, "No direct provider is configured for this customer."
