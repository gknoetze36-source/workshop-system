from datetime import datetime, timedelta
from platform_helpers import fetch_all
from platform_messaging import (
    auto_send_reminder,
    fetch_reminders_for_user,
    generate_due_reminders,
    send_cheapest_message,
    send_inquiry_followups,
    send_missed_booking_followups,
)
import sys


# ---------------- DAY BEFORE ---------------- #

def send_day_before_reminders():
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    bookings = fetch_all("""
        SELECT id, franchise_id, branch_id, first_name, customer_email, phone, scheduled_date 
        FROM bookings 
        WHERE scheduled_date = %s
    """, (tomorrow,))

    for b in bookings:
        send_cheapest_message(b, "Booking reminder", f"Reminder: You have a booking tomorrow ({b['scheduled_date']}).")

    print("Day-before reminders sent")


# ---------------- DECLINED WORK ---------------- #

def send_declined_work_reminders():
    bookings = fetch_all("""
        SELECT id, franchise_id, branch_id, first_name, customer_email, phone 
        FROM bookings 
        WHERE quote_declined = 'Yes'
    """)

    for b in bookings:
        send_cheapest_message(b, "Pending work reminder", "Reminder: You still have pending work. Book this month?")

    print("Declined reminders sent")


# ---------------- YEARLY ---------------- #

def yearly_reminders():
    created = generate_due_reminders(force=True)
    sent = 0
    for reminder in fetch_reminders_for_user({"role": "super_admin"}):
        if reminder.get("status") == "Pending":
            success, _ = auto_send_reminder(reminder)
            if success:
                sent += 1
    print(f"Running yearly reminders... created={created} sent={sent}")


def send_missed_booking_jobs():
    total = send_missed_booking_followups()
    print(f"Missed-booking follow-ups sent: {total}")


def send_inquiry_followup_jobs():
    total = send_inquiry_followups()
    print(f"Inquiry follow-ups sent: {total}")


# ---------------- ENTRY ---------------- #

if __name__ == "__main__":
    job = sys.argv[1] if len(sys.argv) > 1 else None

    if job == "daily":
        send_day_before_reminders()

    elif job == "monthly":
        send_declined_work_reminders()

    elif job == "yearly":
        yearly_reminders()

    elif job == "missed":
        send_missed_booking_jobs()

    elif job == "inquiry":
        send_inquiry_followup_jobs()

    else:
        print("No valid job provided")
