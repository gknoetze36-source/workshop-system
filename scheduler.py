import time
from datetime import datetime, timedelta

from database import fetch_all
from platform_messaging import send_twilio_message


# ---------------- DAY BEFORE REMINDERS ---------------- #

def send_day_before_reminders():
    tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")

    bookings = fetch_all("""
        SELECT phone, scheduled_date 
        FROM bookings 
        WHERE scheduled_date = %s
    """, (tomorrow,))

    for b in bookings:
        msg = f"Reminder: You have a booking tomorrow ({b['scheduled_date']})"
        send_twilio_message("whatsapp", b["phone"], msg)

    print(f"[{datetime.now()}] Day-before reminders sent")


# ---------------- DECLINED WORK ---------------- #

def send_declined_work_reminders():
    bookings = fetch_all("""
        SELECT phone 
        FROM bookings 
        WHERE quote_declined = 'Yes'
        AND DATE(scheduled_date) < DATE('now', '-30 day')
    """)

    for b in bookings:
        send_twilio_message(
            "whatsapp",
            b["phone"],
            "Reminder: You still have work pending. Would you like to book?"
        )

    print(f"[{datetime.now()}] Declined work reminders sent")


# ---------------- MAIN LOOP ---------------- #

def run_scheduler():
    print("Scheduler started...")

    while True:
        now = datetime.now()

        # Run every day at 08:00
        if now.hour == 8:
            send_day_before_reminders()

        # Run every day at 18:00
        if now.hour == 18:
            send_declined_work_reminders()

        time.sleep(3600)  # check every hour


if __name__ == "__main__":
    run_scheduler()
    