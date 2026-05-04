import time
from datetime import datetime, timedelta

from cron_jobs import send_day_before_reminders, send_declined_work_reminders, send_missed_booking_jobs, yearly_reminders


# ---------------- DAY BEFORE REMINDERS ---------------- #

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
            send_missed_booking_jobs()

        # Run every day at 19:00
        if now.hour == 19:
            yearly_reminders()

        time.sleep(3600)  # check every hour


if __name__ == "__main__":
    run_scheduler()
    
