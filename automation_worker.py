import os
import time

from automation_engine import process_due_jobs
from database import initialize_database


def run_worker():
    initialize_database()
    interval = int(os.environ.get("AUTOMATION_WORKER_INTERVAL_SECONDS", "30"))
    limit = int(os.environ.get("AUTOMATION_WORKER_BATCH_SIZE", "50"))
    print("Automation worker started")
    while True:
        processed = process_due_jobs(limit=limit)
        if processed:
            print(f"Automation jobs processed: {processed}")
        time.sleep(interval)


if __name__ == "__main__":
    run_worker()
