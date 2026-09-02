"""PHANTA billing cron entrypoint."""

from jobs.scheduler import run_billing_jobs

run_billing_jobs()
