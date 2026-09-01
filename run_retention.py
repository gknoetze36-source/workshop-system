"""PHANTA retention cron entrypoint."""

from jobs.scheduler import run_retention_jobs

run_retention_jobs()
