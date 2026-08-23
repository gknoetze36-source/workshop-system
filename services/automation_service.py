"""
Universal Automation Engine service for PHANTA.

This service contains business logic for automation workflows.

Dependencies:
- Automation Repository
- Booking Repository
"""

from repositories.automation_repository import (
    get_automation_rules_by_location_and_event as _get_automation_rules_by_location_and_event,
    get_pending_scheduled_jobs as _get_pending_scheduled_jobs,
    get_failed_job_by_id as _get_failed_job_by_id,
    get_scheduled_job_by_id_and_status_running_and_locked_at as _get_scheduled_job_by_id_and_status_running_and_locked_at,
    get_scheduled_job_by_id_and_status_pending as _get_scheduled_job_by_id_and_status_pending,
    get_scheduled_job_by_id as _get_scheduled_job_by_id,
    get_failed_jobs_count as _get_failed_jobs_count,
    get_location_by_id as _get_location_by_id,
)
from repositories.booking_repository import get_booking_by_reference_raw as _get_booking_by_reference_raw


def get_automation_rules_by_location_and_event(location_id, event_type):
    """Get active automation rules for a location and event type, with template details."""
    return _get_automation_rules_by_location_and_event(location_id, event_type)


def get_pending_scheduled_jobs(limit=50):
    """Get pending scheduled jobs due now or earlier, with event type."""
    return _get_pending_scheduled_jobs(limit)


def get_failed_job_by_id(failed_id):
    """Get a failed job by ID that is not yet resolved."""
    return _get_failed_job_by_id(failed_id)


def get_scheduled_job_by_id_and_status_running_and_locked_at(job_id, locked_at):
    """Get a scheduled job by ID that is running with a specific locked_at."""
    return _get_scheduled_job_by_id_and_status_running_and_locked_at(job_id, locked_at)


def get_scheduled_job_by_id_and_status_pending(job_id):
    """Get a scheduled job by ID that is pending."""
    return _get_scheduled_job_by_id_and_status_pending(job_id)


def get_scheduled_job_by_id(job_id):
    """Get a scheduled job by ID (any status)."""
    return _get_scheduled_job_by_id(job_id)


def get_failed_jobs_count():
    """Get count of failed jobs (not resolved)."""
    return _get_failed_jobs_count()


def get_location_by_id(location_id):
    """Get a location by ID for automation eligibility checks."""
    return _get_location_by_id(location_id)


def get_booking_by_reference(booking_reference):
    """Get a booking by reference without user scope for background jobs."""
    return _get_booking_by_reference_raw(booking_reference)