"""
Automation Repository for Workshop System Version 2.

This repository handles all database operations for the Automation entity.
It interacts with the existing tables: automation_rules, automation_templates,
scheduled_jobs, failed_jobs.
"""

from database import query_db


def get_automation_rules_by_franchise_and_event(franchise_id, event_type):
    """Get active automation rules for a franchise and event type, with template details."""
    sql = """
        SELECT ar.*, at.name AS template_name, at.default_delay_minutes, at.default_message
        FROM automation_rules ar
        LEFT JOIN automation_templates at ON at.id = ar.template_id
        WHERE ar.franchise_id = %s
          AND ar.event_type = %s
          AND COALESCE(ar.active, TRUE) = TRUE
        ORDER BY ar.id
    """
    return query_db(sql, (franchise_id, event_type))


def get_pending_scheduled_jobs(limit=50):
    """Get pending scheduled jobs due now or earlier, with event type."""
    sql = """
        SELECT sj.*, ar.event_type
        FROM scheduled_jobs sj
        LEFT JOIN automation_rules ar ON ar.id = sj.automation_rule_id
        WHERE sj.status = 'pending'
          AND sj.scheduled_for <= %s
        ORDER BY sj.scheduled_for ASC, sj.id ASC
        LIMIT %s
    """
    return query_db(sql, (utc_now(), limit))


def get_failed_job_by_id(failed_job_id):
    """Get a failed job by ID that is not yet resolved."""
    sql = "SELECT * FROM failed_jobs WHERE id = %s AND COALESCE(resolved, 0) = 0"
    return query_db(sql, (failed_job_id,), one=True)


def get_scheduled_job_by_id_and_status_running_and_locked_at(job_id, locked_at):
    """Get a scheduled job by ID that is running with a specific locked_at."""
    sql = "SELECT * FROM scheduled_jobs WHERE id = %s AND status = 'running' AND locked_at = %s"
    return query_db(sql, (job_id, locked_at), one=True)


def get_failed_jobs_count():
    """Get count of failed jobs (not resolved)."""
    sql = "SELECT COUNT(*) AS total FROM failed_jobs WHERE resolved = 0"
    result = query_db(sql, (), one=True)
    return result['total'] if result else 0


def get_scheduled_job_by_id_and_status_pending(job_id):
    """Get a scheduled job by ID that is pending."""
    sql = "SELECT * FROM scheduled_jobs WHERE id = %s AND status = 'pending'"
    return query_db(sql, (job_id,), one=True)


def get_scheduled_job_by_id(job_id):
    """Get a scheduled job by ID (any status)."""
    sql = "SELECT * FROM scheduled_jobs WHERE id = %s"
    return query_db(sql, (job_id,), one=True)


