"""
Automation Repository for Workshop System Version 2.

This repository handles all database operations for the Automation entity.
It interacts with the existing tables:
- automation_rules
- automation_templates
- scheduled_jobs
- failed_jobs
"""

from database import query_db, utc_now


# ============================================================================
# Shared SQL
# ============================================================================

_AUTOMATION_RULE_SELECT = """
    SELECT
        ar.*,
        ar.location_id AS automation_location_id,
        at.name AS template_name,
        at.default_delay_minutes,
        at.default_message
    FROM automation_rules ar
    LEFT JOIN automation_templates at
        ON at.id = ar.template_id
"""

_PENDING_JOB_SELECT = """
    SELECT
        sj.*,
        ar.location_id AS automation_location_id,
        ar.event_type
    FROM scheduled_jobs sj
    LEFT JOIN automation_rules ar
        ON ar.id = sj.automation_rule_id
"""


# ============================================================================
# Internal Helpers
# ============================================================================

def _get_scheduled_job(job_id, status=None, locked_at=None, location_id=None):
    """
    Generic scheduled job lookup.
    """

    sql = """
        SELECT *
        FROM scheduled_jobs
        WHERE id = %s
    """

    params = [job_id]

    if status is not None:
        sql += " AND status = %s"
        params.append(status)

    if locked_at is not None:
        sql += " AND locked_at = %s"
        params.append(locked_at)

    if location_id is not None:
        sql += """ AND EXISTS (
            SELECT 1 FROM automation_rules ar
            WHERE ar.id = scheduled_jobs.automation_rule_id
              AND ar.location_id = %s
        )"""
        params.append(location_id)

    sql += " LIMIT 1"

    return query_db(
        sql,
        tuple(params),
        one=True,
    )


# ============================================================================
# Location
# ============================================================================

def get_location_by_id(location_id):
    """Return a location."""

    return query_db(
        "SELECT * FROM locations WHERE id = %s",
        (location_id,),
        one=True,
    )


# ============================================================================
# Automation Rules
# ============================================================================

def get_automation_rules_by_location_and_event(
    location_id,
    event_type,
):
    """Return active automation rules."""

    sql = f"""
        {_AUTOMATION_RULE_SELECT}
        WHERE ar.location_id = %s
          AND ar.event_type = %s
          AND COALESCE(ar.active, TRUE) = TRUE
        ORDER BY ar.id
    """

    return query_db(
        sql,
        (location_id, event_type),
    )


# ============================================================================
# Scheduled Jobs
# ============================================================================

def get_pending_scheduled_jobs(limit=50, location_id=None):
    """Return pending scheduled jobs ready to execute."""

    sql = f"""
        {_PENDING_JOB_SELECT}
        WHERE sj.status = 'pending'
          AND sj.scheduled_for <= %s
    """
    params = [utc_now()]
    if location_id is not None:
        sql += " AND ar.location_id = %s"
        params.append(location_id)
    sql += """ ORDER BY sj.scheduled_for ASC,
                 sj.id ASC
        LIMIT %s
    """
    params.append(limit)

    return query_db(sql, tuple(params))


def get_scheduled_job_by_id(job_id):
    """Return a scheduled job."""

    return _get_scheduled_job(job_id)


def get_scheduled_job_by_id_and_status_pending(job_id):
    """Return a pending scheduled job."""

    return _get_scheduled_job(
        job_id,
        status="pending",
    )


def get_scheduled_job_by_id_and_status_running_and_locked_at(
    job_id,
    locked_at,
):
    """Return a running locked scheduled job."""

    return _get_scheduled_job(
        job_id,
        status="running",
        locked_at=locked_at,
    )


# ============================================================================
# Failed Jobs
# ============================================================================

def get_failed_job_by_id(failed_job_id, location_id=None):
    """Return an unresolved failed job."""

    sql = """
        SELECT fj.*, ar.location_id AS automation_location_id
        FROM failed_jobs fj
        JOIN scheduled_jobs sj ON sj.id = fj.scheduled_job_id
        JOIN automation_rules ar ON ar.id = sj.automation_rule_id
        WHERE fj.id = %s
          AND COALESCE(fj.resolved, FALSE) = FALSE
    """
    params = [failed_job_id]
    if location_id is not None:
        sql += " AND ar.location_id = %s"
        params.append(location_id)
    return query_db(sql, tuple(params), one=True)


def get_failed_jobs_count(location_id=None):
    """Return the number of unresolved failed jobs."""

    sql = """
        SELECT COUNT(*) AS total
        FROM failed_jobs fj
        JOIN scheduled_jobs sj ON sj.id = fj.scheduled_job_id
        JOIN automation_rules ar ON ar.id = sj.automation_rule_id
        WHERE COALESCE(fj.resolved, FALSE) = FALSE
    """
    params = []
    if location_id is not None:
        sql += " AND ar.location_id = %s"
        params.append(location_id)

    result = query_db(sql, tuple(params), one=True)

    return result["total"] if result else 0