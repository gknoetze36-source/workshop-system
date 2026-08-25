"""
Franchise Repository for Workshop System Version 2.

This repository handles all database operations for the Franchise entity.
"""

from database import query_db, execute_db, utc_now
from helpers.common import fetch_one, fetch_all
from helpers.common import db_bool


# ============================================================================
# Internal Helpers
# ============================================================================


def _get_franchise_by_field(field, value):
    """
    Generic franchise lookup helper.

    Args:
        field: Database column name.
        value: Value to search for.
    """
    allowed_fields = {
        "id",
        "slug",
    }

    if field not in allowed_fields:
        raise ValueError(f"Unsupported franchise lookup field: {field}")

    sql = f"""
        SELECT *
        FROM franchises
        WHERE {field} = %s
        LIMIT 1
    """

    return query_db(sql, (value,), one=True)


# ============================================================================
# Franchise Lookups
# ============================================================================


def get_franchise_by_id(franchise_id):
    """Return a franchise by its ID."""
    return _get_franchise_by_field("id", franchise_id)


def get_franchise_by_slug(slug):
    """Return a franchise by its slug."""
    return _get_franchise_by_field("slug", slug)


# ============================================================================
# Visibility
# ============================================================================


def get_visible_franchises(user=None, include_inactive=False):
    """
    Return the franchises visible to the supplied user.
    """

    clauses = []
    params = []

    if not include_inactive:
        clauses.append("active = TRUE")

    if user:
        role = user.get("role")

        if role != "super_admin":
            franchise_id = user.get("franchise_id")

            if not franchise_id:
                return []

            clauses.append("id = %s")
            params.append(franchise_id)

    where_clause = ""

    if clauses:
        where_clause = "WHERE " + " AND ".join(clauses)

    sql = f"""
        SELECT *
        FROM franchises
        {where_clause}
        ORDER BY name
    """

    return query_db(sql, tuple(params))


# ============================================================================
# Franchise Statistics & Counts
# ============================================================================


def get_franchise_branch_and_user_counts(franchise_id):
    """Get counts of branches and users for a franchise."""
    branch_total = query_db(
        "SELECT COUNT(*) AS total FROM branches WHERE franchise_id=%s AND COALESCE(active, TRUE)=TRUE",
        (franchise_id,),
        one=True,
    )
    user_total = query_db(
        "SELECT COUNT(*) AS total FROM users WHERE franchise_id=%s AND COALESCE(active, TRUE)=TRUE",
        (franchise_id,),
        one=True,
    )
    return {
        "branches": int((branch_total or {}).get("total") or 0),
        "users": int((user_total or {}).get("total") or 0),
    }


# ============================================================================
# Franchise Provisioning & Setup
# ============================================================================


def update_franchise_provision(
    franchise_id,
    industry,
    plan_code,
    branch_limit,
    user_limit,
    automation_enabled,
    chatbot_enabled,
    reporting_enabled,
    custom_integrations_enabled,
    priority_support_enabled,
    monthly_message_limit,
):
    """Update franchise with provisioning details."""
    execute_db(
        """
        UPDATE franchises
        SET industry=%s, plan_code=%s, branch_limit=%s, user_limit=%s,
            automation_enabled=%s, chatbot_enabled=%s, reporting_enabled=%s,
            custom_integrations_enabled=%s, priority_support_enabled=%s,
            monthly_message_limit=%s, updated_at=%s
        WHERE id=%s
        """,
        (
            industry,
            plan_code,
            branch_limit,
            user_limit,
            automation_enabled,
            chatbot_enabled,
            reporting_enabled,
            custom_integrations_enabled,
            priority_support_enabled,
            monthly_message_limit,
            utc_now(),
            franchise_id,
        ),
    )


def update_feature_flag(franchise_id, feature_key, enabled):
    """Update or create a feature flag for a franchise."""
    existing = fetch_one(
        "SELECT id FROM feature_flags WHERE franchise_id=%s AND feature_key=%s",
        (franchise_id, feature_key),
    )
    if existing:
        execute_db(
            "UPDATE feature_flags SET enabled=%s, updated_at=%s WHERE id=%s",
            (enabled, utc_now(), existing["id"]),
        )
    else:
        execute_db(
            "INSERT INTO feature_flags (franchise_id, feature_key, enabled, created_at, updated_at) VALUES (%s, %s, %s, %s, %s)",
            (franchise_id, feature_key, enabled, utc_now(), utc_now()),
        )


def create_onboarding_session(franchise_id, industry, plan_code):
    """Create or update an onboarding session for a franchise."""
    session_row = fetch_one(
        "SELECT id FROM onboarding_sessions WHERE franchise_id=%s ORDER BY id DESC LIMIT 1",
        (franchise_id,),
    )
    if not session_row:
        execute_db(
            """
            INSERT INTO onboarding_sessions
            (franchise_id, industry, selected_plan, status, current_step, started_at, completed_at, created_at, updated_at)
            VALUES (%s, %s, %s, 'completed', 'provisioned', %s, %s, %s, %s)
            """,
            (franchise_id, industry, plan_code, utc_now(), utc_now(), utc_now(), utc_now()),
        )
    else:
        execute_db(
            """
            UPDATE onboarding_sessions
            SET industry=%s, selected_plan=%s, status='completed', current_step='provisioned',
                completed_at=%s, updated_at=%s
            WHERE id=%s
            """,
            (industry, plan_code, utc_now(), utc_now(), session_row["id"]),
        )


def create_or_update_onboarding_state(
    franchise_id, services_created, automations_enabled, go_live_ready
):
    """Create or update onboarding state for a franchise."""
    state = fetch_one(
        "SELECT id FROM onboarding_state WHERE franchise_id=%s", (franchise_id,)
    )
    if state:
        execute_db(
            """
            UPDATE onboarding_state
            SET setup_progress=100, services_created=%s, automations_enabled=%s,
                go_live_ready=%s, updated_at=%s
            WHERE id=%s
            """,
            (services_created, automations_enabled, go_live_ready, utc_now(), state["id"]),
        )
    else:
        execute_db(
            """
            INSERT INTO onboarding_state
            (franchise_id, setup_progress, services_created, automations_enabled,
                go_live_ready, created_at, updated_at)
            VALUES (%s, 100, %s, %s, %s, %s, %s)
            """,
            (franchise_id, services_created, automations_enabled, go_live_ready, utc_now(), utc_now()),
        )


def get_first_branch_for_franchise(franchise_id):
    """Get the first branch for a franchise."""
    return fetch_one(
        "SELECT * FROM branches WHERE franchise_id=%s ORDER BY id LIMIT 1",
        (franchise_id,),
    )


# ============================================================================
# Template and Automation Rules
# ============================================================================


def get_template_by_industry(industry):
    """Get a template by industry."""
    return fetch_one(
        "SELECT * FROM industry_templates WHERE industry=%s AND active=TRUE",
        (industry,),
    )


def get_automation_templates_by_industry(industry):
    """Get automation templates by industry."""
    return fetch_all(
        "SELECT * FROM automation_templates WHERE industry=%s AND active=TRUE",
        (industry,),
    )


def get_automation_rule_(franchise_id, template_id):
    """Get an automation rule for a franchise and template."""
    return fetch_one(
        "SELECT id FROM automation_rules WHERE franchise_id=%s AND template_id=%s",
        (franchise_id, template_id),
    )


def update_automation_rule(rule_id, active, delay_minutes):
    """Update an automation rule."""
    execute_db(
        """
        UPDATE automation_rules
        SET active=%s, delay_minutes=%s, updated_at=%s
        WHERE id=%s
        """,
        (db_bool(active), delay_minutes, utc_now(), rule_id),
    )


def create_automation_rule(franchise_id, template_id, name, event_type, action_json, delay_minutes, active):
    """Create an automation rule."""
    execute_db(
        """
        INSERT INTO automation_rules (
            franchise_id, template_id, name, event_type, conditions_json, action_json,
            delay_minutes, active, created_at, updated_at
        ) VALUES (%s, %s, %s, %s, '{}', %s, %s, %s, %s, %s)
        """,
        (
            franchise_id,
            template_id,
            name,
            event_type,
            action_json,
            db_bool(delay_minutes),
            db_bool(active),
            utc_now(),
            utc_now(),
        ),
    )
