"""
Franchise Service for Workshop System Version 2.

Business logic for Franchise management.
Depends only on the Franchise Repository.
"""

# ============================================================================
# Repository
# ============================================================================

from repositories.franchise_repository import (
    get_franchise_by_id as _get_franchise_by_id,
    get_franchise_by_slug as _get_franchise_by_slug,
    get_visible_franchises as _get_visible_franchises,
    get_franchise_branch_and_user_counts as _get_franchise_branch_and_user_counts,
    update_franchise_provision as _update_franchise_provision,
    update_feature_flag as _update_feature_flag,
    create_onboarding_session as _create_onboarding_session,
    create_or_update_onboarding_state as _create_or_update_onboarding_state,
    get_first_branch_for_franchise as _get_first_branch_for_franchise,
    get_template_by_industry,
    get_automation_templates_by_industry,
    get_automation_rule_ as get_automation_rule,
    update_automation_rule,
    create_automation_rule,
)

# ============================================================================
# Service Dependencies
# ============================================================================

from helpers.common import db_bool
from services.catalog_service import ensure_service
from constants.platform_constants import (
    PLAN_DEFINITIONS,
    DEFAULT_SERVICES_BY_INDUSTRY,
)

from helpers.common import boolish



# ============================================================================
# Franchise Services
# ============================================================================


def get_franchise_by_id(franchise_id):
    """Return a franchise by its ID."""
    return _get_franchise_by_id(franchise_id)


def get_franchise_by_slug(slug):
    """Return a franchise by its public slug."""
    return _get_franchise_by_slug(slug)


def get_visible_franchises(user=None, include_inactive=False):
    """Return the franchises visible to the supplied user."""
    return _get_visible_franchises(user=user, include_inactive=include_inactive)


# ============================================================================
# Franchise Business Logic
# ============================================================================


def franchise_counts(franchise_id):
    """Get counts of branches and users for a franchise."""
    return _get_franchise_branch_and_user_counts(franchise_id)


def can_add_branch(franchise):
    """Check if a franchise can add another branch based on its limit."""
    counts = franchise_counts(franchise["id"])
    limit = int(franchise.get("branch_limit") or 0)
    return limit <= 0 or counts["branches"] < limit


def can_add_user(franchise):
    """Check if a franchise can add another user based on its limit."""
    counts = franchise_counts(franchise["id"])
    limit = int(franchise.get("user_limit") or 0)
    return limit <= 0 or counts["users"] < limit


def provision_business(franchise_id, answers=None):
    """Provision a business/franchise with initial setup."""
    franchise = _get_franchise_by_id(franchise_id)
    if not franchise:
        return {"ok": False, "error": "business not found"}

    answers = answers or {}
    industry = (
        (answers.get("industry") or franchise.get("industry") or "workshop")
        .strip()
        .lower()
    )
    plan_code = (
        (answers.get("plan") or franchise.get("plan_code") or "core")
        .strip()
        .lower()
    )
    plan = PLAN_DEFINITIONS.get(plan_code, PLAN_DEFINITIONS["core"])
    template = get_template_by_industry(industry)  # TODO: Implement repository method
    message_limit = int(
        answers.get("monthly_message_limit")
        or (template or {}).get("default_message_limit")
        or franchise.get("monthly_message_limit")
        or 2000
    )

    # Update franchise with provisioning details
    _update_franchise_provision(
        franchise_id=franchise_id,
        industry=industry,
        plan_code=plan_code,
        branch_limit=plan["branch_limit"],
        user_limit=plan["user_limit"],
        automation_enabled=db_bool(plan["automation_enabled"]),
        chatbot_enabled=db_bool(plan["chatbot_enabled"]),
        reporting_enabled=db_bool(plan["reporting_enabled"]),
        custom_integrations_enabled=db_bool(plan["custom_integrations_enabled"]),
        priority_support_enabled=db_bool(plan["priority_support_enabled"]),
        monthly_message_limit=message_limit,
    )

    # Update feature flags
    for key in (
        "automation_enabled",
        "chatbot_enabled",
        "reporting_enabled",
        "custom_integrations_enabled",
        "priority_support_enabled",
    ):
        enabled = db_bool(plan.get(key, 0))
        _update_feature_flag(franchise_id, key, enabled)

    # Ensure services exist
    branch = _get_first_branch_for_franchise(franchise_id)
    branch_id = branch.get("id") if branch else None
    for service_name in DEFAULT_SERVICES_BY_INDUSTRY.get(
        industry, ["Consultation", "Booking", "Follow-up"]
    ):
        ensure_service(franchise_id, branch_id, service_name)

    # Setup automation rules
    assigned_rules = 0
    if boolish(plan.get("automation_enabled", 0)):
        templates = get_automation_templates_by_industry(industry)  # TODO: Implement repository method
        for item in templates:
            existing = get_automation_rule(franchise_id, item["id"])  # TODO: Implement repository method
            action_json = (
                '{"type":"send_message","job_type":"send_message"}'
                if item.get("event_type") == "booking.created"
                else '{"type":"log","job_type":"automation_log"}'
            )
            if existing:
                update_automation_rule(existing["id"], db_bool(True), item.get("default_delay_minutes") or 0)  # TODO: Implement repository method
            else:
                create_automation_rule(
                    franchise_id=franchise_id,
                    template_id=item["id"],
                    name=item["name"],
                    event_type=item["event_type"],
                    action_json=action_json,
                    delay_minutes=item.get("default_delay_minutes") or 0,
                    active=True,
                )  # TODO: Implement repository method
                assigned_rules += 1

    # Setup onboarding records
    _create_onboarding_session(franchise_id, industry, plan_code)
    _create_or_update_onboarding_state(
        franchise_id,
        services_created=db_bool(True),
        automations_enabled=db_bool(plan.get("automation_enabled", 0)),
        go_live_ready=db_bool(True),
    )

    return {
        "ok": True,
        "industry": industry,
        "plan": plan_code,
        "message_limit": message_limit,
        "automation_rules_created": assigned_rules,
    }
