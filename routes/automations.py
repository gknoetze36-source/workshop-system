from flask import Blueprint, flash, redirect, render_template, request, url_for
from database import query_db, utc_now
from services.auth_service import active_location_required, current_user, login_required

automations_bp = Blueprint("automations", __name__)


@automations_bp.route("/automations/settings", methods=["GET", "POST"])
def automations_settings():
    # Check if user is logged in and has active location
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]
    location = query_db(
        "SELECT id, industry FROM locations WHERE id = %s AND owner_id = %s AND active = TRUE",
        (location_id, user["owner_id"]),
        one=True,
    )
    if not location:
        flash("Location is unavailable.", "error")
        return redirect(url_for("auth.logout"))
    industry = location.get("industry") or "workshop"

    if request.method == "GET":
        # GET request - show the automation settings form
        # Get all automation templates
        templates = query_db(
            "SELECT * FROM automation_templates WHERE industry = %s ORDER BY name",
            (industry,),
        )

        # Get existing automation rules for this location
        rules = query_db("""
            SELECT ar.*, at.name as template_name, at.industry, at.event_type as template_event_type,
                   at.default_delay_minutes, at.default_message, at.trigger_timing
            FROM automation_rules ar
            JOIN automation_templates at ON ar.template_id = at.id
            WHERE ar.location_id = %s
            ORDER BY at.industry, at.name
        """, (location_id,))

        # Convert rules to a dict for easy lookup: (template_id) -> rule
        rules_by_template_id = {}
        for rule in rules:
            rules_by_template_id[rule['template_id']] = rule

        # Prepare template data with current settings
        template_data = []
        for template in templates:
            template_id = template['id']
            rule = rules_by_template_id.get(template_id)

            template_info = {
                'id': template_id,
                'industry': template['industry'],
                'name': template['name'],
                'event_type': template['event_type'],
                'trigger_timing': template['trigger_timing'],
                'default_delay_minutes': template['default_delay_minutes'],
                'default_message': template['default_message'],
                'message': template['default_message'],
                'is_active': rule['active'] if rule else False,
                'delay_minutes': rule['delay_minutes'] if rule else template['default_delay_minutes']
            }
            template_data.append(template_info)

        # Group by industry for display
        from collections import defaultdict
        automation_templates_by_industry = defaultdict(list)
        for template in template_data:
            automation_templates_by_industry[template['industry']].append(template)

        return render_template("automation_settings.html",
                             automation_templates_by_industry=automation_templates_by_industry)

    elif request.method == "POST":
        # POST request - process form submission
        from database import execute_db, utc_now

        form_data = request.form

        # Process each template that was submitted
        updated_count = 0

        # Get all templates to know what to look for
        all_templates = query_db(
            "SELECT id, default_delay_minutes, event_type FROM automation_templates WHERE industry = %s",
            (industry,),
        )

        for template in all_templates:
            template_id = template['id']

            # Check if this template was submitted in the form
            enabled_key = f"enabled_{template_id}"
            delay_key = f"delay_{template_id}"

            is_active = 1 if form_data.get(enabled_key) == "1" else 0
            delay_minutes_str = form_data.get(delay_key, "")

            # Get existing rule for this location and template
            existing_rule = query_db(
                "SELECT id FROM automation_rules WHERE location_id = %s AND template_id = %s",
                (location_id, template_id)
            )

            if existing_rule:
                # Update existing rule
                updates = []
                params = []
                if 'enabled_' + str(template_id) in form_data:
                    updates.append("active = %s")
                    params.append(is_active)
                if delay_key in form_data and delay_minutes_str != "":
                    try:
                        delay_int = int(delay_minutes_str)
                        updates.append("delay_minutes = %s")
                        params.append(delay_int)
                    except ValueError:
                        pass  # Keep existing value if invalid

                if updates:
                    updates.append("updated_at = %s")
                    params.append(utc_now())
                    params.extend([location_id, template_id])

                    query = f"UPDATE automation_rules SET {', '.join(updates)} WHERE location_id = %s AND template_id = %s"
                    execute_db(query, tuple(params))
                    updated_count += 1
            else:
                # Create new rule if enabled
                if is_active == 1:
                    # Get the default delay for this template
                    default_delay = template['default_delay_minutes']

                    delay_to_use = int(delay_minutes_str) if delay_minutes_str != "" and delay_minutes_str.isdigit() else default_delay

                    # Insert new rule
                    execute_db(
                        """INSERT INTO automation_rules
                           (location_id, template_id, event_type, active, delay_minutes, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                        (
                            location_id,
                            template_id,
                            template['event_type'],
                            is_active,  # active
                            delay_to_use,
                            utc_now(),
                            utc_now()
                        )
                    )
                    updated_count += 1

        if updated_count > 0:
            flash(f'Automation settings updated successfully ({updated_count} changes)', 'success')
        else:
            flash('No changes were made to automation settings', 'info')

        return redirect(url_for('automations.automations_settings'))

# Automation history route
@automations_bp.route("/automations/history")
@login_required
def automations_history():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    # Get recent automation job executions for this location
    # Join with automation_rules and automation_templates to get meaningful names
    from database import query_db, utc_now

    # Get successful/completed jobs from the last 30 days
    from datetime import timedelta
    cutoff = utc_now() - timedelta(days=30)
    automation_history = query_db("""
        SELECT sj.id,sj.status,sj.scheduled_for,sj.completed_at,sj.created_at,
               ar.event_type,at.name AS template_name,at.industry,sj.attempts,sj.last_error
        FROM scheduled_jobs sj
        JOIN automation_rules ar ON sj.automation_rule_id = ar.id
        JOIN automation_templates at ON ar.template_id = at.id
        WHERE ar.location_id = %s AND sj.scheduled_for >= %s
        ORDER BY sj.scheduled_for DESC LIMIT 50
    """, (location_id, cutoff))

    # Get failed jobs from the failed_jobs table (these are permanently failed)
    failed_jobs = query_db("""
        SELECT fj.id,fj.failed_at AS scheduled_for,fj.resolved_at AS completed_at,
               fj.created_at,ar.event_type,at.name AS template_name,at.industry,
               fj.attempts,fj.last_error,'failed' AS status
        FROM failed_jobs fj
        JOIN automation_rules ar ON fj.automation_rule_id = ar.id
        JOIN automation_templates at ON ar.template_id = at.id
        WHERE ar.location_id = %s AND fj.failed_at >= %s
        ORDER BY fj.failed_at DESC LIMIT 20
    """, (location_id, cutoff))

    # Combine and sort by date
    all_jobs = list(automation_history) + list(failed_jobs)
    all_jobs.sort(key=lambda x: x['scheduled_for'] if x['scheduled_for'] else x['created_at'], reverse=True)

    # Get statistics
    stats = {
        'total': len(all_jobs),
        'successful': len([j for j in all_jobs if j['status'] == 'completed']),
        'failed': len([j for j in all_jobs if j['status'] == 'failed']),
        'pending': len([j for j in all_jobs if j['status'] == 'pending']),
        'running': len([j for j in all_jobs if j['status'] == 'running'])
    }

    return render_template("automation_history.html",
                             automation_history=all_jobs[:50],  # Limit to 50 most recent
                             stats=stats)
