from flask import Blueprint, render_template, request, redirect, url_for, flash, session
from database import query_db, execute_db, utc_now, get_session
from sqlalchemy import select
from models.integration_models import MetaBusinessConnection, MetaSocialConnection
from services.auth_service import active_location_required, login_required, current_user
from services.industry import get_industry_profile

onboarding_bp = Blueprint("onboarding", __name__)

def _get_onboarding_state(location_id):
    return query_db("SELECT * FROM onboarding_state WHERE location_id = %s", (location_id,), one=True)


def _create_or_update_onboarding_state(
    location_id, services_created=False, automations_enabled=False,
    go_live_ready=False, setup_progress=0
):
    existing = _get_onboarding_state(location_id)
    if existing:
        execute_db(
            """UPDATE onboarding_state
               SET setup_progress=%s, services_created=%s, automations_enabled=%s,
                   go_live_ready=%s, updated_at=%s
               WHERE location_id=%s""",
            (
                setup_progress, bool(services_created),
                bool(automations_enabled), bool(go_live_ready),
                utc_now(), location_id,
            ),
        )
    else:
        execute_db(
            """INSERT INTO onboarding_state
               (location_id, setup_progress, services_created,
                automations_enabled, go_live_ready, created_at, updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s)""",
            (
                location_id, setup_progress, bool(services_created),
                bool(automations_enabled), bool(go_live_ready),
                utc_now(), utc_now(),
            ),
        )


def _update_onboarding_state(location_id, **updates):
    if not updates:
        return
    updates["updated_at"] = utc_now()
    assignments = ", ".join(f"{key}=%s" for key in updates)
    execute_db(
        f"UPDATE onboarding_state SET {assignments} WHERE location_id=%s",
        tuple(updates.values()) + (location_id,),
    )



@onboarding_bp.route("/onboarding/location", methods=["GET", "POST"])
@login_required
def onboarding_location():
    """Create the owner's single canonical location and select its industry."""
    user = current_user()
    if user.get("role") in {"super_admin", "phanta_admin", "platform_admin"}:
        return redirect(url_for("platform_dashboard.platform_dashboard"))
    owner_id = user.get("owner_id")
    if not owner_id:
        flash("Owner account context is required.", "error")
        return redirect(url_for("auth.logout"))

    existing = query_db(
        "SELECT id, owner_id, name, industry, active FROM locations WHERE owner_id=%s LIMIT 1",
        (owner_id,), one=True,
    )
    if existing:
        if int(existing.get("id")) != int(user.get("location_id") or 0):
            session_user = dict(user)
            session_user["location_id"] = existing["id"]
            from flask import session
            session["user"] = session_user
        return redirect(url_for("onboarding.onboarding_whatsapp"))

    industries = [
        {"value": key, "label": get_industry_profile(key)["label"]}
        for key in ("workshop", "salon", "barber")
    ]

    if request.method == "GET":
        return render_template("onboarding_location.html", industries=industries)

    name = (request.form.get("location_name") or "").strip()
    industry = (request.form.get("industry") or "").strip().lower()
    if not name:
        flash("Location name is required.", "error")
        return render_template("onboarding_location.html", industries=industries)
    allowed = {item["value"] for item in industries}
    if industry not in allowed:
        flash("Please select a valid industry.", "error")
        return render_template("onboarding_location.html", industries=industries)

    # Re-check ownership immediately before creation to prevent duplicate
    # locations if the form is submitted twice.
    existing = query_db("SELECT id FROM locations WHERE owner_id=%s LIMIT 1", (owner_id,), one=True)
    if existing:
        flash("Your location already exists.", "info")
        session_user = dict(user)
        session_user["location_id"] = existing["id"]
        from flask import session
        session["user"] = session_user
        return redirect(url_for("onboarding.onboarding_whatsapp"))

    owner = query_db("SELECT id,name,email,active FROM owners WHERE id=%s", (owner_id,), one=True)
    if not owner or not owner.get("active", True):
        flash("Owner account is inactive.", "error")
        return redirect(url_for("auth.logout"))

    from services.location_provisioning_service import provision_owner_location
    result = provision_owner_location(owner_id=owner_id, name=name, industry=industry)
    if not result.get("ok"):
        flash(result.get("error") or "Unable to create location.", "error")
        return render_template("onboarding_location.html", industries=industries)

    location_id = result["location_id"]
    session_user = dict(user)
    session_user["location_id"] = location_id
    from flask import session
    session["user"] = session_user

    _create_or_update_onboarding_state(location_id)
    flash("Location created. Continue with your location configuration.", "success")
    return redirect(url_for("onboarding.onboarding_whatsapp"))


@onboarding_bp.route("/onboarding")
@login_required
def onboarding():
    """Onboarding dashboard showing progress and allowing navigation to steps."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    # Get onboarding state
    onboarding_state = _get_onboarding_state(location_id)
    if not onboarding_state:
        # Initialize onboarding state if it doesn't exist
        _create_or_update_onboarding_state(
            location_id,
            services_created=False,
            automations_enabled=False,
            go_live_ready=False
        )
        onboarding_state = _get_onboarding_state(location_id)

    # Calculate completion percentage
    completion_percentage = onboarding_state.get('setup_progress', 0) if onboarding_state else 0

    # Determine current step based on progress. Order matches the actual
    # redirect chain: location -> whatsapp -> flyer-lady -> business ->
    # services -> automation -> team -> review. WhatsApp and Flyer Lady
    # moved to the front of onboarding (Flyer Lady is new here entirely --
    # it never had an onboarding step before) so both channel connections
    # happen immediately after creating a location, ahead of the more
    # administrative steps.
    current_step = 'whatsapp'
    if completion_percentage >= 15:
        current_step = 'flyer_lady'
    if completion_percentage >= 30:
        current_step = 'business'
    if completion_percentage >= 45:
        current_step = 'services'
    if completion_percentage >= 60:
        current_step = 'automation'
    if completion_percentage >= 80:
        current_step = 'team'
    if completion_percentage >= 100:
        current_step = 'review'

    return render_template("onboarding.html",
                         onboarding_state=onboarding_state,
                         completion_percentage=completion_percentage,
                         current_step=current_step)


@onboarding_bp.route("/onboarding/business", methods=["GET", "POST"])
@login_required
def onboarding_business():
    """Business information step of onboarding."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        # Get existing business information
        location = query_db("SELECT * FROM locations WHERE id = %s", (location_id,), one=True)
        if not location:
            location = {
                'name': '', 'trading_name': '', 'business_registration_number': '',
                'vat_number': '', 'email': '', 'phone': '', 'whatsapp_number': '',
                'timezone': 'UTC', 'currency': 'USD', 'language': 'en',
                'description': '', 'physical_address': '', 'postal_address': '',
                'website': ''
            }

        timezones = [
            {"value": "Africa/Johannesburg", "label": "South Africa (Johannesburg)"},
            {"value": "UTC", "label": "UTC"},
            {"value": "America/New_York", "label": "Eastern Time (New York)"},
            {"value": "America/Chicago", "label": "Central Time (Chicago)"},
            {"value": "America/Denver", "label": "Mountain Time (Denver)"},
            {"value": "America/Los_Angeles", "label": "Pacific Time (Los Angeles)"},
            {"value": "Europe/London", "label": "Greenwich Mean Time (London)"},
            {"value": "Europe/Paris", "label": "Central European Time (Paris)"},
            {"value": "Asia/Tokyo", "label": "Japan Standard Time (Tokyo)"},
            {"value": "Asia/Shanghai", "label": "China Standard Time (Shanghai)"},
            {"value": "Australia/Sydney", "label": "Australian Eastern Time (Sydney)"}
        ]

        form = {
            "business_name": location.get("name", ""),
            "trading_name": location.get("trading_name", ""),
            "business_email": location.get("contact_email", ""),
            "business_phone": location.get("contact_phone", ""),
            "province": location.get("province", ""),
            "city": location.get("city", ""),
            "address": location.get("physical_address", ""),
            "workshop_type": location.get("workshop_type", "") if location.get("industry") == "workshop" else "",
            "timezone": location.get("timezone", "Africa/Johannesburg"),
            "currency": location.get("currency", "ZAR"),
            "language": location.get("language", "en"),
            "description": location.get("description", ""),
        }
        return render_template("onboarding_business.html",
                             location=location, form=form, timezones=timezones)

    elif request.method == "POST":
        # Process business information form
        form_data = request.form

        # Map the onboarding form to the canonical location columns.
        field_map = {
            'business_name': 'name',
            'business_email': 'contact_email',
            'business_phone': 'contact_phone',
            'address': 'physical_address',
            'trading_name': 'trading_name',
            'province': 'province',
            'city': 'city',
            'workshop_type': 'workshop_type',
            'timezone': 'timezone',
            'currency': 'currency',
            'language': 'language',
            'description': 'description',
        }
        updates = {}
        for source, target in field_map.items():
            if source in form_data:
                value = form_data[source].strip()
                updates[target] = value if value else None

        # workshop_type belongs to the workshop industry layer, not the
        # universal location contract.
        location_row = query_db(
            "SELECT industry FROM locations WHERE id=%s AND owner_id=%s",
            (location_id, user["owner_id"]), one=True
        )
        if (location_row or {}).get("industry") != "workshop":
            updates.pop("workshop_type", None)

        if updates:
            updates["updated_at"] = utc_now()
            set_clause = ", ".join([f"{key}=%s" for key in updates.keys()])
            query = f"UPDATE locations SET {set_clause} WHERE id=%s"
            params = list(updates.values()) + [location_id]
            execute_db(query, tuple(params))

        # Update onboarding progress to 20% (business info complete)
        _update_onboarding_state(location_id, setup_progress=20)

        flash('Business information saved successfully', 'success')
        return redirect(url_for('onboarding.onboarding_services'))


@onboarding_bp.route("/onboarding/services", methods=["GET", "POST"])
@login_required
def onboarding_services():
    """Configure services using the canonical services schema."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        services = query_db(
            """SELECT id, name, description, duration_minutes,
                      active AS is_enabled, display_order
               FROM services
               WHERE location_id=%s
               ORDER BY display_order, name""",
            (location_id,),
        )
        if not services:
            location = query_db(
                "SELECT industry FROM locations WHERE id=%s", (location_id,), one=True
            )
            industry = (location or {}).get("industry") or "workshop"
            profile = get_industry_profile(industry)
            for order, (name, description, duration) in enumerate(profile["default_services"], 1):
                execute_db(
                    """INSERT INTO services
                       (location_id,name,description,duration_minutes,active,display_order,created_at,updated_at)
                       VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
                    (location_id,name,description,duration,True,order,utc_now(),utc_now()),
                )
            services = query_db(
                """SELECT id,name,description,duration_minutes,
                          active AS is_enabled,display_order
                   FROM services WHERE location_id=%s
                   ORDER BY display_order,name""",
                (location_id,),
            )
        return render_template("onboarding_services.html", services=services)

    form_data = request.form
    service_id = form_data.get("service_id")
    action = form_data.get("action")
    if service_id and action == "delete":
        execute_db("DELETE FROM services WHERE id=%s AND location_id=%s", (service_id, location_id))
        flash("Service deleted successfully", "success")
        return redirect(url_for("onboarding.onboarding_services"))

    name = form_data.get("name", "").strip()
    description = form_data.get("description", "").strip()
    if not name:
        flash("Service name is required", "error")
        return redirect(url_for("onboarding.onboarding_services"))
    try:
        duration = max(5, int(form_data.get("duration", 60)))
    except (TypeError, ValueError):
        flash("Duration must be a number greater than or equal to 5", "error")
        return redirect(url_for("onboarding.onboarding_services"))
    active = form_data.get("is_enabled", "1") == "1"
    try:
        display_order = int(form_data.get("display_order", 0))
    except ValueError:
        display_order = 0

    if service_id:
        existing = query_db(
            "SELECT id FROM services WHERE id=%s AND location_id=%s", (service_id, location_id), one=True
        )
        if not existing:
            flash("Service not found or access denied", "error")
            return redirect(url_for("onboarding.onboarding_services"))
        execute_db(
            """UPDATE services SET name=%s,description=%s,duration_minutes=%s,
               active=%s,display_order=%s,updated_at=%s
               WHERE id=%s AND location_id=%s""",
            (name,description,duration,active,display_order,utc_now(),service_id,location_id),
        )
        flash("Service updated successfully", "success")
    else:
        if display_order <= 0:
            row = query_db(
                "SELECT COALESCE(MAX(display_order),0) AS max_order FROM services WHERE location_id=%s",
                (location_id,), one=True
            )
            display_order = int((row or {}).get("max_order") or 0) + 1
        execute_db(
            """INSERT INTO services
               (location_id,name,description,duration_minutes,active,display_order,created_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s)""",
            (location_id,name,description,duration,active,display_order,utc_now(),utc_now()),
        )
        flash("Service added successfully", "success")
    _update_onboarding_state(location_id, setup_progress=40)
    return redirect(url_for("onboarding.onboarding_whatsapp"))


@onboarding_bp.route("/onboarding/whatsapp", methods=["GET", "POST"])
@login_required
def onboarding_whatsapp():
    """Phase 5 WhatsApp onboarding: use Meta Embedded Signup, never manual tokens."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    session_db = get_session()
    try:
        connection = session_db.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.location_id == location_id
            )
        )
        return render_template(
            "connect_whatsapp.html",
            connection=connection,
            onboarding=True,
        )
    finally:
        session_db.close()


@onboarding_bp.route("/onboarding/flyer-lady", methods=["GET"])
@login_required
def onboarding_flyer_lady():
    """Connect Flyer Lady's Facebook Page as part of initial account setup,
    the same way onboarding_whatsapp() above connects WhatsApp -- moved
    here (and WhatsApp moved earlier too, see the redirect chain through
    this file) so both channel connections happen immediately after
    creating a location, rather than being buried later in onboarding or,
    for Flyer Lady specifically, never being part of onboarding at all."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    session_db = get_session()
    try:
        connection = session_db.scalar(
            select(MetaSocialConnection).where(
                MetaSocialConnection.location_id == location_id
            )
        )
        return render_template(
            "connect_flyer_lady.html",
            connection=connection,
            onboarding=True,
        )
    finally:
        session_db.close()


@onboarding_bp.route("/onboarding/automation", methods=["GET", "POST"])
@login_required
def onboarding_automation():
    """Automation setup step of onboarding."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    if request.method == "GET":
        # Get automation templates with current settings
        templates = query_db("""
            SELECT at.*,
                   COALESCE(ar.active, FALSE) as is_active,
                   COALESCE(ar.delay_minutes, at.default_delay_minutes) as delay_minutes,
                   COALESCE(ar.preferred_channel, 'whatsapp') as preferred_channel
            FROM automation_templates at
            LEFT JOIN automation_rules ar ON at.id = ar.template_id AND ar.location_id = %s
            WHERE at.industry = (SELECT industry FROM locations WHERE id = %s)
            ORDER BY at.name
        """, (location_id, location_id))

        # Group by industry for display
        from collections import defaultdict
        automation_templates_by_industry = defaultdict(list)
        for template in templates:
            automation_templates_by_industry[template['industry']].append(template)

        return render_template("onboarding_automation.html",
                             automation_templates_by_industry=automation_templates_by_industry)

    elif request.method == "POST":
        # Process automation settings form
        form_data = request.form
        from database import execute_db, utc_now

        # Process each template that was submitted
        updated_count = 0

        # Get all templates to know what to look for
        selected_industry = query_db(
            "SELECT industry FROM locations WHERE id=%s", (location_id,), one=True
        )
        industry = (selected_industry or {}).get("industry")
        if not industry:
            flash("Location industry is not configured.", "error")
            return redirect(url_for("onboarding.onboarding_location"))
        all_templates = query_db(
            "SELECT id, default_delay_minutes, event_type FROM automation_templates WHERE industry=%s",
            (industry,),
        )

        for template in all_templates:
            template_id = template['id']

            # Check if this template was submitted in the form
            enabled_key = f"enabled_{template_id}"
            delay_key = f"delay_{template_id}"
            channel_key = f"channel_{template_id}"

            is_enabled = form_data.get(enabled_key) == "1"
            delay_minutes_str = form_data.get(delay_key, "")
            preferred_channel = form_data.get(channel_key, "whatsapp")

            # Validate delay minutes
            delay_minutes = None
            if delay_minutes_str.strip():
                try:
                    delay_minutes = int(delay_minutes_str)
                    if delay_minutes < 0:
                        delay_minutes = None  # Invalid, will use default
                except ValueError:
                    delay_minutes = None  # Invalid, will use default

            # Get existing rule for this location and template
            existing_rule = query_db(
                "SELECT id FROM automation_rules WHERE location_id = %s AND template_id = %s",
                (location_id, template_id)
            )

            if existing_rule:
                # Update existing rule
                updates = []
                params = []
                if enabled_key in form_data:
                    updates.append("active = %s")
                    params.append(is_enabled)
                if delay_key in form_data and delay_minutes is not None:
                    updates.append("delay_minutes = %s")
                    params.append(delay_minutes)
                if channel_key in form_data:
                    updates.append("preferred_channel = %s")
                    params.append(preferred_channel)

                if updates:
                    updates.append("updated_at = %s")
                    params.append(utc_now())
                    params.extend([location_id, template_id])

                    query = f"UPDATE automation_rules SET {', '.join(updates)} WHERE location_id = %s AND template_id = %s"
                    execute_db(query, tuple(params))
                    updated_count += 1
            else:
                # Create new rule if at least one field was specified
                if enabled_key in form_data or delay_key in form_data or channel_key in form_data:
                    # Get the default values from the template
                    default_delay = template['default_delay_minutes']
                    event_type = template['event_type']

                    if event_type:
                        delay_to_use = delay_minutes if delay_minutes is not None else default_delay

                        # Insert new rule
                        execute_db("""
                            INSERT INTO automation_rules
                            (location_id, template_id, event_type, active, delay_minutes, preferred_channel, created_at, updated_at)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            location_id,
                            template_id,
                            event_type,
                            is_enabled,
                            delay_to_use,
                            preferred_channel,
                            utc_now(),
                            utc_now()
                        ))
                        updated_count += 1

        if updated_count > 0:
            flash(f'Automation settings updated successfully ({updated_count} changes)', 'success')
        else:
            flash('No changes were made to automation settings', 'info')

        # Update onboarding progress to 80% (automation setup complete)
        _update_onboarding_state(location_id, setup_progress=80)

        return redirect(url_for('onboarding.onboarding_team'))


@onboarding_bp.route("/onboarding/team", methods=["GET", "POST"])
@login_required
def onboarding_team():
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect
    user = current_user()
    location_id = user["location_id"]
    if user.get("role") not in {"owner", "admin", "location_admin"}:
        flash("Access denied. Owner or admin privileges required.", "error")
        return redirect(url_for("onboarding.onboarding_review"))

    if request.method == "GET":
        team_members = query_db(
            """SELECT id,username,email,full_name,role,active AS is_active,last_login,location_id
               FROM users WHERE location_id=%s ORDER BY role,username""",
            (location_id,),
        )
        roles = [
            {"value":"owner","label":"Owner"},
            {"value":"manager","label":"Manager"},
            {"value":"reception","label":"Reception"},
            {"value":"technician","label":"Technician"},
            {"value":"readonly","label":"Read Only"},
        ]
        return render_template("onboarding_team.html", team_members=team_members, roles=roles)

    action = request.form.get("action")
    if action == "invite":
        email = request.form.get("email","").strip().lower()
        role = request.form.get("role","reception").strip().lower()
        if not email or "@" not in email:
            flash("A valid email address is required", "error")
            return redirect(url_for("onboarding.onboarding_team"))
        if role not in {"owner","manager","reception","technician","readonly"}:
            flash("Invalid role specified", "error")
            return redirect(url_for("onboarding.onboarding_team"))
        if query_db("SELECT id FROM users WHERE lower(email)=lower(%s)", (email,), one=True):
            flash("A user with this email already exists", "error")
            return redirect(url_for("onboarding.onboarding_team"))
        import secrets
        from werkzeug.security import generate_password_hash
        temp_password = secrets.token_urlsafe(9)
        execute_db(
            """INSERT INTO users
               (location_id,username,email,password,password_hash,full_name,role,active,must_reset_password,created_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
            (location_id,email,email,"",generate_password_hash(temp_password),
             request.form.get("full_name","").strip(),role,True,True,utc_now(),utc_now()),
        )
        flash(f"Invitation prepared for {email}. Temporary password: {temp_password}", "info")
    elif action == "toggle_status":
        user_id = request.form.get("user_id")
        if user_id:
            row = query_db("SELECT active FROM users WHERE id=%s AND location_id=%s", (user_id,location_id), one=True)
            if row:
                execute_db("UPDATE users SET active=%s,updated_at=%s WHERE id=%s AND location_id=%s",
                           (not row["active"],utc_now(),user_id,location_id))
    elif action == "change_role":
        user_id = request.form.get("user_id")
        role = request.form.get("new_role","").strip().lower()
        if role not in {"owner","manager","reception","technician","readonly"}:
            flash("Invalid role specified", "error")
        elif user_id:
            execute_db("UPDATE users SET role=%s,updated_at=%s WHERE id=%s AND location_id=%s",
                       (role,utc_now(),user_id,location_id))
    _update_onboarding_state(location_id, setup_progress=100)
    return redirect(url_for("onboarding.onboarding_review"))


@onboarding_bp.route("/onboarding/review")
@login_required
def onboarding_review():
    """Onboarding review step showing all configured settings."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    # Get location information
    location = query_db("SELECT * FROM locations WHERE id = %s", (location_id,), one=True)

    # Get onboarding state
    onboarding_state = _get_onboarding_state(location_id)

    # Get services count and details
    services = query_db("""
        SELECT id, name, description, duration_minutes, active AS is_enabled
        FROM services
        WHERE location_id = %s
        ORDER BY name
    """, (location_id,))

    # Phase 5 WhatsApp status comes from the canonical Meta connection table.
    session_db = get_session()
    try:
        meta_connection = session_db.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.location_id == location_id
            )
        )
    finally:
        session_db.close()
    whatsapp_config = {
        "phone_number_id": getattr(meta_connection, "phone_number_id", None),
        "business_account_id": getattr(meta_connection, "business_id", None),
        "is_verified": bool(meta_connection and getattr(meta_connection, "connection_status", "") == "connected"),
        "webhook_verified": bool(meta_connection and getattr(meta_connection, "connection_status", "") == "connected"),
    } if meta_connection else None

    # Get automation rules count and status
    automation_rules = query_db("""
        SELECT COUNT(*) as total_count,
               SUM(CASE WHEN active THEN 1 ELSE 0 END) as active_count
        FROM automation_rules
        WHERE location_id = %s
    """, (location_id,), one=True)

    # Get team members count
    team_members = query_db("""
        SELECT COUNT(*) as total_count,
               SUM(CASE WHEN active THEN 1 ELSE 0 END) as active_count,
               SUM(CASE WHEN role IN ('owner', 'manager') THEN 1 ELSE 0 END) as management_count
        FROM users
        WHERE location_id = %s
    """, (location_id,), one=True)

    # Determine if onboarding is complete
    is_complete = (
        location and
        onboarding_state and
        onboarding_state.get('setup_progress', 0) >= 100 and
        bool(location.get('name')) and
        len(services) > 0 and
        whatsapp_config is not None and
        (automation_rules['total_count'] or 0) > 0 and
        (team_members['total_count'] or 0) >= 1  # At least the owner
    )

    return render_template("onboarding_review.html",
                         location=location,
                         onboarding_state=onboarding_state,
                         services=services,
                         whatsapp_config=whatsapp_config,
                         automation_rules=automation_rules,
                         team_members=team_members,
                         is_complete=is_complete)


@onboarding_bp.route("/onboarding/complete", methods=["POST"])
@login_required
def onboarding_complete():
    """Complete the onboarding process."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user["location_id"]

    # Verify that onboarding is actually complete before allowing completion
    location = query_db("SELECT * FROM locations WHERE id = %s", (location_id,), one=True)
    onboarding_state = _get_onboarding_state(location_id)
    services = query_db("SELECT id FROM services WHERE location_id = %s", (location_id,))
    session_db = get_session()
    try:
        meta_connection = session_db.scalar(
            select(MetaBusinessConnection).where(
                MetaBusinessConnection.location_id == location_id
            )
        )
    finally:
        session_db.close()
    whatsapp_config = meta_connection
    automation_rules = query_db("SELECT id FROM automation_rules WHERE location_id = %s", (location_id,), one=True)
    team_members = query_db("SELECT id FROM users WHERE location_id = %s", (location_id,), one=True)

    is_complete = (
        location and
        onboarding_state and
        bool(location.get('name')) and
        len(services) > 0 and
        whatsapp_config is not None and
        automation_rules is not None and
        team_members is not None
    )

    if not is_complete:
        flash('Please complete all required steps before finishing onboarding.', 'error')
        return redirect(url_for('onboarding.onboarding_review'))

    # Mark onboarding as complete
    if onboarding_state:
        execute_db("""
            UPDATE onboarding_state
            SET setup_progress = 100,
                services_created = 1,
                automations_enabled = 1,
                go_live_ready = 1,
                updated_at = %s
            WHERE location_id = %s
        """, (utc_now(), location_id))

    # Also update the onboarding session if it exists
    onboarding_session = query_db("SELECT id FROM onboarding_sessions WHERE location_id = %s ORDER BY id DESC LIMIT 1", (location_id,), one=True)
    if onboarding_session:
        execute_db("""
            UPDATE onboarding_sessions
            SET status = 'completed',
                current_step = 'completed',
                completed_at = %s,
                updated_at = %s
            WHERE id = %s
        """, (utc_now(), utc_now(), onboarding_session['id']))

    flash('Congratulations! Your workshop setup is now complete. You\'re ready to start using PHANTA.', 'success')
    return redirect(url_for('index'))

