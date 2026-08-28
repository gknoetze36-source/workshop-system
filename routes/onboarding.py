import json
import logging

from flask import Blueprint, render_template, request, redirect, url_for, flash, session, jsonify
from services.onboarding_service import (
    STAGES, stage_status, required_outstanding, is_onboarding_complete,
    next_incomplete_stage, progress_percent,
)
from services.legal_acceptance_service import (
    REQUIRED_DOCUMENTS, DOCUMENT_LABELS, DOCUMENT_ORDER, document_text,
    record_acceptance, outstanding_documents, accepted_documents,
    has_accepted_all, METHOD_ONBOARDING_CHECKBOX,
)
from validators.cipc_validator import validate as validate_cipc
from database import query_db, execute_db, utc_now, get_session
from sqlalchemy import select
from models.integration_models import MetaBusinessConnection, MetaSocialConnection
from services.auth_service import active_location_required, login_required, current_user
from helpers.audit import record_audit
from services.industry import get_industry_profile

logger = logging.getLogger(__name__)

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
        return redirect(url_for("onboarding.onboarding_business"))

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
        return redirect(url_for("onboarding.onboarding_business"))

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
    flash("Location created. Continue with your business details.", "success")
    return redirect(url_for("onboarding.onboarding_business"))


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

    # Stage completion is derived from the actual data by
    # services/onboarding_service, not from the stored percentage. The
    # previous version of this page mapped setup_progress onto a hardcoded
    # ladder of step names and then built an endpoint by string concatenation
    # ("onboarding." + current_step). That produced a BuildError -- a 500 --
    # for any step name without a matching route, which is exactly what
    # happened once the services step was removed from onboarding.
    #
    # The next stage is now resolved to a real registered endpoint in Python,
    # so the template can never be asked to build a route that does not exist.
    status = stage_status(user)
    completion_percentage = progress_percent(user)
    next_stage = next_incomplete_stage(user)

    return render_template("onboarding.html",
                         onboarding_state=onboarding_state,
                         completion_percentage=completion_percentage,
                         stages=STAGES,
                         status=status,
                         next_stage=next_stage,
                         current_step=(next_stage or {}).get("key", "review"))


@onboarding_bp.route("/onboarding/business", methods=["GET", "POST"])
@login_required
def onboarding_business():
    """Business identity. Belongs to the OWNER, not the location.

    A business has one legal identity regardless of how many locations it
    later operates, so legal name, CIPC number, trading name and business
    email are stored on `owners`.

    Deliberately NOT collected here:
      * VAT number -- billing information, captured at the paywall. Collecting
        it twice invites the two copies disagreeing.
      * owner personal details -- the person completing onboarding may well be
        reception rather than the owner, so no personal profile is built.
      * phone, website, description, timezone/currency/language -- none are
        needed to operate the Service.
    """
    user = current_user()
    owner_id = user.get("owner_id")
    if not owner_id:
        flash("Owner account context is required.", "error")
        return redirect(url_for("auth.logout"))

    owner = query_db(
        """
        SELECT id, legal_name, business_registration_number, trading_name, business_email, email
        FROM owners WHERE id=%s
        """,
        (owner_id,), one=True,
    ) or {}

    def _render(form, errors=None):
        return render_template(
            "onboarding_business.html",
            form=form, errors=errors or {},
            stages=STAGES, status=stage_status(user), progress=progress_percent(user),
        )

    if request.method == "GET":
        return _render({
            "legal_name": owner.get("legal_name") or "",
            "business_registration_number": owner.get("business_registration_number") or "",
            "trading_name": owner.get("trading_name") or "",
            "business_email": owner.get("business_email") or owner.get("email") or "",
        })

    form = {key: (request.form.get(key) or "").strip() for key in
            ("legal_name", "business_registration_number", "trading_name", "business_email")}
    errors = {}

    if not form["legal_name"]:
        errors["legal_name"] = "Enter the registered name of the business."
    if not form["trading_name"]:
        errors["trading_name"] = "Enter the trading name. This is the name your customers see."

    ok, normalised_cipc, cipc_error = validate_cipc(form["business_registration_number"])
    if not ok:
        errors["business_registration_number"] = cipc_error
    else:
        form["business_registration_number"] = normalised_cipc

    email = form["business_email"]
    if not email:
        errors["business_email"] = "Enter the business email address."
    elif "@" not in email or "." not in email.split("@")[-1]:
        errors["business_email"] = "Enter a valid email address."

    if errors:
        return _render(form, errors)

    execute_db(
        """
        UPDATE owners
        SET legal_name=%s, business_registration_number=%s, trading_name=%s,
            business_email=%s, updated_at=%s
        WHERE id=%s
        """,
        (form["legal_name"], form["business_registration_number"], form["trading_name"],
         form["business_email"], utc_now(), owner_id),
    )

    # The shop name is the trading name: the registered name is frequently not
    # what is on the door, and the trading name is what appears on a customer's
    # WhatsApp message. The location name is kept in step with it, but remains
    # editable in the workshop step for a branch trading under its own name.
    location_id = user.get("location_id")
    if location_id:
        existing_name = query_db("SELECT name FROM locations WHERE id=%s", (location_id,), one=True)
        if not (existing_name or {}).get("name"):
            execute_db(
                "UPDATE locations SET name=%s, updated_at=%s WHERE id=%s",
                (form["trading_name"], utc_now(), location_id),
            )

    flash("Business information saved.", "success")
    return redirect(url_for("onboarding.onboarding_workshop"))


DAY_KEYS = ("monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday")
WEEKDAY_KEYS = DAY_KEYS[:5]


def _parse_operating_hours(form):
    """Build the operating-hours structure from the workshop form.

    PHANTA does not use appointment time slots -- this records WHEN the
    workshop is open, nothing more.

    Weekdays are submitted as a single Mon-Fri pair, because a workshop that
    keeps different hours on a Wednesday is rare enough not to justify five
    rows of inputs during signup. Saturday and Sunday are independent and
    default to CLOSED: not working weekends is normal and must be recordable
    as a fact rather than as missing information.
    """
    hours, errors = {}, {}

    weekday_closed = form.get("weekday_closed") == "on"
    weekday_open = (form.get("weekday_open") or "").strip()
    weekday_close = (form.get("weekday_close") or "").strip()

    if weekday_closed:
        errors["weekday"] = "The workshop must be open on at least one weekday."
    elif not weekday_open or not weekday_close:
        errors["weekday"] = "Enter the weekday opening and closing times."
    elif weekday_close <= weekday_open:
        errors["weekday"] = "The weekday closing time must be after the opening time."

    for day in WEEKDAY_KEYS:
        hours[day] = ({"closed": True} if weekday_closed
                      else {"closed": False, "open": weekday_open, "close": weekday_close})

    for day in ("saturday", "sunday"):
        closed = form.get(f"{day}_closed") == "on"
        open_at = (form.get(f"{day}_open") or "").strip()
        close_at = (form.get(f"{day}_close") or "").strip()
        if closed or (not open_at and not close_at):
            hours[day] = {"closed": True}
            continue
        if not open_at or not close_at:
            errors[day] = f"Enter both an opening and a closing time for {day.title()}, or mark it closed."
            hours[day] = {"closed": True}
            continue
        if close_at <= open_at:
            errors[day] = f"The {day.title()} closing time must be after the opening time."
            hours[day] = {"closed": True}
            continue
        hours[day] = {"closed": False, "open": open_at, "close": close_at}

    return hours, errors


@onboarding_bp.route("/onboarding/workshop", methods=["GET", "POST"])
@login_required
def onboarding_workshop():
    """Workshop operating detail. Belongs to the LOCATION.

    Collects the name, address and operating hours. Services are deliberately
    NOT collected here -- they are configured later in settings, so onboarding
    is not blocked on constructing a service catalogue, and no pricing matrix
    is forced on the customer during signup.
    """
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    location_id = user.get("location_id")
    owner_id = user.get("owner_id")

    location = query_db(
        """
        SELECT name, physical_address, city, province, postal_code, operating_hours_json
        FROM locations WHERE id=%s
        """,
        (location_id,), one=True,
    ) or {}

    def _render(form, errors=None):
        return render_template(
            "onboarding_workshop.html",
            form=form, errors=errors or {},
            stages=STAGES, status=stage_status(user), progress=progress_percent(user),
        )

    if request.method == "GET":
        try:
            hours = json.loads(location.get("operating_hours_json") or "{}")
        except (ValueError, TypeError):
            hours = {}
        monday = hours.get("monday") or {}
        saturday = hours.get("saturday") or {}
        sunday = hours.get("sunday") or {}

        name = location.get("name") or ""
        if not name and owner_id:
            owner = query_db("SELECT trading_name FROM owners WHERE id=%s", (owner_id,), one=True) or {}
            name = owner.get("trading_name") or ""

        return _render({
            "name": name,
            "physical_address": location.get("physical_address") or "",
            "city": location.get("city") or "",
            "province": location.get("province") or "",
            "postal_code": location.get("postal_code") or "",
            "weekday_open": monday.get("open", "08:00"),
            "weekday_close": monday.get("close", "17:00"),
            "weekday_closed": bool(monday.get("closed")),
            "saturday_open": saturday.get("open", ""),
            "saturday_close": saturday.get("close", ""),
            "saturday_closed": saturday.get("closed", True),
            "sunday_open": sunday.get("open", ""),
            "sunday_close": sunday.get("close", ""),
            "sunday_closed": sunday.get("closed", True),
        })

    form = dict(request.form)
    errors = {}

    name = (request.form.get("name") or "").strip()
    address = (request.form.get("physical_address") or "").strip()
    city = (request.form.get("city") or "").strip()
    province = (request.form.get("province") or "").strip()
    postal_code = (request.form.get("postal_code") or "").strip()

    if not name:
        errors["name"] = "Enter the workshop name."
    if not address:
        errors["physical_address"] = "Enter the street address."
    if not city:
        errors["city"] = "Enter the city or town."
    if not province:
        errors["province"] = "Select the province."

    hours, hour_errors = _parse_operating_hours(request.form)
    errors.update(hour_errors)

    if errors:
        return _render(form, errors)

    execute_db(
        """
        UPDATE locations
        SET name=%s, physical_address=%s, city=%s, province=%s, postal_code=%s,
            operating_hours_json=%s, updated_at=%s
        WHERE id=%s
        """,
        (name, address, city, province, postal_code or None,
         json.dumps(hours, separators=(",", ":")), utc_now(), location_id),
    )

    flash("Workshop details saved.", "success")
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
    # Team is the last location-scoped step; legal acceptance follows so the
    # customer confirms the documents after seeing what they have configured.
    return redirect(url_for("onboarding.onboarding_legal"))


@onboarding_bp.route("/onboarding/legal", methods=["GET", "POST"])
@login_required
def onboarding_legal():
    """Per-document legal acceptance.

    Each document is confirmed separately (§16 of the onboarding brief) rather
    than with one blanket checkbox, and each acceptance is recorded as its own
    row against the exact version shown. When a document's version is later
    bumped, only that document becomes outstanding -- the customer is not made
    to re-accept four unrelated documents.

    The documents themselves are opened in a modal and served in full by
    legal_document(); they are never truncated or inlined into this page.

    Acceptance belongs to the BUSINESS (owner), not to the branch or to
    whichever staff member happened to click.
    """
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    owner_id = user.get("owner_id")
    location_id = user.get("location_id")

    accepted = accepted_documents(user.get("id"), location_id, owner_id=owner_id)

    documents = [{
        "key": key,
        "label": DOCUMENT_LABELS.get(key, key),
        "version": REQUIRED_DOCUMENTS[key],
        "accepted": accepted.get(key) == REQUIRED_DOCUMENTS[key],
        "previously_accepted_version": accepted.get(key),
    } for key in DOCUMENT_ORDER]

    def _render(errors=None):
        return render_template(
            "onboarding_legal.html",
            documents=documents, errors=errors or [],
            stages=STAGES, status=stage_status(user), progress=progress_percent(user),
        )

    if request.method == "GET":
        return _render()

    # Never auto-tick: each confirmation must be an active choice.
    missing = []
    for document in documents:
        if document["accepted"]:
            continue
        if request.form.get(f"confirm_{document['key']}") != "on":
            missing.append(document["label"])
            continue
        record_acceptance(
            document_key=document["key"],
            version=document["version"],
            user_id=user.get("id"),
            location_id=location_id,
            owner_id=owner_id,
            method=METHOD_ONBOARDING_CHECKBOX,
            ip_address=request.remote_addr,
            user_agent=request.headers.get("User-Agent"),
        )

    if missing:
        accepted = accepted_documents(user.get("id"), location_id, owner_id=owner_id)
        for document in documents:
            document["accepted"] = accepted.get(document["key"]) == document["version"]
        return _render([f"Please confirm the {label}." for label in missing])

    flash("Legal documents confirmed.", "success")
    return redirect(url_for("onboarding.onboarding_review"))


@onboarding_bp.route("/onboarding/legal/<document_key>")
@login_required
def legal_document(document_key):
    """Serve one legal document in full for the acceptance modal.

    Returned as JSON so the modal can present it scrollable and complete. The
    version is returned alongside the text so the page confirms the same
    version it is about to record.
    """
    if document_key not in REQUIRED_DOCUMENTS:
        return jsonify({"error": "unknown document"}), 404
    try:
        text = document_text(document_key)
    except (LookupError, OSError):
        logger.exception("legal_document_unavailable key=%s", document_key)
        return jsonify({"error": "document unavailable"}), 503
    return jsonify({
        "key": document_key,
        "label": DOCUMENT_LABELS.get(document_key, document_key),
        "version": REQUIRED_DOCUMENTS[document_key],
        "text": text,
    })


@onboarding_bp.route("/onboarding/review")
@login_required
def onboarding_review():
    """Final review before completion."""
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    owner_id = user.get("owner_id")
    location_id = user.get("location_id")

    owner = query_db(
        """
        SELECT legal_name, business_registration_number, trading_name, business_email
        FROM owners WHERE id=%s
        """,
        (owner_id,), one=True,
    ) or {}
    location = query_db(
        """
        SELECT name, physical_address, city, province, postal_code, operating_hours_json
        FROM locations WHERE id=%s
        """,
        (location_id,), one=True,
    ) or {}

    try:
        hours = json.loads(location.get("operating_hours_json") or "{}")
    except (ValueError, TypeError):
        hours = {}

    status = stage_status(user)
    return render_template(
        "onboarding_review.html",
        owner=owner, location=location, hours=hours,
        stages=STAGES, status=status, progress=progress_percent(user),
        outstanding=required_outstanding(user),
        can_complete=is_onboarding_complete(user),
    )


@onboarding_bp.route("/onboarding/complete", methods=["POST"])
@login_required
def onboarding_complete():
    """Finish onboarding.

    Completion is verified SERVER-SIDE against the actual data, not inferred
    from the customer having reached this page. Every required stage is
    re-derived here; reaching the final screen proves nothing on its own.

    WhatsApp and Flyer Lady are intentionally not required: a customer may
    skip them and connect later, and the account is marked as having no
    messaging channel rather than being blocked from finishing.
    """
    inactive_redirect = active_location_required()
    if inactive_redirect:
        return inactive_redirect

    user = current_user()
    outstanding = required_outstanding(user)
    if outstanding:
        labels = {stage["key"]: stage["label"] for stage in STAGES}
        flash(
            "Please complete these steps before finishing: "
            + ", ".join(labels.get(key, key) for key in outstanding),
            "error",
        )
        return redirect(url_for("onboarding.onboarding_review"))

    location_id = user.get("location_id")
    _update_onboarding_state(location_id, setup_progress=100, go_live_ready=True)

    record_audit(
        "onboarding.completed", "location", entity_id=location_id,
        actor_user=user, location_id=location_id, user_id=user.get("id"),
        details={"whatsapp_connected": stage_status(user).get("whatsapp")},
    )

    flash("Onboarding complete. Welcome to PHANTA.", "success")
    return redirect(url_for("workshop_dashboard.workshop_dashboard"))
