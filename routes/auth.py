from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
)

import logging
import os

from werkzeug.security import generate_password_hash
from database import query_db, execute_db, utc_now

from services.auth_service import (
    authenticate_user,
    logout_user,
)
from extensions import limiter

auth_bp = Blueprint("auth", __name__)

logger = logging.getLogger(__name__)


@auth_bp.route("/login", methods=["GET", "POST"])
@limiter.limit("10 per minute; 50 per hour", methods=["POST"])
def login():

    if request.method == "POST":

        username = (request.form.get("email") or request.form.get("username") or "").strip()
        password = request.form.get("password", "")

        if authenticate_user(username, password):
            return redirect(url_for("index"))

        flash("Invalid username or password", "error")

    return render_template("login.html")



@auth_bp.route("/register", methods=["GET", "POST"])
@limiter.limit("5 per hour", methods=["POST"])
def register():
    """Create the canonical PHANTA owner account.

    A new owner intentionally has no location yet. The next onboarding step
    creates the owner's single location and binds it to the authenticated
    owner.

    REGISTRATION GATE
    -----------------
    Registration was previously open to anyone: no invite, no email
    verification, no throttle. Each submission created an owner account and a
    tenant, so the endpoint could be used to create unlimited tenants.

    PHANTA onboards pilot workshops by hand, so registration now requires
    REGISTRATION_INVITE_CODE. It fails closed: if the variable is not set in a
    production deployment, registration is disabled entirely rather than left
    open. Local development without the variable set is unaffected.
    """
    invite_code = os.getenv("REGISTRATION_INVITE_CODE", "").strip()
    is_production = os.getenv("FLASK_ENV", "").lower() == "production" or bool(os.getenv("RAILWAY_ENVIRONMENT"))

    if is_production and not invite_code:
        flash("Registration is not open. Please contact PHANTA to set up an account.", "error")
        return redirect(url_for("auth.login"))

    if request.method == "POST":
        if invite_code and (request.form.get("invite_code") or "").strip() != invite_code:
            logger.warning("registration_rejected reason=invalid_invite_code")
            flash("That invitation code is not valid.", "error")
            return render_template("register.html", invite_required=bool(invite_code))

        full_name = (request.form.get("full_name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if not full_name or not email or "@" not in email:
            flash("Name and a valid email address are required.", "error")
            return render_template("register.html", invite_required=bool(invite_code))
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("register.html", invite_required=bool(invite_code))
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("register.html", invite_required=bool(invite_code))
        if query_db(
            "SELECT id FROM users WHERE lower(username)=lower(%s) OR lower(COALESCE(email,''))=lower(%s) LIMIT 1",
            (email, email), one=True,
        ):
            flash("An account with that email already exists.", "error")
            return render_template("register.html", invite_required=bool(invite_code))

        execute_db(
            """INSERT INTO users
               (username,email,password,password_hash,full_name,role,active,must_reset_password,created_at,updated_at)
               VALUES (%s,%s,%s,%s,%s,'owner',TRUE,FALSE,%s,%s)""",
            (email, email, "", generate_password_hash(password), full_name, utc_now(), utc_now()),
        )
        user = query_db(
            "SELECT id,username,email,role,owner_id,location_id FROM users WHERE lower(email)=lower(%s) LIMIT 1",
            (email,), one=True,
        )
        execute_db(
            """INSERT INTO owners (user_id,name,email,active,created_at,updated_at)
               VALUES (%s,%s,%s,TRUE,%s,%s)""",
            (user["id"], full_name, email, utc_now(), utc_now()),
        )
        owner = query_db("SELECT id FROM owners WHERE user_id=%s", (user["id"],), one=True)
        execute_db("UPDATE users SET owner_id=%s,updated_at=%s WHERE id=%s",
                   (owner["id"], utc_now(), user["id"]))

        session_user = {
            "id": user["id"], "username": user["username"], "email": user["email"],
            "role": "owner", "owner_id": owner["id"], "location_id": None,
        }
        from flask import session
        session["user"] = session_user
        flash("Owner account created. Now create your location.", "success")
        return redirect(url_for("onboarding.onboarding_location"))

    return render_template("register.html", invite_required=bool(invite_code))


@auth_bp.route("/logout", methods=["POST"])
def logout():

    logout_user()

    flash("Logged out successfully.", "success")

    return redirect(url_for("auth.login"))
