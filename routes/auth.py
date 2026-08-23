from flask import (
    Blueprint,
    render_template,
    request,
    redirect,
    url_for,
    flash,
)

from werkzeug.security import generate_password_hash
from database import query_db, execute_db, utc_now

from services.auth_service import (
    authenticate_user,
    logout_user,
)

auth_bp = Blueprint("auth", __name__)


@auth_bp.route("/login", methods=["GET", "POST"])
def login():

    if request.method == "POST":

        username = (request.form.get("email") or request.form.get("username") or "").strip()
        password = request.form.get("password", "")

        if authenticate_user(username, password):
            return redirect(url_for("index"))

        flash("Invalid username or password", "error")

    return render_template("login.html")



@auth_bp.route("/register", methods=["GET", "POST"])
def register():
    """Create the canonical PHANTA owner account.

    A new owner intentionally has no location yet. The next onboarding step
    creates the owner's single location and binds it to the authenticated
    owner.
    """
    if request.method == "POST":
        full_name = (request.form.get("full_name") or "").strip()
        email = (request.form.get("email") or "").strip().lower()
        password = request.form.get("password") or ""
        confirm = request.form.get("confirm_password") or ""

        if not full_name or not email or "@" not in email:
            flash("Name and a valid email address are required.", "error")
            return render_template("register.html")
        if len(password) < 8:
            flash("Password must be at least 8 characters.", "error")
            return render_template("register.html")
        if password != confirm:
            flash("Passwords do not match.", "error")
            return render_template("register.html")
        if query_db(
            "SELECT id FROM users WHERE lower(username)=lower(%s) OR lower(COALESCE(email,''))=lower(%s) LIMIT 1",
            (email, email), one=True,
        ):
            flash("An account with that email already exists.", "error")
            return render_template("register.html")

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

    return render_template("register.html")


@auth_bp.route("/logout")
def logout():

    logout_user()

    flash("Logged out successfully.", "success")

    return redirect(url_for("auth.login"))