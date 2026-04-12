from datetime import datetime
import os

from flask import Flask, redirect, render_template, request, session

from database import initialize_database, query_db

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-key")

DATABASE_INIT_ERROR = None

try:
    initialize_database()
except Exception as exc:
    DATABASE_INIT_ERROR = exc
    if os.environ.get("DATABASE_URL"):
        raise


DATE_FORMATS = ("%Y-%m-%d", "%m/%d/%Y", "%m/%d/%y")


def require_login():
    return "username" in session


def local_database_unavailable():
    return DATABASE_INIT_ERROR is not None and not os.environ.get("DATABASE_URL")


def parse_job_date(value):
    text = str(value or "").strip()
    if not text:
        return None

    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def filter_company(rows):
    if session.get("role") == "super_admin":
        return rows

    company = session.get("company", "MAIN")
    return [row for row in rows if (row.get("company") or "MAIN") == company]


def can_access_booking(booking):
    if not booking:
        return False

    booking_company = booking.get("company") or "MAIN"
    if session.get("role") != "super_admin" and booking_company != session.get("company", "MAIN"):
        return False

    if session.get("role") not in {"admin", "super_admin"}:
        return booking.get("branch") == session.get("branch")

    return True


def process_bookings(bookings):
    today = datetime.now()
    phone_count = {}

    for booking in bookings:
        phone = str(booking.get("phone") or "").strip()
        if not phone:
            continue
        phone_count[phone] = phone_count.get(phone, 0) + 1

    processed = []
    for booking in bookings:
        item = dict(booking)
        phone = str(item.get("phone") or "").strip()
        item["visit_count"] = phone_count.get(phone, 1) if phone else 1
        item["repeat"] = item["visit_count"] > 1
        item["reminder_type"] = None

        job_date = parse_job_date(item.get("date"))
        if job_date:
            days_since = (today - job_date).days
            if item.get("quote_declined") == "Yes" and 30 <= days_since <= 60:
                item["reminder_type"] = "Follow-Up"
            elif days_since >= 365:
                item["reminder_type"] = "Service"

        processed.append(item)

    return processed


def visible_bookings():
    bookings = query_db("SELECT * FROM bookings ORDER BY id DESC") or []
    bookings = filter_company(bookings)

    if session.get("role") not in {"admin", "super_admin"}:
        bookings = [booking for booking in bookings if booking.get("branch") == session.get("branch")]

    return process_bookings(bookings)


def booking_matches_search(booking, term):
    haystack = " ".join(str(value or "") for value in booking.values()).lower()
    return term in haystack


def selected_branch():
    return session.get("branch", "Dashboard")


@app.route("/", methods=["GET", "POST"])
def login():
    if local_database_unavailable():
        return render_template(
            "login.html",
            error="Local database is unavailable. Set SQLITE_PATH to a writable path or use DATABASE_URL.",
        )

    error = None
    if request.method == "POST":
        user = query_db(
            "SELECT * FROM users WHERE username=%s AND password=%s",
            (request.form["username"], request.form["password"]),
            one=True,
        )

        if user:
            session["username"] = user.get("username")
            session["branch"] = user.get("branch") or "MAIN"
            session["role"] = user.get("role") or "staff"
            session["company"] = user.get("company") or "MAIN"
            return redirect("/dashboard")

        error = "Invalid username or password."

    return render_template("login.html", error=error)


@app.route("/signup", methods=["GET", "POST"])
def signup():
    if local_database_unavailable():
        return render_template(
            "signup.html",
            error="Local database is unavailable. Set SQLITE_PATH to a writable path or use DATABASE_URL.",
        )

    error = None
    if request.method == "POST":
        try:
            query_db(
                """
                INSERT INTO users (username, password, branch, role, company)
                VALUES (%s, %s, 'MAIN', 'admin', %s)
                """,
                (
                    request.form["username"],
                    request.form["password"],
                    request.form["company"],
                ),
            )
            return redirect("/")
        except Exception:
            error = "That username already exists."

    return render_template("signup.html", error=error)


@app.route("/dashboard")
def dashboard():
    if not require_login():
        return redirect("/")

    bookings = visible_bookings()
    search = request.args.get("search", "").strip().lower()
    if search:
        bookings = [booking for booking in bookings if booking_matches_search(booking, search)]

    today = datetime.now().strftime("%Y-%m-%d")
    return render_template(
        "dashboard.html",
        bookings=bookings,
        total=len(bookings),
        today=len([booking for booking in bookings if str(booking.get("date")) == today]),
        lost=len([booking for booking in bookings if booking.get("quote_declined") == "Yes"]),
        reminder_count=len([booking for booking in bookings if booking.get("reminder_type")]),
        branch=selected_branch(),
    )


@app.route("/planner")
def planner():
    if not require_login():
        return redirect("/")

    selected_date = request.args.get("date") or datetime.now().strftime("%Y-%m-%d")
    bookings = []
    for booking in visible_bookings():
        job_date = parse_job_date(booking.get("date"))
        if job_date and job_date.strftime("%Y-%m-%d") == selected_date:
            bookings.append(booking)

    return render_template(
        "Planner.html",
        bookings=bookings,
        selected_date=selected_date,
        branch=selected_branch(),
    )


@app.route("/customers")
def customers():
    if not require_login():
        return redirect("/")

    customer_map = {}
    for booking in visible_bookings():
        key = str(booking.get("phone") or f"customer-{booking.get('id')}")
        customer_map.setdefault(
            key,
            {
                "first_name": booking.get("first_name") or "",
                "surname": booking.get("surname") or "",
                "phone": booking.get("phone") or "",
            },
        )

    customers_list = sorted(
        customer_map.values(),
        key=lambda item: (item["first_name"].lower(), item["surname"].lower(), item["phone"]),
    )
    return render_template("Customers.html", customers=customers_list, branch=selected_branch())


@app.route("/customer/<phone>")
def customer_history(phone):
    if not require_login():
        return redirect("/")

    bookings = [booking for booking in visible_bookings() if str(booking.get("phone") or "") == phone]
    customer_name = phone
    if bookings:
        customer_name = f"{bookings[0].get('first_name', '')} {bookings[0].get('surname', '')}".strip() or phone

    return render_template(
        "Customer History.html",
        bookings=bookings,
        customer_name=customer_name,
        branch=selected_branch(),
    )


@app.route("/reminders")
def reminders():
    if not require_login():
        return redirect("/")

    bookings = [booking for booking in visible_bookings() if booking.get("reminder_type")]
    return render_template("reminders.html", bookings=bookings, branch=selected_branch())


@app.route("/lost")
def lost_work():
    if not require_login():
        return redirect("/")

    bookings = [booking for booking in visible_bookings() if booking.get("quote_declined") == "Yes"]
    return render_template("lost.html", bookings=bookings, branch=selected_branch())


@app.route("/reports")
def reports():
    if not require_login():
        return redirect("/")

    bookings = visible_bookings()
    return render_template(
        "Report.html",
        total=len(bookings),
        completed=len([booking for booking in bookings if booking.get("status") == "Done"]),
        declined=len([booking for booking in bookings if booking.get("quote_declined") == "Yes"]),
        walkins=len([booking for booking in bookings if booking.get("source") == "Walk-in"]),
        bookings=len([booking for booking in bookings if booking.get("source") == "Booking"]),
        branch=selected_branch(),
    )


@app.route("/add", methods=["GET", "POST"])
def add_booking():
    if not require_login():
        return redirect("/")

    if request.method == "POST":
        query_db(
            """
            INSERT INTO bookings
            (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Pending', %s, 'Booking', %s, 'No', %s)
            """,
            (
                request.form["first_name"],
                request.form["surname"],
                request.form["phone"],
                request.form["make"],
                request.form["model"],
                request.form["service"],
                request.form["date"],
                session["branch"],
                request.form["work"],
                request.form["quote_declined"],
                session.get("company", "MAIN"),
            ),
        )
        return redirect("/dashboard")

    return render_template("add_booking.html", branch=selected_branch())


@app.route("/walkin", methods=["GET", "POST"])
def walkin():
    if not require_login():
        return redirect("/")

    if request.method == "POST":
        query_db(
            """
            INSERT INTO bookings
            (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'In Progress', %s, 'Walk-in', %s, 'No', %s)
            """,
            (
                request.form["first_name"],
                request.form["surname"],
                request.form["phone"],
                request.form["make"],
                request.form["model"],
                request.form["service"],
                datetime.now().strftime("%Y-%m-%d"),
                session["branch"],
                request.form["work"],
                request.form["quote_declined"],
                session.get("company", "MAIN"),
            ),
        )
        return redirect("/dashboard")

    return render_template("add_walkin.html", branch=selected_branch())


@app.route("/update", methods=["POST"])
def update_booking():
    if not require_login():
        return redirect("/")

    booking_id = request.form.get("id")
    booking = query_db("SELECT * FROM bookings WHERE id=%s", (booking_id,), one=True)
    if not can_access_booking(booking):
        return redirect("/dashboard")

    updates = []
    values = []
    for field in ("status", "quote_declined"):
        if field in request.form:
            updates.append(f"{field}=%s")
            values.append(request.form[field])

    if updates:
        values.append(booking_id)
        query_db(f"UPDATE bookings SET {', '.join(updates)} WHERE id=%s", tuple(values))

    return redirect(request.referrer or "/dashboard")


@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


if __name__ == "__main__":
    app.run(debug=True)
