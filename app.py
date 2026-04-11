from flask import Flask, render_template, request, redirect, session
import psycopg2
import psycopg2.extras
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev-key")


# ================= DATABASE =================
def get_db():
    DATABASE_URL = os.environ.get("DATABASE_URL")
    return psycopg2.connect(DATABASE_URL)


def query_db(query, args=(), one=False):
    conn = get_db()
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    cur.execute(query, args)

    if query.strip().lower().startswith("select"):
        result = cur.fetchall()
    else:
        conn.commit()
        result = None

    cur.close()
    conn.close()

    return (result[0] if result else None) if one else result


# ================= SECURITY =================
def require_login():
    return "username" in session


def filter_company(data):
    if session.get("role") == "super_admin":
        return data
    return [d for d in data if d.get("company") == session.get("company")]


# ================= HELPERS =================
def process_bookings(bookings):
    today = datetime.now()
    phone_count = {}

    for b in bookings:
        phone = str(b.get("phone", ""))
        phone_count[phone] = phone_count.get(phone, 0) + 1

    processed = []

    for b in bookings:
        b = dict(b)

        phone = str(b.get("phone", ""))
        b["visit_count"] = phone_count.get(phone, 1)
        b["repeat"] = b["visit_count"] > 1

        b["reminder_type"] = None
        try:
            job_date = datetime.strptime(str(b.get("date")), "%Y-%m-%d")
            days = (today - job_date).days

            if b.get("quote_declined") == "Yes" and 30 <= days <= 60:
                b["reminder_type"] = "Follow-Up"
            elif days >= 365:
                b["reminder_type"] = "Service"
        except:
            pass

        processed.append(b)

    return processed


# ================= LOGIN =================
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        user = query_db(
            "SELECT * FROM users WHERE username=%s AND password=%s",
            (request.form["username"], request.form["password"]),
            one=True
        )

        if user:
            session["username"] = user.get("username")
            session["branch"] = user.get("branch")
            session["role"] = user.get("role")
            session["company"] = user.get("company", "MAIN")  # ✅ FIXED

            return redirect("/dashboard")

    return render_template("login.html")


# ================= SIGNUP =================
@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        query_db("""
        INSERT INTO users (username, password, branch, role, company)
        VALUES (%s, %s, 'MAIN', 'admin', %s)
        """, (
            request.form["username"],
            request.form["password"],
            request.form["company"]
        ))

        return redirect("/")

    return render_template("signup.html")


# ================= DASHBOARD =================
@app.route("/dashboard")
def dashboard():
    if not require_login():
        return redirect("/")

    bookings = query_db("SELECT * FROM bookings ORDER BY id DESC")
    bookings = filter_company(bookings)

    if session["role"] != "admin":
        bookings = [b for b in bookings if b["branch"] == session["branch"]]

    bookings = process_bookings(bookings)

    search = request.args.get("search", "").lower()
    if search:
        bookings = [b for b in bookings if search in str(b).lower()]

    today = datetime.now().strftime("%Y-%m-%d")

    return render_template(
        "dashboard.html",
        bookings=bookings,
        total=len(bookings),
        today=len([b for b in bookings if str(b.get("date")) == today]),
        lost=len([b for b in bookings if b.get("quote_declined") == "Yes"]),
        reminder_count=len([b for b in bookings if b.get("reminder_type")]),
        branch=session["branch"]
    )


# ================= ADD BOOKING =================
@app.route("/add", methods=["GET", "POST"])
def add_booking():
    if not require_login():
        return redirect("/")

    if request.method == "POST":
        query_db("""
        INSERT INTO bookings 
        (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Pending', %s, 'Booking', %s, 'No', %s)
        """, (
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
            session["company"]
        ))

        return redirect("/dashboard")

    return render_template("add_booking.html")


# ================= WALK-IN =================
@app.route("/walkin", methods=["GET", "POST"])
def walkin():
    if not require_login():
        return redirect("/")

    if request.method == "POST":
        query_db("""
        INSERT INTO bookings 
        (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'In Progress', %s, 'Walk-in', %s, 'No', %s)
        """, (
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
            session["company"]
        ))

        return redirect("/dashboard")

    return render_template("add_walkin.html")


# ================= LOGOUT =================
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


# ================= RUN =================
if __name__ == "__main__":
    app.run(debug=True)