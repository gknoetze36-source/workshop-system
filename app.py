from flask import Flask, render_template, request, redirect, session
import sqlite3
from datetime import datetime
import os

app = Flask(__name__)
app.secret_key = "secret123"


# ---------------- DB ----------------
def get_db():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    return sqlite3.connect(os.path.join(BASE_DIR, "database.db"))


def init_db():
    db = get_db()

    db.execute("""
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT,
        password TEXT,
        branch TEXT,
        role TEXT,
        company TEXT
    )
    """)

    db.execute("""
    CREATE TABLE IF NOT EXISTS bookings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT,
        surname TEXT,
        phone TEXT,
        make TEXT,
        model TEXT,
        service TEXT,
        date TEXT,
        branch TEXT,
        status TEXT,
        work_to_be_done TEXT,
        source TEXT,
        quote_declined TEXT,
        contacted TEXT,
        company TEXT
    )
    """)

    db.commit()
    db.close()


def get_bookings():
    db = get_db()
    cursor = db.execute("SELECT * FROM bookings")
    columns = [col[0] for col in cursor.description]
    data = [dict(zip(columns, row)) for row in cursor.fetchall()]
    db.close()
    return data


# ---------------- SECURITY ----------------
def filter_company(data):
    if session.get("role") == "admin":
        return data
    return [
        d for d in data
        if d.get("company") == session.get("company")
    ]


# ---------------- HELPERS ----------------
def add_repeat(bookings):
    count = {}

    for b in bookings:
        phone = str(b.get("phone", "")).strip()
        if phone:
            count[phone] = count.get(phone, 0) + 1

    for b in bookings:
        phone = str(b.get("phone", "")).strip()
        b["visit_count"] = count.get(phone, 1)
        b["repeat"] = b["visit_count"] > 1

    return bookings


def add_reminders(bookings):
    today = datetime.now()

    for b in bookings:
        b["reminder_type"] = None

        try:
            job_date = datetime.strptime(str(b.get("date")), "%Y-%m-%d")
        except:
            continue

        days = (today - job_date).days

        if b.get("quote_declined") == "Yes" and 30 <= days <= 60:
            b["reminder_type"] = "Follow-Up"

        if days >= 365:
            b["reminder_type"] = "Service"

    return bookings


# ---------------- LOGIN ----------------
@app.route("/", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        db = get_db()

        user = db.execute(
            "SELECT * FROM users WHERE username=? AND password=?",
            (request.form["username"], request.form["password"])
        ).fetchone()

        db.close()

        if user:
            session["username"] = user[1]
            session["branch"] = user[3]
            session["role"] = user[4]
            session["company"] = user[5]
            return redirect("/dashboard")

    return render_template("login.html")


# ---------------- SIGNUP ----------------
@app.route("/signup", methods=["GET", "POST"])
def signup():
    if request.method == "POST":
        db = get_db()

        db.execute("""
        INSERT INTO users (username, password, branch, role, company)
        VALUES (?, ?, ?, 'admin', ?)
        """, (
            request.form["username"],
            request.form["password"],
            "MAIN",
            request.form["company"]
        ))

        db.commit()
        db.close()

        return redirect("/")

    return render_template("signup.html")


# ---------------- DASHBOARD ----------------
@app.route("/dashboard")
def dashboard():
    if "username" not in session:
        return redirect("/")

    bookings = filter_company(get_bookings())

    if session["role"] != "admin":
        bookings = [
            b for b in bookings
            if b.get("branch") == session.get("branch")
        ]

    bookings = add_repeat(bookings)
    bookings = add_reminders(bookings)

    search = request.args.get("search", "").lower()
    if search:
        bookings = [b for b in bookings if search in str(b).lower()]

    total = len(bookings)

    today = datetime.now().strftime("%Y-%m-%d")
    today_count = [b for b in bookings if str(b.get("date")) == today]

    lost = [b for b in bookings if b.get("quote_declined") == "Yes"]
    reminders = [b for b in bookings if b.get("reminder_type")]

    return render_template(
        "dashboard.html",
        bookings=bookings,
        total=total,
        today=len(today_count),
        lost=len(lost),
        reminder_count=len(reminders),
        branch=session["branch"]
    )


# ---------------- ADD BOOKING ----------------
@app.route("/add", methods=["GET", "POST"])
def add_booking():
    if "username" not in session:
        return redirect("/")

    if request.method == "POST":
        db = get_db()

        db.execute("""
        INSERT INTO bookings 
        (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form.get("first_name"),
            request.form.get("surname"),
            request.form.get("phone"),
            request.form.get("make"),
            request.form.get("model"),
            request.form.get("service"),
            request.form.get("date"),
            session.get("branch"),
            "Pending",
            request.form.get("work"),
            "Booking",
            request.form.get("quote_declined"),
            "No",
            session.get("company")
        ))

        db.commit()
        db.close()

        return redirect("/dashboard")

    return render_template("add_booking.html")


# ---------------- WALK-IN ----------------
@app.route("/walkin", methods=["GET", "POST"])
def walkin():
    if "username" not in session:
        return redirect("/")

    if request.method == "POST":
        db = get_db()

        db.execute("""
        INSERT INTO bookings 
        (first_name, surname, phone, make, model, service, date, branch, status, work_to_be_done, source, quote_declined, contacted, company)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            request.form.get("first_name"),
            request.form.get("surname"),
            request.form.get("phone"),
            request.form.get("make"),
            request.form.get("model"),
            request.form.get("service"),
            datetime.now().strftime("%Y-%m-%d"),
            session.get("branch"),
            "In Progress",
            request.form.get("work"),
            "Walk-in",
            request.form.get("quote_declined"),
            "No",
            session.get("company")
        ))

        db.commit()
        db.close()

        return redirect("/dashboard")

    return render_template("add_walkin.html")


# ---------------- LOGOUT ----------------
@app.route("/logout")
def logout():
    session.clear()
    return redirect("/")


# ---------------- INIT ----------------
init_db()

if __name__ == "__main__":
    app.run()
