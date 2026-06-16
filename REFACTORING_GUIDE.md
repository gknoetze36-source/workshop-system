# REFACTORING GUIDE: app.py → Modular Blueprints

## Current State
- **app.py**: 127KB, 2000+ lines
- **All routes**: Registered directly on app instance
- **No modularity**: Single file contains 100+ route handlers
- **Problem**: Hard to test, maintain, and scale

## Target State
```
project/
├── app/
│   ├── __init__.py          # App factory
│   ├── config.py            # Configuration
│   ├── routes/
│   │   ├── __init__.py
│   │   ├── auth.py          # Login, logout, password
│   │   ├── bookings.py      # Booking management
│   │   ├── dashboard.py     # Dashboard, overview
│   │   ├── customers.py     # Customer management
│   │   ├── admin.py         # Admin/franchise/staff mgmt
│   │   ├── messaging.py     # ChatBot, messaging
│   │   ├── webhooks.py      # Paystack, Meta webhooks
│   │   ├── api.py           # API endpoints (/api/*)
│   │   └── public.py        # Public booking page
│   ├── utils.py             # Shared helpers (decorators, etc)
│   └── models.py            # Response models, serializers
├── main.py                  # Entry point
└── requirements.txt
```

## Step-by-Step Refactoring

### PHASE 1: Prepare Infrastructure (2-3 hours)

#### Step 1.1: Create app factory (`app/__init__.py`)
```python
# app/__init__.py
from flask import Flask
from flask_wtf import CSRFProtect
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address

csrf = CSRFProtect()
limiter = Limiter(key_func=get_remote_address)

def create_app(config_name="development"):
    """Application factory."""
    app = Flask(__name__)
    
    # Load config
    from app.config import config_by_name
    app.config.from_object(config_by_name[config_name])
    
    # Initialize extensions
    csrf.init_app(app)
    limiter.init_app(app)
    
    # Register blueprints
    from app.routes import (
        auth_bp, bookings_bp, dashboard_bp, customers_bp,
        admin_bp, messaging_bp, webhooks_bp, api_bp, public_bp
    )
    
    app.register_blueprint(auth_bp)
    app.register_blueprint(bookings_bp)
    app.register_blueprint(dashboard_bp)
    app.register_blueprint(customers_bp)
    app.register_blueprint(admin_bp)
    app.register_blueprint(messaging_bp)
    app.register_blueprint(webhooks_bp)
    app.register_blueprint(api_bp)
    app.register_blueprint(public_bp)
    
    # Register error handlers
    register_error_handlers(app)
    
    return app

def register_error_handlers(app):
    """Register Flask error handlers."""
    @app.errorhandler(404)
    def not_found(e):
        return render_template("404.html"), 404
    
    @app.errorhandler(403)
    def forbidden(e):
        return render_template("403.html"), 403
    
    @app.errorhandler(500)
    def server_error(e):
        return render_template("500.html"), 500
```

#### Step 1.2: Create config module (`app/config.py`)
```python
# app/config.py
import os
from datetime import timedelta

class Config:
    """Base configuration."""
    SECRET_KEY = os.environ.get("SECRET_KEY", "dev-key-change-in-prod")
    SESSION_COOKIE_SECURE = os.environ.get("SESSION_COOKIE_SECURE", "true").lower() == "true"
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = os.environ.get("SESSION_COOKIE_SAMESITE", "Lax")
    PERMANENT_SESSION_LIFETIME = timedelta(hours=12)
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max upload
    # Add more config here...

class DevelopmentConfig(Config):
    """Development configuration."""
    DEBUG = True
    TESTING = False

class ProductionConfig(Config):
    """Production configuration."""
    DEBUG = False
    TESTING = False

class TestingConfig(Config):
    """Testing configuration."""
    TESTING = True
    WTF_CSRF_ENABLED = False

config_by_name = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
}
```

#### Step 1.3: Create utilities module (`app/utils.py`)
```python
# app/utils.py
from functools import wraps
from flask import redirect, url_for, abort, session
from platform_helpers import fetch_one

def login_required(f):
    """Decorator: require login."""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if "user_id" not in session:
            return redirect(url_for("auth.login"))
        return f(*args, **kwargs)
    return decorated_function

def roles_required(*allowed_roles):
    """Decorator: require specific roles."""
    def decorator(f):
        @wraps(f)
        @login_required
        def decorated_function(*args, **kwargs):
            user = fetch_one("SELECT role FROM users WHERE id=%s", (session["user_id"],))
            if not user or user["role"] not in allowed_roles:
                abort(403)
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def current_user():
    """Get current logged-in user."""
    if "user_id" not in session:
        return None
    return fetch_one("SELECT * FROM users WHERE id=%s", (session["user_id"],))

def get_franchise():
    """Get current user's franchise."""
    user = current_user()
    if not user:
        return None
    return fetch_one("SELECT * FROM franchises WHERE id=%s", (user["franchise_id"],))
```

### PHASE 2: Migrate Authentication Routes (1-2 hours)

#### Step 2.1: Create auth blueprint (`app/routes/auth.py`)
```python
# app/routes/auth.py
from flask import Blueprint, render_template, request, redirect, url_for, session, flash
from werkzeug.security import check_password_hash, generate_password_hash
from platform_helpers import fetch_one, execute_db, utc_now
from app.utils import login_required, current_user

auth_bp = Blueprint("auth", __name__, url_prefix="")

@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    """User login page."""
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        
        user = fetch_one(
            "SELECT * FROM users WHERE lower(username)=lower(%s) AND active=1",
            (username,)
        )
        
        if not user or not check_password_hash(user["password_hash"], password):
            flash("Invalid username or password.", "error")
        elif not user.get("active"):
            flash("Account is inactive.", "error")
        else:
            session["user_id"] = user["id"]
            session.permanent = True
            return redirect(url_for("dashboard.index"))
    
    return render_template("login.html")

@auth_bp.route("/logout", methods=["GET", "POST"])
@login_required
def logout():
    """User logout."""
    session.clear()
    flash("You have been logged out.", "success")
    return redirect(url_for("auth.login"))

@auth_bp.route("/account/password", methods=["GET", "POST"])
@login_required
def change_password():
    """Change user password."""
    user = current_user()
    
    if request.method == "POST":
        current_pass = request.form.get("current_password", "")
        new_pass = request.form.get("new_password", "")
        
        if not check_password_hash(user["password_hash"], current_pass):
            flash("Current password is incorrect.", "error")
        elif len(new_pass) < 10:
            flash("New password must be at least 10 characters.", "error")
        else:
            password_hash = generate_password_hash(new_pass)
            execute_db(
                "UPDATE users SET password_hash=%s, updated_at=%s WHERE id=%s",
                (password_hash, utc_now(), user["id"])
            )
            flash("Password changed successfully.", "success")
            return redirect(url_for("auth.change_password"))
    
    return render_template("change_password.html")

@auth_bp.route("/api/auth/login", methods=["POST"])
def api_login():
    """API login endpoint."""
    # Move API auth logic here
    pass
```

#### Step 2.2: Create routes/__init__.py
```python
# app/routes/__init__.py
from .auth import auth_bp
from .bookings import bookings_bp
from .dashboard import dashboard_bp
from .customers import customers_bp
from .admin import admin_bp
from .messaging import messaging_bp
from .webhooks import webhooks_bp
from .api import api_bp
from .public import public_bp

__all__ = [
    "auth_bp",
    "bookings_bp",
    "dashboard_bp",
    "customers_bp",
    "admin_bp",
    "messaging_bp",
    "webhooks_bp",
    "api_bp",
    "public_bp",
]
```

### PHASE 3: Migrate Feature Routes (4-6 hours per feature)

#### Step 3.1: Create booking blueprint (`app/routes/bookings.py`)
```python
# app/routes/bookings.py
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.utils import login_required, roles_required, current_user
from platform_helpers import fetch_all, fetch_one, execute_db

bookings_bp = Blueprint("bookings", __name__, url_prefix="/bookings")

@bookings_bp.route("", methods=["GET"])
@login_required
def list():
    """List all bookings for user's franchise."""
    user = current_user()
    bookings = fetch_all(
        """
        SELECT b.* FROM bookings b
        WHERE b.franchise_id=%s
        ORDER BY b.scheduled_date DESC
        """,
        (user["franchise_id"],)
    )
    return render_template("bookings/list.html", bookings=bookings)

@bookings_bp.route("/<int:booking_id>", methods=["GET"])
@login_required
def view(booking_id):
    """View single booking."""
    booking = fetch_one("SELECT * FROM bookings WHERE id=%s", (booking_id,))
    if not booking:
        flash("Booking not found.", "error")
        return redirect(url_for("bookings.list"))
    return render_template("bookings/view.html", booking=booking)

@bookings_bp.route("/<int:booking_id>/edit", methods=["GET", "POST"])
@login_required
def edit(booking_id):
    """Edit booking."""
    booking = fetch_one("SELECT * FROM bookings WHERE id=%s", (booking_id,))
    if not booking:
        return redirect(url_for("bookings.list"))
    
    if request.method == "POST":
        # Update logic here
        flash("Booking updated.", "success")
        return redirect(url_for("bookings.view", booking_id=booking_id))
    
    return render_template("bookings/edit.html", booking=booking)
```

#### Step 3.2: Create dashboard blueprint (`app/routes/dashboard.py`)
```python
# app/routes/dashboard.py
from flask import Blueprint, render_template
from app.utils import login_required, current_user
from platform_helpers import fetch_all, fetch_one

dashboard_bp = Blueprint("dashboard", __name__, url_prefix="")

@dashboard_bp.route("/")
@dashboard_bp.route("/dashboard", methods=["GET"])
@login_required
def index():
    """Main dashboard."""
    user = current_user()
    # Fetch dashboard data
    return render_template("dashboard.html", user=user)
```

#### Step 3.3: Follow pattern for other routes
Create:
- `app/routes/customers.py` (customers management)
- `app/routes/admin.py` (franchises, branches, staff)
- `app/routes/messaging.py` (ChatBot, WhatsApp)
- `app/routes/webhooks.py` (Paystack, Meta)
- `app/routes/api.py` (API endpoints)
- `app/routes/public.py` (public booking page)

### PHASE 4: Update Entry Point (30 minutes)

#### Step 4.1: Create main.py
```python
# main.py
import os
from app import create_app, csrf
from database import initialize_database
from deployment_check import validate_startup_environment

# Validate environment first
validate_startup_environment()

# Initialize database
db_state = initialize_database()
print(f"✓ Database ready: {db_state['backend']}")

# Create Flask app
app = create_app(config_name=os.environ.get("ENVIRONMENT", "development"))

if __name__ == "__main__":
    # Development
    app.run(
        host="0.0.0.0",
        port=int(os.environ.get("PORT", 5000)),
        debug=os.environ.get("DEBUG", "false").lower() == "true"
    )
```

#### Step 4.2: Update gunicorn command
```bash
# Before:
gunicorn app:app

# After:
gunicorn main:app
```

### PHASE 5: Refactor Shared Helpers (1-2 hours)

#### Step 5.1: Create models.py
```python
# app/models.py
"""Response models and serializers."""

def serialize_booking(booking):
    """Convert booking row to JSON-safe dict."""
    return {
        "id": booking["id"],
        "reference": booking["booking_reference"],
        "customer_name": booking["first_name"],
        "phone": booking["phone"],
        "status": booking["status"],
        "scheduled_date": booking["scheduled_date"].isoformat() if booking["scheduled_date"] else None,
    }

def serialize_franchise(franchise):
    """Convert franchise row to JSON-safe dict."""
    return {
        "id": franchise["id"],
        "name": franchise["name"],
        "slug": franchise["slug"],
        "plan": franchise["plan_code"],
    }
```

### PHASE 6: Update Tests (1-2 hours)

#### Step 6.1: Create test structure
```python
# tests/conftest.py
import pytest
from app import create_app, csrf
from database import initialize_database

@pytest.fixture
def app():
    """Create app for testing."""
    app = create_app(config_name="testing")
    return app

@pytest.fixture
def client(app):
    """Flask test client."""
    return app.test_client()

# tests/test_auth.py
def test_login_page(client):
    """Test login page renders."""
    response = client.get("/login")
    assert response.status_code == 200
    assert b"Login" in response.data

def test_login_invalid(client):
    """Test invalid login."""
    response = client.post("/login", data={
        "username": "invalid",
        "password": "wrong"
    })
    assert b"Invalid username or password" in response.data
```

---

## Migration Checklist

### Before You Start
- [ ] Create feature branch: `git checkout -b refactor/app-blueprints`
- [ ] Backup current app.py
- [ ] Ensure all tests pass currently
- [ ] Document current route list

### Phase 1 (Infra)
- [ ] Create `app/` directory
- [ ] Create `app/__init__.py` (app factory)
- [ ] Create `app/config.py` (configuration)
- [ ] Create `app/utils.py` (decorators, helpers)
- [ ] Create `app/routes/__init__.py`
- [ ] Update `main.py` entry point
- [ ] Test: `python main.py` starts without error

### Phase 2 (Auth)
- [ ] Create `app/routes/auth.py`
- [ ] Move login, logout, password routes
- [ ] Test: `curl http://localhost:5000/login` returns 200
- [ ] Test: `curl -X POST http://localhost:5000/login` with bad creds shows error
- [ ] Run existing tests for auth

### Phase 3 (Features)
- [ ] Create `app/routes/bookings.py` + test
- [ ] Create `app/routes/dashboard.py` + test
- [ ] Create `app/routes/customers.py` + test
- [ ] Create `app/routes/admin.py` + test
- [ ] Create `app/routes/messaging.py` + test
- [ ] Create `app/routes/webhooks.py` + test
- [ ] Create `app/routes/api.py` + test
- [ ] Create `app/routes/public.py` + test

### Phase 4 (Finalize)
- [ ] Update gunicorn/deployment configs
- [ ] Run full test suite (should pass)
- [ ] Check file sizes (app/routes/* should be < 30KB each)
- [ ] Verify no duplicate route definitions
- [ ] Delete old app.py (keep backup)

### After Refactoring
- [ ] Code review with team
- [ ] Deploy to staging
- [ ] Monitor error logs (Sentry)
- [ ] Merge to main
- [ ] Deploy to production

---

## Benefits After Refactoring

| Metric | Before | After |
|--------|--------|-------|
| **app.py size** | 127 KB | <5 KB (factory only) |
| **File count** | 1 giant file | 10-12 focused modules |
| **Route per file** | 100+ | 5-12 |
| **Test isolation** | Hard | Easy |
| **Merge conflicts** | High | Low |
| **Onboarding time** | 2-3 hours | 30 min |
| **Maintainability** | ⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## Estimated Timeline

| Phase | Task | Hours | Total |
|-------|------|-------|-------|
| 1 | Infrastructure | 2-3 | 2-3 |
| 2 | Auth routes | 1-2 | 3-5 |
| 3 | Feature routes | 4-6 each | 20-30 |
| 4 | Entry point | 0.5 | 20-30 |
| 5 | Helpers | 1-2 | 21-32 |
| 6 | Tests | 1-2 | 22-34 |
| - | **Total** | - | **22-34 hours** |

**Recommended approach**: 
- 3 days sprint (8h/day) = 24 hours ✓
- Or 1 week part-time (4h/day) = 20 hours ✓

---

## Questions?

- Why Blueprints? Flask standard, clean isolation, easy testing
- Will this break existing functionality? No, routes unchanged, only organization
- How to migrate without downtime? Deploy new code, test locally first, same routes work
- Can we do this incrementally? Yes! Start with auth, then add features one by one
