# Railway Deployment Crash Analysis

This document provides a comprehensive analysis of why the Railway deployments for "web", "automation_worker", and "scheduler" services continue to crash. Each service failure point is analyzed with specific file references, line numbers, error explanations, and solutions.

## Overview of Services

Based on `Procfile` and `RAILWAY_DEPLOYMENT.md`:

- **web**: `gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}`
- **automation_worker**: `python automation_worker.py` 
- **scheduler**: `python scheduler.py`
- **billing** (cron): `python cron_jobs.py billing`

## Common Failure Points Across All Services

### 1. Environment Variable Validation Failures

**File**: `app.py` (lines 98-128)
**Critical Issue**: The `validate_startup_environment()` function is called at module level (line 128) before any routes are defined.

**Failure Scenario**: 
- Missing `DATABASE_URL` or `SECRET_KEY` environment variables
- Results in: `RuntimeError: Missing required environment variables: DATABASE_URL, SECRET_KEY`
- Crash occurs immediately at import time, before Gunicorn can start serving

**Lines of Interest**:
- Line 118: `configure_database_url_from_railway_env()`
- Lines 119-122: Critical variable check for `DATABASE_URL` and `SECRET_KEY`
- Line 128: Function call that triggers validation at import time

**Fix**: Ensure both `DATABASE_URL` and `SECRET_KEY` are set in Railway service variables.

### 2. Database Initialization Failures

**File**: `database.py` (lines 2089-2112)
**Critical Issue**: `initialize_database()` is called by all services at startup and can fail at multiple points.

**Failure Scenarios**:

**A. PostgreSQL Connection Issues** (lines 91-103)
- Missing PostgreSQL driver (`psycopg2-binary` not installed)
- Invalid `DATABASE_URL` format
- Railway Postgres service not properly linked
- Network/firewall blocking database connection
- **Error**: `OperationalError: could not connect to server` or `ImportError: No module named 'psycopg2'`

**B. Alembic Migration Failures** (lines 232-249 in `run_alembic_migrations()`)
- Missing `alembic.ini` or migration scripts
- Database schema mismatch
- Migration permission issues
- **Error**: `ProgrammingError: relation "alembic_version" does not exist` or `FileNotFoundError`

**C. SQLite Fallback Issues** (lines 152-154)
- Unable to create/write to `database.db` file
- Permission issues in container filesystem
- **Error**: `OperationalError: unable to open database file`

**Lines of Interest**:
- Line 2090: `connection, backend = get_connection()`
- Line 2092: `_create_tables(connection, backend)`
- Line 2095: `run_alembic_migrations()` (PostgreSQL only)
- Line 2110: Return statement that may not be reached if earlier steps fail

### 3. Import/Dependency Issues

**Critical Pattern**: All service worker files import from local modules at the top level:
- `automation_worker.py`: Lines 4-5 (`from automation_engine import process_due_jobs`, `from database import initialize_database`)
- `scheduler.py`: Lines 3-5 (multiple imports from `cron_jobs`, `database`, `platform_messaging`)
- `cron_jobs.py`: Lines 1-15 (multiple imports)

**Failure Scenarios**:
- Missing dependencies in `requirements.txt`
- Import errors due to syntax issues in dependent modules
- Circular import problems
- **Error**: `ModuleNotFoundError: No module named 'X'` or `ImportError: cannot import name 'Y'`

### 4. Railway-Specific Configuration Issues

**File**: `railway.json` (lines 1-6)
**Issue**: Only defines start command for web service, but Railway expects each service to have its own configuration.

**Failure Scenario**:
- Worker/scheduler/billing services not properly configured in Railway dashboard
- Services using wrong start commands or environment variables
- **Error**: Service crashes immediately due to missing configuration

## Service-Specific Analysis

### Web Service Failures

**Primary Files**: `app.py`, `database.py`, `platform_helpers.py`, `platform_messaging.py`

**Additional Failure Points**:

**A. Meta/WhatsApp Integration Issues** (app.py lines 20-69)
- Missing Meta API credentials cause imports to fail silently but break functionality
- **Error**: May appear as webhook validation failures rather than startup crashes

**B. Paystack Integration Issues** (app.py line 92)
- Missing Paystack configuration causes import failures
- **Error**: `ImportError: cannot import name 'claim_webhook_event' from 'services.paystack'`

**C. Rate Limiting Issues** (app.py line 161)
- Missing `RATELIMIT_STORAGE_URI` in Redis environments
- **Error**: `redis.exceptions.ConnectionError` if using Redis storage

**D. CSRF Protection** (app.py line 150)
- Missing `SECRET_KEY` causes CSRF initialization to fail
- **Error**: Already covered in environment validation

### Automation Worker Failures

**Primary File**: `automation_worker.py` (lines 4-5, 9-17)

**Specific Failure Points**:

**A. Database Initialization** (line 9)
- Same as common database failures above
- **Critical**: Worker crashes immediately if DB init fails

**B. Automation Engine Import** (line 4)
- **Error**: `ModuleNotFoundError: No module named 'automation_engine'` if file missing or renamed
- **Error**: Import errors from `automation_engine.py` dependencies

**C. Infinite Loop Issues** (lines 13-17)
- While loop without proper error handling
- **Error**: Unhandled exception in `process_due_jobs()` breaks worker loop silently

### Scheduler Failures

**Primary File**: `scheduler.py` (lines 3-5, 10-42)

**Specific Failure Points**:

**A. Database Initialization** (line 11)
- Same common database failures

**B. Cron Jobs Import** (line 4)
- **Error**: Import failures from `cron_jobs.py` or its dependencies

**C. Time Zone Issues** (line 5)
- **Error**: `ImportError: cannot import name 'sast_now'` from `platform_messaging`
- **Root Cause**: Missing `pytz` or `zoneinfo` dependency

**D. Sleep Interval** (line 42)
- Generally safe, but if `sast_now()` fails, loop breaks

## Step-by-Step Diagnostic Procedure

To identify the exact cause of crashes, follow this procedure:

### 1. Check Railway Service Logs
- In Railway dashboard, view logs for each failing service
- Look for the first error that appears (usually the root cause)

### 2. Verify Environment Variables
For each service, ensure these are set:
- **Web Service**: All variables from `deployment_check.py` plus `PORT`
- **Worker/Scheduler**: At minimum `DATABASE_URL` and `SECRET_KEY`
- **Billing**: Same as worker/scheduler

Use the `deployment_check.py` script locally to validate:
```bash
DATABASE_URL=your_url SECRET_KEY=your_key python deployment_check.py
```

### 3. Test Database Connectivity
Manual test using Python:
```python
from database import initialize_database
try:
    state = initialize_database()
    print(f"Database OK: {state}")
except Exception as e:
    print(f"Database failed: {e}")
```

### 4. Check Dependency Installation
Verify all `requirements.txt` packages are installed:
```bash
pip list | grep -E "(psycopg2|Flask|gunicorn|alembic)"
```

### 5. Service-Specific Tests

**Web Service**:
```bash
# Test import
python -c "from app import app; print('App imports OK')"

# Test health endpoint simulation
python -c "
from app import app
with app.test_client() as client:
    resp = client.get('/health')
    print(f'Health check: {resp.status_code}')"
```

**Automation Worker**:
```bash
python -c "
from automation_worker import run_worker
print('Worker imports OK')
# Test one iteration (will run forever, so Ctrl+C)
import threading
timer = threading.Timer(2.0, lambda: (_ for _ in ()).throw(KeyboardError))
timer.start()
try:
    run_worker()
except KeyboardError:
    print('Worker loop tested OK')
"
```

**Scheduler**:
```bash
python -c "
from scheduler import run_scheduler
print('Scheduler imports OK')
"
```

## Most Likely Causes & Solutions

### 🔴 **HIGH PROBABILITY**: Missing Critical Environment Variables
**Evidence**: 
- `validate_startup_environment()` runs at import time in `app.py`
- Will crash web service immediately if `DATABASE_URL` or `SECRET_KEY` missing
- Worker/scheduler services also call `initialize_database()` which requires DB config

**Solution**:
1. In Railway dashboard, for each service (web, worker, scheduler, billing):
2. Add переменные:
   - `DATABASE_URL` (from Railway Postgres service)
   - `SECRET_KEY` (generate strong random string)
3. For full functionality, also add variables from `deployment_check.py`

### 🟡 **MEDIUM PROBABILITY**: PostgreSQL Connection/Driver Issues
**Evidence**:
- Complex database initialization logic in `database.py`
- Multiple failure points in connection pooling and migration
- Common in containerized deployments

**Solution**:
1. Verify `psycopg2-binary` is in `requirements.txt` (it is, line 5)
2. Check Railway Postgres service is correctly linked
3. Test connection manually using `_database_url()` function
4. Consider adding `PGSSLMODE=require` if needed for Railway Postgres

### 🟡 **MEDIUM PROBABILITY**: Alembic Migration Issues
**Evidence**:
- Migration runs automatically on PostgreSQL backend (line 2095)
- Failure prevents table creation if migrations fail

**Solution**:
1. Check if `SKIP_ALEMBIC_MIGRATIONS` is accidentally set to `true`
2. Verify migration scripts exist in `database/migrations/versions/`
3. Try running migrations manually:
   ```bash
   alembic upgrade head
   ```
4. Consider setting `STRICT_ALEMBIC_MIGRATIONS=false` to allow startup despite migration warnings

### 🔴 **HIGH PROBABILITY FOR WORKER/SCHEDULER**: Missing Service-Specific Config
**Evidence**:
- `railway.json` only configures web service
- Worker/scheduler/billing require proper Railway service setup

**Solution**:
1. In Railway dashboard, create separate services for:
   - worker: `python automation_worker.py`
   - scheduler: `python scheduler.py`  
   - billing: `python cron_jobs.py billing` (or use Railway Cron)
2. Ensure each service gets the same environment variables as web
3. Verify services are set to "Always On" (not development/deploy-only)

### 🟢 **LOWER PROBABILITY BUT STILL POSSIBLE**: Import/Dependency Errors
**Evidence**:
- Complex import chains across multiple custom modules
- Services import from shared modules at top level

**Solution**:
1. Run `pip install -r requirements.txt` to verify all deps install
2. Try importing each service module individually to isolate failures
3. Check for syntax errors in Python files (`python -m py_compile <file>`)

## Recommended Immediate Actions

1. **Verify Core Environment Variables**:
   ```bash
   # In Railway dashboard for each service:
   DATABASE_URL=[from Postgres service]
   SECRET_KEY=[generate with: openssl rand -hex 32]
   ```

2. **Test Database Connection Locally**:
   ```bash
   # Copy production DATABASE_URL to local env for testing
   export DATABASE_URL=[your railway postgres url]
   export SECRET_KEY=[test key]
   python -c "from database import initialize_database; print(initialize_database())"
   ```

3. **Check Service Logs for First Error**:
   - The very first error in logs is usually the root cause
   - Look for `RuntimeError`, `ModuleNotFoundError`, `OperationalError`, `ImportError`

4. **Validate Railway Service Configuration**:
   - Each service (web, worker, scheduler, billing) must be properly configured
   - Worker/scheduler/billing often missed when only web service is set up

## Files to Reference for Troubleshooting

| File | Purpose | Key Lines for Debugging |
|------|---------|------------------------|
| `app.py` | Web application entrypoint | 98-128 (env validation), 165-176 (db init) |
| `database.py` | Database abstraction layer | 2089-2112 (initialize_database), 91-103 (PostgreSQL pool) |
| `Procfile` | Process definitions | All lines (service commands) |
| `railway.json` | Railway deploy config | 1-6 (web start command) |
| `deployment_check.py` | Validation script | 8-21 (required variables) |
| `requirements.txt` | Python dependencies | All lines |
| `automation_worker.py` | Worker service | 4-5, 9-17 |
| `scheduler.py` | Scheduler service | 3-5, 10-42 |
| `cron_jobs.py` | Billing/service jobs | 1-15, 118-162 |

## Conclusion

The most common causes of Railway deployment crashes for this application are:

1. **Missing `DATABASE_URL` or `SECRET_KEY`** - Causes immediate `RuntimeError` at import time
2. **PostgreSQL connection failures** - Due to missing driver, incorrect URL, or network issues  
3. **Improper service configuration** - Worker/scheduler/billing services not properly set up in Railway
4. **Alembic migration failures** - Preventing database schema initialization

Start troubleshooting by checking service logs for the earliest error, then verify the two critical environment variables (`DATABASE_URL` and `SECRET_KEY`) are set for ALL services, not just the web service.