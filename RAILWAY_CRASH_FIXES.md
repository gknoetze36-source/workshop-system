# Railway Deployment Crash Fixes

Immediate solutions for crashing Railway services. Each fix includes the exact error, file location, line numbers, and solution.

## 🔴 CRITICAL: Missing Environment Variables (Most Common Cause)

**Error**: `RuntimeError: Missing required environment variables: DATABASE_URL, SECRET_KEY`

**Location**: `app.py`, lines 119-128
```python
missing_critical = [key for key in CRITICAL_STARTUP_ENV_VARS if not os.environ.get(key)]
if missing_critical:
    logger.error("startup_missing_critical_environment variables=%s", ",".join(missing_critical))
    raise RuntimeError(f"Missing required environment variables: {', '.join(missing_critical)}")
```

**Why it crashes**: This validation runs at module import time (line 128) before Flask starts, causing immediate crash.

**Files affected**: All services (`app.py` imported by web worker, scheduler, and cron_jobs via database initialization)

**Fix**:
1. Go to Railway dashboard → Each service (web, worker, scheduler, billing) → Settings → Variables
2. Add:
   - `DATABASE_URL`: Copy from your Railway Postgres service (Settings → Variables → DATABASE_URL)
   - `SECRET_KEY`: Generate with `openssl rand -hex 32` or use a secure random string

**Verification**: After setting, restart service. Should see `startup_environment_validated` in logs.

---

## 🔴 CRITICAL: Database Connection Failures

**Error**: `OperationalError: could not connect to server: Connection refused`  
**OR** `ImportError: No module named 'psycopg2'`

**Location**: `database.py`, lines 91-103 (PostgreSQL pool creation)
```python
_POOL = ThreadedConnectionPool(minconn, maxconn, database_url, connect_timeout=timeout)
```

**Why it crashes**: 
- Missing `psycopg2-binary` dependency
- Invalid `DATABASE_URL` format  
- Railway Postgres service not linked or paused
- Network/firewall blocking connection

**Files affected**: All services (call `initialize_database()` at startup)

**Fix**:
1. **Verify dependency**: Ensure `psycopg2-binary>=2.9,<3.0` is in `requirements.txt` (line 5)
2. **Check database URL**: In Railway dashboard, verify Postgres service is linked to each service
3. **Test connection**: 
   ```bash
   # Add to each service temporarily for debugging
   VARIABLE NAME: DEBUG_DB_CONNECT
   VARIABLE VALUE: 1
   ```
   Then check logs for successful connection message
4. **Ensure Postgres is awake**: Railway Postgres services sleep when inactive - add a wake-up ping or upgrade plan

**Alternative Fix for SQLite** (temporary debugging):
Set in service variables:
- `SKIP_DATABASE_INIT=true` (bypasses PostgreSQL requirement)
- This will use SQLite file `database.db` (not persistent across redeploys)

---

## 🔴 CRITICAL: Missing Service Configuration in Railway

**Error**: Service crashes immediately or shows "command not found"  
**OR** Service uses wrong start command

**Location**: Railway service configuration (not in code)

**Why it crashes**: 
- `railway.json` only configures web service (lines 1-6)
- Worker, scheduler, and billing services must be manually configured in Railway dashboard
- If not configured, Railway may use default or wrong start command

**Files affected**: worker, scheduler, billing services

**Fix**:
1. In Railway dashboard, create/delete services as needed:
   - **web**: `gunicorn app:app --bind 0.0.0.0:${PORT:-8080} --workers ${WEB_CONCURRENCY:-1} --threads ${GUNICORN_THREADS:-2} --timeout ${GUNICORN_TIMEOUT:-60}`
   - **worker**: `python automation_worker.py`
   - **scheduler**: `python scheduler.py`
   - **billing**: `python cron_jobs.py billing` (or use Railway Cron instead)

2. **For each service, copy environment variables from web service**:
   - `DATABASE_URL`
   - `SECRET_KEY` 
   - All variables from `deployment_check.py` (lines 8-21)
   - Service-specific vars like `AUTOMATION_WORKER_INTERVAL_SECONDS` (for worker)

3. **Verify service type**: 
   - Web: "Web Service" 
   - Worker/Scheduler: "Background Worker"
   - Billing: Either "Background Worker" or set up as Railway Cron

---

## 🟡 HIGH: Alembic Migration Failures

**Error**: `ProgrammingError: relation "alembic_version" does not exist`  
**OR** `FileNotFoundError: [Errno 2] No such file or directory: 'database/migrations/versions/'`

**Location**: `database.py`, lines 232-249 (`run_alembic_migrations()`)
```python
command.upgrade(config, "head")
```

**Why it crashes**: 
- Migration scripts missing or corrupted
- Database schema mismatch
- `STRICT_ALEMBIC_MIGRATIONS=true` causes crash on any migration warning

**Files affected**: Web service primarily (only PostgreSQL backend runs migrations)

**Fix**:
1. **Check migration folder**: Ensure `database/migrations/versions/` exists with migration files
2. **Reduce strictness**: Add service variable:
   - `STRICT_ALEMBIC_MIGRATIONS=false`
3. **Skip migrations temporarily** (for debugging):
   - `SKIP_ALEMBIC_MIGRATIONS=true`
   - ⚠️ Only use temporarily - missing migrations cause schema issues
4. **Manual migration fix**:
   ```bash
   # Local debugging
   export DATABASE_URL=[your url]
   alembic upgrade head
   ```

---

## 🟡 MEDIUM: Import/Dependency Errors

**Error**: `ModuleNotFoundError: No module named 'X'`  
**OR** `ImportError: cannot import name 'Y' from 'Z'`

**Location**: Various import statements in service files:
- `automation_worker.py`: lines 4-5
- `scheduler.py`: lines 3-5  
- `cron_jobs.py`: lines 1-15
- `app.py`: lines 13-94

**Why it crashes**: Missing dependencies or syntax errors in imported modules

**Files affected**: Depends on which import fails

**Fix**:
1. **Verify all requirements install**:
   ```bash
   pip install -r requirements.txt
   pip list | grep -E "(psycopg2|Flask|gunicorn|alembic|aisdk)"
   ```
2. **Check for syntax errors**:
   ```bash
   python -m py_compile app.py automation_worker.py scheduler.py cron_jobs.py
   ```
3. **Test imports individually**:
   ```bash
   # Test web imports
   python -c "from app import app; print('Web imports OK')"
   
   # Test worker imports  
   python -c "from automation_worker import run_worker; print('Worker imports OK')"
   
   # Test scheduler imports
   python -c "from scheduler import run_scheduler; print('Scheduler imports OK')"
   ```
4. **Check __pycache__**: Sometimes clearing cache helps:
   ```bash
   find . -name "__pycache__" -type d -exec rm -rf {} + 2>/dev/null || true
   ```

---

## 🟢 LOW: Port Binding Issues (Web Service Only)

**Error**: `Error: Invalid value for "--bind": invalid host/port format`  
**OR** Address already in use errors

**Location**: `Procfile` line 1 and `railway.json` line 4
```bash
web: sh -c "gunicorn app:app --bind 0.0.0.0:${PORT:-8080} ..."
```

**Why it crashes**: 
- `PORT` environment variable not set by Railway
- Invalid characters in PORT value
- Port already occupied (rare in Railway)

**Files affected**: Web service only

**Fix**:
1. **Verify PORT is set**: Railway automatically sets `PORT` variable
2. **Alternative hardcoded port** (for debugging):
   Change Procfile line 1 to:
   ```bash
   web: sh -c "gunicorn app:app --bind 0.0.0.0:8080 --workers 1 --threads 2 --timeout 60"
   ```
3. **Check railway.json**: Ensures web service uses same command (lines 4-5)

---

## 🚀 QUICK START CHECKLIST

Follow this order to fix crashing services:

### ✅ Step 1: Critical Environment Variables (ALL SERVICES)
[ ] `DATABASE_URL` = [from Railway Postgres service]  
[ ] `SECRET_KEY` = [32-byte hex string]  

### ✅ Step 2: Service Configuration  
[ ] Web service: Using Procfile command  
[ ] Worker service: `python automation_worker.py`  
[ ] Scheduler service: `python scheduler.py`  
[ ] Billing service: `python cron_jobs.py billing` OR Railway Cron  

### ✅ Step 3: Dependency Verification  
[ ] `psycopg2-binary` in requirements.txt  
[ ] All services can import without ModuleNotFoundError  

### ✅ Step 4: Database Connection  
[ ] Services show `startup_success` or `Database ready:` in logs  
[ ] No `OperationalError` or `ImportError`  

### ✅ Step 5: Service-Specific Vars  
[ ] Worker: `AUTOMATION_WORKER_INTERVAL_SECONDS` (default 30)  
[ ] All: Missing vars from deployment_check.py cause warnings but not crashes  

### EXPECTED LOGS WHEN FIXED:
**Web**: 
```
startup_environment_validated
startup_success
```
**Worker**: 
```
Database ready: postgres
Automation worker started
```
**Scheduler**: 
```
Database ready: postgres  
Scheduler started...
```

If services still crash after these fixes, check the FIRST error in logs - it's almost always the root cause.