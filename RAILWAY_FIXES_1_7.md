# PHANTA Railway / Production Fixes 1-7

Based on `workshop-system-main(2).zip` supplied on 2026-08-11.

## Changes made

1. **Railway startup**
   - Dockerfile now runs Gunicorn through `sh -c` with Railway's injected `$PORT`.
   - Railway config explicitly uses the same shell-safe start command.

2. **Flyer Lady OAuth redirect URI**
   - The exact redirect URI used when OAuth starts is stored in the Flask session.
   - The callback now requires and reuses that exact URI for the token exchange instead of reconstructing a different URI.

3. **Separate Meta configuration IDs**
   - Added `META_WHATSAPP_CONFIG_ID` and `META_FLYER_LADY_CONFIG_ID` to `.env.example`.
   - `META_EMBEDDED_SIGNUP_CONFIG_ID` remains as a backward-compatible WhatsApp alias for the current code.
   - Flyer Lady now requires its own configuration ID and sends it as `config_id` during its Facebook Login for Business authorization.

4. **Graph API version consistency**
   - `.env.example` now matches the code's default `v25.0`.

5. **Railway health endpoint**
   - Added `GET /health`.
   - It checks database connectivity with `SELECT 1` and returns `200` only when the application can reach its configured database.
   - No secrets or tenant data are returned.

6. **Production migration flow**
   - Added `database/predeploy.py`.
   - Railway now runs this before the application starts.
   - It creates the legacy schema needed by PHANTA, applies compatibility updates, and then runs Alembic to `head`.
   - Application startup no longer runs Alembic migrations itself, preventing web processes from competing over migrations.
   - Existing bootstrap/seed/index work remains in normal application initialization.

7. **Testing / validation**
   - 348 Python files were AST-parsed successfully.
   - `compileall` passed.
   - 45 Python test files are present.
   - The complete pytest suite could not be executed in this isolated environment because project dependencies are not installed and external package installation is unavailable. This must be run in the project's normal Python environment before final production deployment.

## Railway config added

`railway.toml` configures:

- Dockerfile builder
- `python -m database.predeploy` as the pre-deploy command
- shell-safe Gunicorn start command
- `/health` healthcheck
- 120 second healthcheck timeout
- restart on failure with up to 5 retries

## Important deployment rule

Do not pull from GitHub to apply these changes. Replace/copy these changes into the local working repository, inspect the local diff, commit, and **push** to GitHub. Railway should then deploy the pushed commit.
