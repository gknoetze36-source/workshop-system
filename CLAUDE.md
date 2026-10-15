# Working on PHANTA

- **Every user-facing change also updates the ghost help**: `routes/ghost_help.py` (topics, steps, keywords) and the guides in `docs/ghost/` (`meta_setup_guide.md`, `google_setup_guide.md`). Check them for old button names, amounts or steps before shipping.
- Organic posting and paid ads for the same network live in **one Meta app** with **separate PHANTA connections** (e.g. Facebook posting vs Facebook ads). Use the same pattern for Instagram.
- Tests: `rm -f phanta.db && DEV_FLASK_SECRET_KEY=x SUPERADMIN_PASSWORD=x python -m pytest tests -q --ignore=tests/integration`
