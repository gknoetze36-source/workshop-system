"""App-wide error pages.

Previously registered via @error_bp.errorhandler(404)/(500) on a
Blueprint with no routes of its own. Flask blueprint-scoped error
handlers only fire for errors raised by routes registered on that same
blueprint -- with none registered here, these could never actually fire
for a genuine unmatched URL or unhandled exception anywhere else in the
app. Confirmed directly: a request to a nonexistent URL returned
Werkzeug's bare default 404 page, not this file's own 404.html, meaning
the branded error pages had never actually been served in production.
Fixed by registering these as app-level handlers (see phanta_app.py)
instead of blueprint-level ones.
"""
from flask import render_template


def register_error_handlers(app):
    @app.errorhandler(404)
    def page_not_found(e):
        return render_template("404.html"), 404

    @app.errorhandler(500)
    def internal_server_error(e):
        return render_template("500.html"), 500
