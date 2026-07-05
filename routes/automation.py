"""
Automation Route for Workshop System Version 2.

This file defines the Automation Blueprint.
It imports only the Automation Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.automation_service import get_failed_jobs_count  # Import only the service (though we won't call it in the placeholder)

automation_bp = Blueprint('automation', __name__)

@automation_bp.route('/automation', methods=['GET'])
def list_automation_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Automation functionality.
    """
    return "Not Implemented", 501