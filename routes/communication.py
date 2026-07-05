"""
Communication Route for Workshop System Version 2.

This file defines the Communication Blueprint.
It imports only the Communication Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.communication_service import get_communication_logs_count_by_franchise  # Import only the service (though we won't call it in the placeholder)

communication_bp = Blueprint('communication', __name__)

@communication_bp.route('/communication', methods=['GET'])
def list_communication_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Communication functionality.
    """
    return "Not Implemented", 501