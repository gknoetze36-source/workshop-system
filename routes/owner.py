"""
Owner Route for Workshop System Version 2.

This file defines the Owner Blueprint.
It imports only the Owner Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.owner_service import get_owner_by_id  # Import only the service (though we won't call it in the placeholder)

owner_bp = Blueprint('owner', __name__)

@owner_bp.route('/owners', methods=['GET'])
def list_owners_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Owner functionality.
    """
    return "Not Implemented", 501
