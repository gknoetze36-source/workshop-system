"""
Location Route for Workshop System Version 2.

This file defines the Location Blueprint.
It imports only the Location Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.location_service import get_visible_branches  # Import only the service (though we won't call it in the placeholder)

location_bp = Blueprint('location', __name__)

@location_bp.route('/locations', methods=['GET'])
def list_locations_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Location functionality.
    """
    return "Not Implemented", 501
