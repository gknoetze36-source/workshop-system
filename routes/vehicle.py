"""
Vehicle Route for Workshop System Version 2.

This file defines the Vehicle Blueprint.
It imports only the Vehicle Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.vehicle_service import get_vehicles_by_franchise  # Import only the service (though we won't call it in the placeholder)

vehicle_bp = Blueprint('vehicle', __name__)

@vehicle_bp.route('/vehicles', methods=['GET'])
def list_vehicles_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Vehicle functionality.
    """
    return "Not Implemented", 501