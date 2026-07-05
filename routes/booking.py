"""
Booking Route for Workshop System Version 2.

This file defines the Booking Blueprint.
It imports only the Booking Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.booking_service import get_visible_bookings  # Import only the service (though we won't call it in the placeholder)

booking_bp = Blueprint('booking', __name__)

@booking_bp.route('/bookings', methods=['GET'])
def list_bookings_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Booking functionality.
    """
    return "Not Implemented", 501