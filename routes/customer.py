"""
Customer Route for Workshop System Version 2.

This file defines the Customer Blueprint.
It imports only the Customer Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.customer_service import get_customer_count_by_franchise  # Import only the service (though we won't call it in the placeholder)

customer_bp = Blueprint('customer', __name__)

@customer_bp.route('/customers', methods=['GET'])
def list_customers_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Customer functionality.
    """
    return "Not Implemented", 501
