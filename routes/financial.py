"""
Financial Route for Workshop System Version 2.

This file defines the Financial Blueprint.
It imports only the Financial Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.financial_service import fetch_service_prices  # Import only the service (though we won't call it in the placeholder)

financial_bp = Blueprint('financial', __name__)

@financial_bp.route('/financial', methods=['GET'])
def list_financial_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Financial functionality.
    """
    return "Not Implemented", 501