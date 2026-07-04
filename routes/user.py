"""
User Route for Workshop System Version 2.

This file defines the User Blueprint.
It imports only the User Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.user_service import get_user_by_username_or_email  # Import only the service (though we won't call it in the placeholder)

user_bp = Blueprint('user', __name__)

@user_bp.route('/users', methods=['GET'])
def list_users_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any User functionality.
    """
    return "Not Implemented", 501
