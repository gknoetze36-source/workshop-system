"""
Permission Route for Workshop System Version 2.

This file defines the Permission Blueprint.
It imports only the Permission Service and contains a placeholder route
to make the Blueprint syntactically valid.
"""

from flask import Blueprint
from services.permission_service import get_allowed_roles_for_creator  # Import only the service (though we won't call it in the placeholder)

permission_bp = Blueprint('permission', __name__)

@permission_bp.route('/permissions', methods=['GET'])
def list_permissions_placeholder():
    """
    TEMPORARY FOUNDATION ONLY
    This endpoint exists solely to make the Blueprint syntactically valid.
    It does not implement any Permission functionality.
    """
    return "Not Implemented", 501
