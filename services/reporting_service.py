"""
Reporting Service for Workshop System Version 2.

Business logic for reporting and analytics.
Depends only on the Reporting Repository.
"""

# ============================================================================
# Repository
# ============================================================================

from repositories.reporting_repository import (
    get_location_report as _get_location_report,
    get_service_profit as _get_service_profit,
)

# ============================================================================
# Reporting
# ============================================================================

def get_location_report(location_id):
    """Return the reporting dashboard data for a location."""
    return _get_location_report(location_id)


def get_service_profit(location_id):
    """Return revenue grouped by service for a location."""
    return _get_service_profit(location_id)
