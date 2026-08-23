"""
Subscription Service for Workshop System Version 2.

This service contains all subscription, plan and feature access logic.
"""

from datetime import datetime

from database import (
    execute_db,
    query_db,
    fetch_one,
    fetch_all,
    utc_now,
)

from helpers.common import (
    boolish,
    db_bool,
)

from helpers.dates import (
    utc_today,
    parse_date,
)
    
    