"""
Service Knowledge Base
Defines recommended services by vehicle mileage for car workshops
"""

CAR_SERVICE_REQUIREMENTS = {
    # Generic car services - works for all makes by default
    "all": {
        "0-10000": [
            {
                "service": "Lube Service",
                "category": "Maintenance",
                "description": "Regular oil change with Oil filter replacement",
            },
            {
                "service": "Air Filter Inspection",
                "category": "Inspection",
                "description": "Check and replace air filter if needed",
                "duration_minutes": 15,
                "base_price": 1000,
            },
        ],
        "10001-50000": [
            {
                "service": "Oil Change",
                "category": "Maintenance",
                "description": "Regular oil change with new filter",
                "duration_minutes": 30,
                "base_price": 2000,
            },
            {
                "service": "Tire Rotation",
                "category": "Maintenance",
                "description": "Rotate tires for even wear",
                "duration_minutes": 30,
                "base_price": 2500,
            },
            {
                "service": "Battery Check",
                "category": "Inspection",
                "description": "Test battery health and connections",
                "duration_minutes": 15,
                "base_price": 1000,
            },
            {
                "service": "Brake Inspection",
                "category": "Safety",
                "description": "Check brake pad thickness and rotor condition",
                "duration_minutes": 30,
                "base_price": 1500,
            },
        ],
        "50001-100000": [
            {
                "service": "Transmission Flush",
                "category": "Maintenance",
                "description": "Flush and replace transmission fluid",
                "duration_minutes": 60,
                "base_price": 5000,
            },
            {
                "service": "Brake Pad Replacement",
                "category": "Safety",
                "description": "Replace worn brake pads",
                "duration_minutes": 45,
                "base_price": 4000,
            },
            {
                "service": "Suspension Check",
                "category": "Inspection",
                "description": "Inspect suspension components for wear",
                "duration_minutes": 45,
                "base_price": 3000,
            },
            {
                "service": "Coolant Flush",
                "category": "Maintenance",
                "description": "Flush and replace engine coolant",
                "duration_minutes": 45,
                "base_price": 3000,
            },
        ],
        "100001-150000": [
            {
                "service": "Spark Plugs Replacement",
                "category": "Maintenance",
                "description": "Replace engine spark plugs",
                "duration_minutes": 60,
                "base_price": 4000,
            },
            {
                "service": "Engine Oil System Flush",
                "category": "Maintenance",
                "description": "Deep clean engine oil system",
                "duration_minutes": 60,
                "base_price": 4000,
            },
            {
                "service": "Power Steering Fluid Flush",
                "category": "Maintenance",
                "description": "Replace power steering fluid",
                "duration_minutes": 30,
                "base_price": 2000,
            },
        ],
        "150000+": [
            {
                "service": "Timing Belt Inspection",
                "category": "Critical",
                "description": "Inspect/replace timing belt (critical for engine)",
                "duration_minutes": 120,
                "base_price": 12000,
            },
            {
                "service": "Engine Overhaul Assessment",
                "category": "Critical",
                "description": "Comprehensive engine inspection and assessment",
                "duration_minutes": 120,
                "base_price": 8000,
            },
        ],
    }
}

def get_services_for_vehicle_mileage(make=None, mileage=0):
    """
    Get recommended services based on vehicle mileage
    
    Args:
        make: Vehicle make/brand (optional, uses generic if not found)
        mileage: Current vehicle mileage
    
    Returns:
        List of recommended service dictionaries
    """
    if not mileage or mileage < 0:
        return []
    
    # Try specific make first, fall back to "all"
    services_by_make = CAR_SERVICE_REQUIREMENTS.get(make) or CAR_SERVICE_REQUIREMENTS.get("all")
    
    # Find matching mileage range
    for range_key in sorted(services_by_make.keys()):
        if "-" in range_key:
            min_km, max_km = map(int, range_key.split("-"))
            if min_km <= mileage <= max_km:
                return services_by_make[range_key]
    
    # Return highest mileage services if over max
    return services_by_make.get("150000+", [])

def get_next_service_due_for_vehicle(current_mileage, last_service_mileage=0):
    """
    Calculate next service due based on mileage
    
    Args:
        current_mileage: Current vehicle mileage
        last_service_mileage: Mileage at last service
    
    Returns:
        Dictionary with service due info
    """
    # Standard service intervals (km)
    intervals = [15000, 30000, 75000, 150000]
    
    for interval in intervals:
        if current_mileage < interval and last_service_mileage < interval:
            return {
                "due_mileage": interval,
                "km_remaining": interval - current_mileage,
                "critical": interval >= 150000,
            }
    
    return {
        "due_mileage": current_mileage + 50000,
        "km_remaining": 50000,
        "critical": True,
    }