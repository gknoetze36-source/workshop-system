"""
PHANTA Dashboard Assistant

This module builds the AI assistant data used on the dashboard.

It contains NO database queries.
It only formats and interprets data that has already been
loaded by phanta_app.py.
"""


def build_dashboard_assistant(
    *,
    customer_count,
    vehicle_count,
    todays_bookings,
    upcoming_bookings,
    recent_bookings,
    pending_automation_count,
    failed_automation_count,
):
    """
    Build the assistant object for the dashboard.
    """

    # -------------------------------------------------------
    # Workshop Status
    # -------------------------------------------------------

    workshop_status = "Running Smoothly"
    workshop_status_colour = "green"

    if pending_automation_count >= 10:
        workshop_status = "Busy"
        workshop_status_colour = "orange"

    if failed_automation_count > 0:
        workshop_status = "Needs Attention"
        workshop_status_colour = "red"

    # -------------------------------------------------------
    # Greeting
    # -------------------------------------------------------

    if todays_bookings == 0:
        greeting = (
            "Good morning. There are currently no bookings scheduled for today."
        )

    elif todays_bookings == 1:
        greeting = (
            "Good morning. You have 1 vehicle booked in today."
        )

    else:
        greeting = (
            f"Good morning. You have {todays_bookings} vehicles booked in today."
        )

    # -------------------------------------------------------
    # Recommendation
    # -------------------------------------------------------

    if failed_automation_count > 0:
        recommendation = (
            f"{failed_automation_count} automation job(s) failed in the last 24 hours."
        )

    elif pending_automation_count >= 10:
        recommendation = (
            "You have a high number of pending automations waiting to run."
        )

    elif todays_bookings == 0:
        recommendation = (
            "Consider sending reminders or promotions to increase bookings."
        )

    elif todays_bookings >= 8:
        recommendation = (
            "Today looks busy. Ensure technicians and reception are prepared."
        )

    else:
        recommendation = (
            "Workshop operations look healthy today."
        )

    # -------------------------------------------------------
    # Insights
    # -------------------------------------------------------

    workshop_insights = []

    if todays_bookings == 0:
        workshop_insights.append({
            "icon": "📅",
            "title": "Bookings",
            "message": "No vehicles are booked for today."
        })

    elif todays_bookings <= 3:
        workshop_insights.append({
            "icon": "📅",
            "title": "Bookings",
            "message": "Today has a light workload."
        })

    elif todays_bookings <= 7:
        workshop_insights.append({
            "icon": "📅",
            "title": "Bookings",
            "message": "Today's workload looks balanced."
        })

    else:
        workshop_insights.append({
            "icon": "📅",
            "title": "Bookings",
            "message": "Workshop is expected to be busy today."
        })

    workshop_insights.append({
        "icon": "👥",
        "title": "Customers",
        "message": f"{customer_count} registered customers."
    })

    workshop_insights.append({
        "icon": "🚗",
        "title": "Vehicles",
        "message": f"{vehicle_count} vehicles in the database."
    })

    workshop_insights.append({
        "icon": "📆",
        "title": "Upcoming",
        "message": f"{upcoming_bookings} upcoming bookings."
    })

    if failed_automation_count > 0:
        workshop_insights.append({
            "icon": "⚠️",
            "title": "Automation",
            "message": f"{failed_automation_count} failed automation job(s)."
        })

    elif pending_automation_count > 0:
        workshop_insights.append({
            "icon": "⏳",
            "title": "Automation",
            "message": f"{pending_automation_count} pending automation job(s)."
        })

    else:
        workshop_insights.append({
            "icon": "✅",
            "title": "Automation",
            "message": "All automations are healthy."
        })

    # -------------------------------------------------------
    # Priority
    # -------------------------------------------------------

    if failed_automation_count > 0:
        priority = "Resolve failed automation jobs."

    elif todays_bookings >= 8:
        priority = "Prepare technicians for today's workload."

    elif todays_bookings == 0:
        priority = "Focus on generating new bookings."

    else:
        priority = "Continue normal workshop operations."

    # -------------------------------------------------------
    # Recent Activity
    # -------------------------------------------------------

    recent_activity = []

    for booking in recent_bookings:

        customer = (
            f"{booking['first_name']} {booking['surname']}"
        ).strip()

        recent_activity.append({
            "reference": booking["booking_reference"],
            "customer": customer,
            "vehicle": f"{booking['make']} {booking['model']}",
            "date": booking["scheduled_date"],
            "status": booking["status"],
        })

    if len(recent_activity) == 0:
        activity_summary = "There has been no recent booking activity."

    elif len(recent_activity) == 1:
        activity_summary = "1 recent booking was found."

    else:
        activity_summary = (
            f"{len(recent_activity)} recent bookings are available."
        )

    if recent_bookings:
        next_vehicle = {
            "customer": (
                f"{recent_bookings[0]['first_name']} "
                f"{recent_bookings[0]['surname']}"
            ).strip(),
            "vehicle": (
                f"{recent_bookings[0]['make']} "
                f"{recent_bookings[0]['model']}"
            ),
            "status": recent_bookings[0]["status"],
        }
    else:
        next_vehicle = None

    # -------------------------------------------------------
    # Final Assistant Object
    # -------------------------------------------------------

    return {
        "greeting": greeting,
        "status": workshop_status,
        "status_colour": workshop_status_colour,
        "recommendation": recommendation,
        "priority": priority,
        "customer_count": customer_count,
        "vehicle_count": vehicle_count,
        "todays_bookings": todays_bookings,
        "upcoming_bookings": upcoming_bookings,
        "pending_automations": pending_automation_count,
        "failed_automations": failed_automation_count,
        "insights": workshop_insights,
        "recent_activity": recent_activity,
        "activity_summary": activity_summary,
        "next_vehicle": next_vehicle,
    }