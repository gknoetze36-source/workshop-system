def _available_slot_summary(location_id, days=(0, 1)):
    location = fetch_one("SELECT daily_capacity, name FROM locations WHERE id=%s", (location_id,))
    if not location:
        return ""
    capacity = int(location.get("daily_capacity") or 0)
    labels = []
    for offset in days:
        day = datetime.utcnow().date() + timedelta(days=offset)
        date_key = day.strftime("%Y-%m-%d")
        booked = fetch_one("SELECT COUNT(*) AS total FROM bookings WHERE location_id=%s AND scheduled_date=%s", (location_id, date_key))
        total = int((booked or {}).get("total") or 0)
        remaining = max(capacity - total, 0)
        if remaining > 0:
            label = "today" if offset == 0 else ("tomorrow" if offset == 1 else day.strftime("%a"))
            if remaining >= 3:
                labels.append(f"{label} morning or afternoon")
            else:
                labels.append(f"{label} limited availability")
    return ", ".join(labels[:2])
