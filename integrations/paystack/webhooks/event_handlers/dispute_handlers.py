def handle(session, data: dict, location_id: int):
    # Disputes should be represented in the platform audit trail before any
    # manual financial action. Full dispute table can be added if needed.
    return {"location_id": location_id, "dispute": data}
