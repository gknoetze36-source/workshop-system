from werkzeug.exceptions import BadRequest


def require_fields(source, fields):
    missing = [field for field in fields if not str(source.get(field) or "").strip()]
    if missing:
        raise BadRequest(f"Missing required field(s): {', '.join(missing)}")


def safe_int(value, field="id", minimum=1):
    try:
        number = int(value)
    except (TypeError, ValueError):
        raise BadRequest(f"Invalid {field}")
    if number < minimum:
        raise BadRequest(f"Invalid {field}")
    return number
