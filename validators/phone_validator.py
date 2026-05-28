import re


E164_RE = re.compile(r"^\+[1-9][0-9]{7,14}$")


def normalize_phone(value):
    text = str(value or "").strip()
    if text.startswith("whatsapp:"):
        text = text.split(":", 1)[1]
    text = re.sub(r"[\s().-]+", "", text)
    if text.startswith("00"):
        text = f"+{text[2:]}"
    elif text.startswith("0") and len(text) == 10:
        text = f"+27{text[1:]}"
    elif text.startswith("27") and len(text) == 11:
        text = f"+{text}"
    return text


def is_valid_phone(value):
    return bool(E164_RE.fullmatch(normalize_phone(value)))
