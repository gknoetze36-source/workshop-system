from dataclasses import dataclass
from ..link_service import tracking_url
from ..models import Special
@dataclass(frozen=True)
class WhatsAppStatusAsset:
    media_url: str | None
    caption: str
    status: str = "prepared"
def prepare(special: Special) -> WhatsAppStatusAsset:
    return WhatsAppStatusAsset(special.media_url, f"{special.text.strip()}\n\nBook here: {tracking_url(special.id)}")
