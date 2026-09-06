# PHANTA Meta v26 Repair Report

Applied only confirmed fixes from the Meta/Flyer Lady audit:

- Canonical Graph API version moved from v25.0 to v26.0.
- Removed the hardcoded WhatsApp v20.0 endpoint and made it use the configured canonical version.
- Fixed WhatsApp customer-token double decryption.
- Added long-lived Facebook user-token exchange before Flyer Lady derives Page tokens.
- Flyer Lady now rejects a selected Page that lacks the CREATE_CONTENT task.
- Added Instagram container status polling before media_publish.
- Added Instagram publishing-limit client support for future queue/quota checks.
- Flyer Lady UI now queues Instagram Feed and Story destinations.
- Queue button no longer immediately publishes while claiming to queue; the existing scheduled queue remains responsible for processing.
- Added validation so Story/Instagram posts cannot be queued without a public image.
- Added Embedded Signup postMessage capture for WABA/phone asset IDs and prevented false-success connections without required WhatsApp assets.

Not changed:

- Service Advisor.
- Unrelated dashboard/business logic.
- WhatsApp Status publishing: it remains prepare-and-share because there is no official Cloud API Status publish endpoint.
- No new frameworks or broad refactors were introduced.
