# Phase 18 — Post-service review link

PHANTA's review request is intentionally a small, deterministic feature.

## Scope

- Workshop stores one review platform: `google` or `hellopeter`.
- Workshop stores the corresponding HTTPS review URL.
- Workshop can enable or disable automatic review requests.
- A booking/service completion (`completed`) triggers one WhatsApp review request.
- The message contains the URL as ordinary copyable text.
- No button or interactive WhatsApp message is used.
- No Google OAuth, Google Cloud project, Google API, HelloPeter API, or provider integration exists.

## Message

The default message is:

> Thank you for choosing {workshop}. We hope you were happy with the service. If you have a moment, we would really appreciate your feedback:
> {plain URL}

If the WhatsApp customer-service window is closed, the existing Meta messaging layer requires an approved utility template; the review URL remains in the message body/parameters. PHANTA does not create or call any review-provider API.

## Configuration API

`GET /dashboard/reviews`

`PUT /dashboard/reviews`

Example enable payload:

```json
{
  "platform": "google",
  "url": "https://g.page/r/example/review",
  "enabled": true
}
```

Disable with:

```json
{
  "enabled": false
}
```

## Completion behaviour

The existing booking status endpoint detects a transition into `completed` and invokes the review service. A durable `post_service_review` record prevents a second automatic request for the same booking.

**Phase 18 implementation: complete.**
