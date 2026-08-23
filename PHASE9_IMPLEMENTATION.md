# PHANTA Phase 9 — Meta Messaging + Templates

## Scope
Phase 9 implements the Build Order items:
- outbound message client
- message persistence
- retry policy
- utility templates
- template status tracking
- customer 24-hour-window handling

The Phase 9 implementation intentionally does **not** introduce AI. The recommended milestone is a human-operated WhatsApp echo bot before AI.

## Architecture

```text
Authenticated PHANTA route / internal caller
        |
        v
MetaMessagingService
        |
        +--> WhatsAppSessionWindow
        +--> MetaTemplateRepository
        +--> MetaRetryPolicy
        +--> Message / MetaMessageAttempt persistence
        |
        v
MetaMessageClient
        |
        v
GraphApiClient
        |
        v
Meta Graph API /{phone_number_id}/messages
```

## Outbound text

Free-form text is allowed only when the conversation has an inbound WhatsApp message inside the 24-hour customer-service window.

The service:
1. resolves the connected tenant Meta connection;
2. decrypts the customer-scoped Embedded Signup token;
3. persists a `messages` row as `queued`;
4. calls Meta `/messages`;
5. stores the returned `wamid`;
6. changes the message to `sent`;
7. records the outbound attempt.

## Outside the 24-hour window

`send_auto()` refuses free-form text when the window is closed. If a template is supplied, it uses an approved Utility template.

Templates must be:
- registered in `meta_message_templates`;
- category `UTILITY`;
- status `APPROVED`.

This prevents the application from accidentally treating an unapproved or Marketing template as a routine service message.

## Retry policy

Transient HTTP conditions (`408`, `425`, `429`, `500`, `502`, `503`, `504`) and transport errors are classified as retryable.

Permanent Meta errors include the known validation/permission/template failure codes in the policy.

Maximum attempts: 3.

The service does not sleep inside the request. For a retryable failure it records the retry decision and returns a `queued_retry` state; a background worker can perform the next attempt without holding a web request open.

## Template tracking

Phase 8 already receives `message_template_status_update` webhooks. Phase 9 now mirrors those decisions into `meta_message_templates` while retaining the raw audit event.

## Database

Migration:
`0008_meta_messaging_phase9.py`

Adds:
- `meta_message_templates`
- `meta_message_attempts`

Existing `messages` remains the customer-visible source of truth for outbound message state and `wamid`.

## Testing

Phase 9 tests cover:
- 24-hour window open/closed
- outbound text persistence
- returned `wamid`
- approved Utility template sending
- rejection of pending templates
- retry classification

Validation result:
- Phase 9 + Phase 8 tests: **10 passed**
- Full suite: **73 passed, 3 skipped**
