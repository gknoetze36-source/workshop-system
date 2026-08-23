# PHANTA Production Observability

## Code complete
- Console logging is configured from `LOG_LEVEL` (default `INFO`).
- Logs are emitted to stdout for Railway collection.
- Common secrets are redacted from log messages.
- Flask errors are captured by Sentry when `SENTRY_DSN` is configured.
- Sentry request bodies, cookies, authorization headers and webhook signatures are excluded/redacted.
- Scheduled-job failures are logged and explicitly sent to Sentry when enabled.
- Authentication failures are logged without usernames, emails, passwords or tokens.
- Webhook signature/location/AI processing failures are logged without payloads.

## Credential required
`SENTRY_DSN` is required only to activate Sentry. It is not required for local development.

## Production configuration required
Set:
- `LOG_LEVEL=INFO` (or `WARNING`/`ERROR` as operationally appropriate)
- `SENTRY_DSN=<secret DSN>` if Sentry error tracking is desired

Railway should collect stdout/stderr. Do not put API keys, passwords, access tokens, webhook secrets, or customer message bodies into environment-variable debug output or logs.

Sentry is optional in local development and should not prevent the application from starting when `SENTRY_DSN` is absent.
