# Security & Operations Skeleton

## Secrets
- All provider credentials belong in Railway secrets/environment variables.
- Never commit secrets.
- Never log access tokens, API secrets or OAuth refresh tokens.
- Rotation must require configuration change, not source changes.

## Webhooks
- Meta: raw-body HMAC-SHA256 verification.
- Paystack: raw-body HMAC-SHA512 verification.
- Google push: validate channel/resource identifiers and reconcile via sync token.

## Audit
Track:
- external webhook events
- AI tool executions
- AI usage/cost
- approvals
- manual staff overrides
- integration connection state

## Monitoring
Minimum dashboards:
- WhatsApp delivery failures
- Meta permission/error-code-200 spikes
- Meta token expiry
- WhatsApp quality degradation
- Paystack failed payments
- Paystack webhook/reconciliation failures
- Google token failures
- AI latency
- AI token usage/cost
- tool execution failures

## Operational drills
- secret rotation
- provider outage/fallback
- webhook replay
- database restore
- token re-authentication
- failed payment recovery
- duplicate webhook delivery
