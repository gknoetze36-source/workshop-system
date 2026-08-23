# Integration Plan — Quickest Route

## Meta / WhatsApp Cloud API
**Goal:** workshop connects its own WABA and PHANTA can receive/send messages.

**Quickest code path**
1. GraphApiClient
2. Embedded Signup frontend
3. EmbeddedSignupService
4. encrypted connection storage
5. phone registration
6. webhook handshake/signature
7. webhook routing
8. outbound send
9. templates

**Outside-code work**
- Meta Developer Account
- PHANTA Business Portfolio
- Business Verification
- Meta App + WhatsApp product
- Facebook Login for Business config
- domains + HTTPS
- privacy/terms/data deletion URLs
- App Review for Advanced Access
- Access Verification if required
- billing/partner configuration
- real test business/test number
- App Review screen recording

**Fastest v1 decision:** new/dedicated WhatsApp numbers only; do not build Coexistence until a real customer requires it.

Source basis: the blueprint explicitly puts authentication first, then Embedded Signup, token management, phone registration, webhooks, messaging and templates. It also warns that Meta's verification pipeline is a major lead-time risk.

## Paystack
**Goal:** PHANTA can take payment and later run subscriptions/overage billing.

**Quickest code path**
1. PaystackClient
2. initialize
3. verify
4. webhook signature verification
5. idempotency
6. payment persistence
7. refund
8. reconciliation
9. subscriptions
10. dunning/overage charging

**Outside-code work**
- Paystack account
- Test Mode
- ZAR/payment-channel confirmation
- business verification for Live Mode
- settlement bank account
- production webhook
- live keys
- IP whitelist
- live end-to-end test

**Fastest v1:** one-off payments first. Add subscriptions only after the one-off lifecycle works.

## AI Platform
**Goal:** Service Advisor can use an AI model without PHANTA being locked to one provider.

**Quickest code path**
1. AIProvider interface
2. OpenAI/ChatGPT provider
3. model config
4. AIDispatcher
5. usage/cost logging
6. output guard
7. OpenAI-only routing (no fallback in Phase 10)
8. Provider abstraction reserved for future providers

**Outside-code work**
- provider account(s)
- API keys
- billing/limits
- model availability confirmation
- usage monitoring
- set spend limits/alerts where available

**Phase 10 decision:** OpenAI/ChatGPT is the only active provider. Keep the provider abstraction, but defer multi-provider fallback until a later phase. No local LLM, vector DB or multi-agent framework.

## Google Calendar
**Goal:** confirmed PHANTA bookings can appear on the owner's calendar.

**Quickest code path**
1. OAuth client
2. encrypted token store
3. calendar service
4. one-way event push
5. sync state
6. push notifications
7. renewal job

**Outside-code work**
- separate Google dev and production projects
- consent screen
- branding
- home/privacy URLs
- domain ownership verification
- OAuth verification if required
- production redirect/callback configuration

**Fastest v1:** one-way push from PHANTA bookings to Google Calendar. Keep PHANTA's bookings table as source of truth. Add push notifications only after base sync is stable.
