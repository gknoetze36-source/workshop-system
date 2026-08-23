# PHANTA Phase 12 — Service Advisor v1

Status: **COMPLETE**

Phase 12 implements the Build Order milestone:

> WhatsApp customer → identify themselves → identify vehicle → describe problem → book.

## Implemented

- Customer detection by normalized E.164-style WhatsApp number.
- New customer creation on first inbound WhatsApp contact.
- Returning-customer detection based on prior bookings.
- Existing-lead detection when a customer has no booking history.
- Known vehicle discovery and history-first context.
- Structured slot capture through allowlisted function tools.
- One-loop Service Advisor conversation manager.
- Tenant/ownership validation on every customer/vehicle tool.
- Tool execution audit logging.
- OpenAI-only dispatcher integration from Phase 10.
- Deterministic output guard before customer-facing text.
- WhatsApp delivery loop after the webhook transaction commits.
- Existing Meta session-window/template policy remains authoritative.
- Booking tool now delegates to the Phase 11 BookingService rather than creating bookings independently.
- Structured extraction schema for customer/vehicle/problem/urgency fields.
- Internal `/service-advisor/reply` route for controlled testing.

## Important architecture boundaries

The Service Advisor is a single LLM loop with function calling. No CrewAI,
LangGraph, Temporal, vector database or custom agent framework is introduced.

Customer identity is deterministic database logic. The LLM does not decide
whether a phone number is new/returning.

The relational database remains the memory source of truth. AI only interprets
conversation and calls tools.

WhatsApp webhook persistence happens before the OpenAI call. Therefore a slow
or failed model request cannot cause Meta to retry the same inbound message.

## Required booking fields

Before a booking can be created, the conversation must resolve:
- make
- model
- year
- current problem/symptoms
- urgency
- a confirmed available booking slot

Mileage, engine, transmission, VIN and registration remain optional at booking
time and can be captured later.

## Production configuration

`OPENAI_API_KEY` must be present.

The workshop's operating hours should be injected into the Phase 11 booking
service. The tool registry contains an 08:00–17:00 weekday fallback for local
smoke tests only; production should use tenant-specific configuration.

## Validation

Phase 12 tests cover:
- new/existing/returning customer detection
- vehicle ownership isolation
- context generation
- structured tool calls
- tool-loop persistence
- booking delegation
- WhatsApp inbound-to-AI loop
- outbound delivery integration boundary
- maximum tool-round protection
