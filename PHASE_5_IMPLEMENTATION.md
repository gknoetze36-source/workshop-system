# PHANTA Phase 5 — Service Recommendation Rule Engine

## Status

**Phase 5 implementation complete.**

This phase implements the deterministic maintenance recommendation layer specified by the Service Advisor blueprint. The LLM does not calculate service intervals. It calls `get_due_services(vehicle_id)` and explains the structured result.

## Delivered

- `service_rules` relational table
- Global rules plus tenant-specific overrides
- Rule specificity matching by make/model/engine
- Mileage-based intervals
- Month-based intervals
- Latest-service lookup per service type
- Due/upcoming evaluation
- Persisted `recommendations` records
- Idempotent recommendation persistence
- Service Advisor tool integration
- Presentation/explanation helper
- Migration `0004_service_rules_phase5`
- Deterministic unit tests

## Default seed rules

The migration seeds conservative generic baselines:

- Minor service: 15,000 km
- Major service: 30,000 km / 12 months
- Brake fluid: 24 months
- Coolant: 48 months

These are explicitly generic baselines. Vehicle/manufacturer-specific schedules should override them when authoritative data is available.

## Safety boundary

The rule engine is the source of truth for maintenance intervals. The AI may explain the result but must not invent or modify an interval in conversation.

## Validation

Full test suite:

- **36 passed**
- **3 skipped** (existing PostgreSQL/environment-specific tests)
- **2 warnings** (existing `datetime.utcnow()` deprecation warnings)

Phase 5 tests are deterministic and run without an external AI provider.
