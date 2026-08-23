# PHANTA Phase 13 — Service Recommendation Rules

## Scope
Implements BUILD_ORDER Phase 13:
- deterministic service interval rules
- `get_due_services`
- recommendation records
- AI explanation only after the deterministic rule engine

## Architecture
The ServiceRuleEngine is the source of truth for maintenance intervals. It selects the
most specific active rule for a vehicle, with tenant-specific rules winning ties over
global rules. It evaluates mileage and/or time intervals using the vehicle's current
mileage and latest recorded service.

The Service Advisor never calculates intervals itself. `get_due_services` calls the
rule engine and persists idempotent open Recommendation records. The AI receives the
structured result and is instructed to explain those results rather than inventing
maintenance schedules.

## Existing foundation reused
- `service_rules` table and baseline rules from migration `0004_service_rules_phase5`
- `recommendations` table from the Phase 2 foundation
- `ServiceRepository` service history
- tenant-safe vehicle lookup in the Service Advisor tool registry

## Rule precedence
1. Vehicle make + model + engine
2. Make + model
3. Make
4. Generic rule

Tenant-specific rules override global rules at the same specificity.

## Recommendation behavior
- `due` when an interval has been reached/passed.
- `upcoming` when within the configured early-warning threshold.
- `not_due` results are not returned or persisted as open recommendations.
- Repeated evaluation is idempotent for the same service type/due mileage/due date.

## Safety boundary
The LLM does not create or alter maintenance intervals. Manufacturer-specific rules
can be added as workshop/vehicle knowledge improves. The AI's role is conversational
explanation only.
