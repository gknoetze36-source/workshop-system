# PHANTA Phase 13 — Validation

## Automated checks
- Rule engine due-mileage evaluation
- Rule engine time-based evaluation
- Rule specificity precedence
- Tenant-specific rule precedence
- Service history used to calculate next interval
- Recommendation persistence
- Recommendation persistence idempotency
- Service Advisor `get_due_services` tool integration

## Required command
`python -m pytest -q`

## Required static check
`python -m compileall -q .`

## Acceptance milestone
A Service Advisor request for maintenance must obtain its maintenance facts from
`get_due_services`. The tool must use the deterministic rule engine and persist the
result as an auditable recommendation. The model may explain the returned result but
must not invent an interval.
