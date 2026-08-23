# Part 12 — Universal Customer + Subject Architecture

## Purpose

Separate VANTA's universal customer/booking foundation from industry-specific
knowledge.

The key rule is:

> A customer is universal. A subject is optional and determined by industry
> knowledge.

## Customer-only industries

These operate on the customer without an additional work subject:

- Salon
- Dentist
- Restaurant
- Barber

Flow:

    Customer → Booking / Service

A customer-only booking must not require `vehicle_id`, `property_id`, or any
other subject.

## Customer + subject industries

These work on a customer plus the thing/place the service concerns:

### Workshop

    Customer → Vehicle → Booking / Service

The vehicle remains linked to the customer so vehicle history can be retained.

### Plumber

    Customer → Property / House → Job / Booking

A customer may have multiple properties.

### Construction

    Customer → Work Site → Job / Booking

A customer may have multiple work sites.

## Universal model

The universal domain contains:

- Business
- Customer
- Booking
- Lead
- Communication
- Usage event
- Outcome event

The optional subject model contains:

- subject_id
- subject_type
- industry-defined metadata

## Why subject_type + subject_id

This prevents automotive assumptions from leaking into the universal booking
model.

We do NOT make:

    booking.vehicle_id NOT NULL

a universal rule.

Instead:

    booking.customer_id
    booking.subject_id NULL
    booking.subject_type NULL

The industry definition decides whether the subject is required.

## Ownership

A subject belongs to a business and is linked to a customer according to the
industry's knowledge rules.

Examples:

    Workshop:
        customer 101
            └── vehicle 501
            └── vehicle 502

    Plumber:
        customer 101
            └── property 601
            └── property 602

    Salon:
        customer 101
            └── no subject required

## Important implementation rule

Do not turn this into a generic "anything table" with arbitrary unvalidated
JSON.

Subject types remain explicitly registered and validated.

The universal system knows the subject boundary; industry knowledge defines
the subject fields and workflow.

## Compatibility with Parts 8–11

### Part 8 / 8.1
Usage remains business-scoped and independent of whether an industry uses a
subject.

### Part 8.2
Booking and lead outcomes remain universal. A booking outcome may carry the
booking reference and therefore indirectly identify the subject involved.

### Part 9 / 10
Subscriptions, payments, and client management remain business-scoped.

### Part 11
Customer, booking, subject, lead and outcome access must obey tenant
permissions.

## Migration strategy

Do not attempt a giant destructive migration.

The rebuild should proceed in this order:

1. Create universal customer foundation.
2. Add optional subject relationship.
3. Create subject definitions/registry.
4. Implement Workshop vehicle subject.
5. Implement Workshop workflow.
6. Verify booking/customer/vehicle history.
7. Add additional industries one at a time.
8. Remove obsolete automotive-only assumptions after replacement tests pass.

## Workshop-specific rule

Workshop remains the first industry implementation.

A workshop booking requires:
- customer
- vehicle
- booking

Vehicle belongs to the customer.

This allows:

    customer history
    booking history
    vehicle history

without forcing those requirements onto salons, dentists, restaurants, or
other customer-only industries.
