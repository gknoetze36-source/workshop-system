# Database Audit Report

## Overview
The workshop system uses a hybrid database approach:
- PostgreSQL for production (with Alembic migrations)
- SQLite for development/testing (with schema managed in database.py)
- The database.py file contains table creation logic that handles both PostgreSQL and SQLite syntax differences

## Key Findings

### 1. Schema Integrity Issues
**Duplicate Table Definitions (CRITICAL)**
- `service_requirements` table defined twice in `_create_tables()` function (lines 824-838 and 841-855)
  - First definition: `vehicle_make TEXT NOT NULL`
  - Second definition: `vehicle_make TEXT` (nullable)
  - This creates inconsistency in whether vehicle_make is required
- `vehicles` table defined twice (lines 378-397 and 858-877)
  - Fortunately identical definitions, so harmless but redundant

**Denormalization Risks**
- `users` table contains both `branch` (TEXT) and `branch_id` (INTEGER) fields
- `users` table contains both `company` (TEXT) and `franchise_id` (INTEGER) fields
- This denormalization creates risk of inconsistent data if not properly maintained

**Redundant Fields**
- `users` table has both `password` and `password_hash` fields
  - Storing both plaintext passwords and hashes is a security anti-pattern
  - The `password` field should not exist in production

### 2. Indexes Analysis
**Existing Indexes (from _ensure_indexes):**
- Comprehensive coverage for most common query patterns
- Includes unique indexes where appropriate (slugs, booking references, etc.)
- Composite indexes for multi-column filtering

**Potential Missing Indexes:**
1. `reminder_campaigns(booking_id)` - for finding campaigns by booking
2. `communication_logs(booking_id)` - for finding communications by booking
3. `communication_logs(reminder_id)` - for finding communications by reminder
4. `audit_logs(user_id)` - for finding actions by specific user
5. `feature_flags(franchise_id)` - though covered by unique index on (franchise_id, feature_key)

**Index Consistency:**
- Most index definitions use `IF NOT EXISTS` and are backend-agnostic
- PostgreSQL-specific indexes in migrations properly check dialect before execution

### 3. Foreign Keys and Relationships
**Implementation Approach:**
- Foreign key constraints are NOT defined in `_create_tables()` (for SQLite compatibility)
- Foreign keys are added via Alembic migrations for PostgreSQL only
- This creates a potential inconsistency where SQLite databases lack enforceable referential integrity

**Defined Foreign Key Constraints (from migrations):**
- `branches.franchise_id` → `franchises.id`
- `users.franchise_id` → `franchises.id`
- `users.branch_id` → `branches.id`
- `bookings.franchise_id` → `franchises.id`
- `bookings.branch_id` → `branches.id`
- `bookings.customer_id` → `customers.id`
- `bookings.service_id` → `services.id`
- `webhook_events.workshop_id` → `workshops.id`

**Missing Foreign Keys:**
- No explicit foreign key from `onboarding_sessions.franchise_id` → `franchises.id`
- No explicit foreign key from `onboarding_answers.franchise_id` → `franchises.id`
- No explicit foreign key from `onboarding_answers.session_id` → `onboarding_sessions.id`
- No explicit foreign key from `feature_flags.franchise_id` → `franchises.id`
- No explicit foreign key from `automation_rules.franchise_id` and `branch_id` → respective tables
- No explicit foreign key from `automation_rules.template_id` → `automation_templates.id`
- No explicit foreign key from `scheduled_jobs.franchise_id` → `franchises.id`
- No explicit foreign key from `scheduled_jobs.automation_rule_id` → `automation_rules.id`
- No explicit foreign key from `automation_logs.franchise_id` → `franchises.id`
- No explicit foreign key from `automation_logs.automation_rule_id` → `automation_rules.id`
- No explicit foreign key from `automation_logs.scheduled_job_id` → `scheduled_jobs.id`
- No explicit foreign key from `failed_jobs.franchise_id` → `franchises.id`
- No explicit foreign key from `failed_jobs.scheduled_job_id` → `scheduled_jobs.id`
- No explicit foreign key from `billing_records.franchise_id` → `franchises.id`
- No explicit foreign key from `usage_daily.franchise_id` → `franchises.id`
- No explicit foreign key from `chatbot_usage_daily.franchise_id` → `franchises.id`
- No explicit foreign key from `chatbot_usage_monthly.franchise_id` → `franchises.id`
- No explicit foreign key from `onboarding_state.franchise_id` → `franchises.id`

### 4. Migration Safety
**Strengths:**
- Migrations check `bind.dialect.name != "postgresql"` to skip non-PGSQL operations
- Use `IF NOT EXISTS` and constraint existence checks before creating
- Proper transaction handling in migration scripts
- Downgrade paths provided for all migrations

**Potential Risks:**
- Hybrid schema management (migrations for PGSQL, direct SQL for SQLite) risks schema drift
- No automated testing shown for migration compatibility between SQLite and PostgreSQL
- The `_ensure_columns()` function may conflict with migration-added columns if not carefully coordinated

### 5. Query Efficiency
**Strengths:**
- Indexes cover common access patterns:
  - Franchise lookup by slug
  - Branch lookup by franchise+slug
  - User lookup by franchise/branch
  - Booking lookup by franchise/branch/date
  - Service lookup by franchise/branch/name
  - Webhook event lookup by workshop/provider/date
  - Automation rule lookup by franchise/event/active status
  - Scheduled job lookup by status/scheduled_for
  - Billing/lookup by franchise/period/status
  - Usage tracking by franchise/date

**Opportunities for Improvement:**
1. Add missing indexes identified above
2. Consider covering indexes for frequent JOIN patterns
3. Analyze actual query logs to verify index usage
4. Consider partial indexes for common filtered queries (e.g., active records only)

### 6. Data Consistency Issues
**Critical:**
- Duplicate `service_requirements` table with conflicting `vehicle_make` nullability
- Storing plaintext passwords in `users.password` field (security risk)

**High:**
- Denormalized `branch` and `company` fields in `users` table risk inconsistency
- No foreign key enforcement in SQLite databases

**Medium:**
- Potential for inconsistent boolean values (0/1 vs TRUE/FALSE) if application logic doesn't normalize
- TEXT-based timestamps may cause sorting issues compared to proper datetime types

### 7. Specific Area Analysis

**Franchises Table:**
- Well-designed with comprehensive fields for subscription, billing, limits, and feature flags
- Proper indexing on slug and workshop_id
- Consider adding index on subscription_status for filtering active franchises

**Branches Table:**
- Good structure with proper foreign key to franchises (via migration)
- Unique index on (franchise_id, slug) prevents duplicate slugs within franchise
- Missing index on location for geographic queries

**Users Table:**
- **Issues:** Duplicate password fields, denormalized branch/company, missing not-null constraints on critical fields
- **Strengths:** Proper indexing on franchise_id and branch_id
- **Security:** Plaintext password storage is critical vulnerability

**Onboarding Tables:**
- `onboarding_sessions` and `onboarding_answers` properly structured
- Missing foreign key constraints (as noted above)
- Index on (session_id, question_key) is appropriate

**Feature Flags:**
- Proper structure with unique constraint on (franchise_id, feature_key)
- Index supports efficient lookups

**Automation System:**
- Templates → Rules → Scheduled Jobs → Logs is a sensible design
- Missing foreign keys compromise data integrity
- Indexes on rule lookup (franchise_id, event_type, active) are appropriate

**Billing:**
- Simple but effective structure
- Index on (franchise_id, billing_period, status) supports common queries
- Consider adding index on paid_at for overdue payment queries

**Usage Tracking:**
- `usage_daily` table properly indexed with unique constraint on (franchise_id, usage_date)
- Chatbot usage tables follow similar pattern
- Consider aggregating usage data periodically for performance

## Optimization Opportunities

### Immediate Fixes (High Priority):
- Remove duplicate `service_requirements` table definition and Standardize on NOT NULL for vehicle_make
- Remove duplicate `vehicles` table definition (harmless but cleanup)
- Remove `password` field from `users` table (security critical)
- Add NOT NULL constraints to critical fields where appropriate

### Schema Improvements (Medium Priority):
- Add missing foreign key constraints for all tables
- Consider normalizing denormalized fields (branch, company in users)
- Add missing indexes identified in analysis
- Consider using proper datetime types instead of TEXT for timestamps

### Migration Improvements (Medium Priority):
- Consider unifying schema management approach (either all migrations or all direct SQL)
- Add migration tests to verify SQLite/PostgreSQL compatibility
- Add check constraints for data validation (e.g., status fields, percentage ranges)

### Performance Optimizations (Low Priority):
- Add covering indexes for frequent JOIN patterns
- Consider table partitioning for high-volume tables (bookings, communication_logs)
- Add indexes on frequently filtered status fields
- Consider materialized views for complex reporting queries

## Migration Risks
1. **Schema Drift Risk:** Hybrid approach may cause PostgreSQL and SQLite schemas to diverge over time
2. **Data Loss Risk:** Improperly structured ALTER TABLE statements in migrations could cause issues
3. **Compatibility Risk:** Missing testing of migrations against both database engines
4. **Constraint Risk:** Adding NOT NULL columns without defaults could break existing data

## Recommendations

### Immediate Actions (Do Now):
1. Fix duplicate table definitions in `database.py` `_create_tables()` function
2. Remove `password` field from `users` table definition and related code
3. Add NOT NULL constraints to `service_requirements.vehicle_make` (choose one definition and stick with it)
4. Audit codebase for usage of `users.password` field and remove all references

### Short-Term Actions (Next Sprint):
1. Add missing foreign key constraints via migrations
2. Add missing indexes identified in analysis
3. Consider adding CHECK constraints for status fields and other validated data
4. Evaluate denormalized fields in users table for normalization

### Long-Term Actions (Future Releases):
1. Consider migrating to pure migration-based schema management
2. Implement database testing suite that validates both SQLite and PostgreSQL behavior
3. Consider connection pooling improvements for high-load scenarios
4. Implement query performance monitoring and slow query logging

## Conclusion
The database schema shows thoughtful design with appropriate consideration for multi-tenant SaaS patterns. Critical issues around duplicate table definitions and plaintext password storage must be addressed immediately. The migration system is well-designed but could benefit from greater consistency between development and production database management approaches.

---
**Handoff to security-auditor:** The plaintext password storage in the users table represents a critical security vulnerability that requires immediate attention from the security auditor. Additionally, the denormalized data structures may create consistency issues that could lead to authorization bypasses if not properly handled in application logic.