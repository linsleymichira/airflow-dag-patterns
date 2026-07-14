# Specification Quality Checklist: Cross-Source Daily Mart

**Purpose**: Validate specification completeness and quality before proceeding to planning
**Created**: 2026-07-13
**Feature**: [spec.md](../spec.md)

## Content Quality

- [x] No implementation details (languages, frameworks, APIs)
- [x] Focused on user value and business needs
- [x] Written for non-technical stakeholders
- [x] All mandatory sections completed

## Requirement Completeness

- [x] No [NEEDS CLARIFICATION] markers remain
- [x] Requirements are testable and unambiguous
- [x] Success criteria are measurable
- [x] Success criteria are technology-agnostic (no implementation details)
- [x] All acceptance scenarios are defined
- [x] Edge cases are identified
- [x] Scope is clearly bounded
- [x] Dependencies and assumptions identified

## Feature Readiness

- [x] All functional requirements have clear acceptance criteria
- [x] User scenarios cover primary flows
- [x] Feature meets measurable outcomes defined in Success Criteria
- [x] No implementation details leak into specification

## Notes

- Items marked incomplete require spec updates before `/speckit-clarify` or `/speckit-plan`.
- Validation run 2026-07-13: all items pass. Zero [NEEDS CLARIFICATION] markers.
- Source swap during specification (verified against the live Socrata API on 2026-07-13). The
  second domain moved from taxi trips to traffic crashes. The configured taxi dataset
  (`gkne-dk5s`) is a fixed 2014 snapshot (min 2014-01-01, max 2014-12-31) with no date overlap
  with the live 311 feed (fresh to 2026-07-13), so a date-and-borough join would always be
  empty.
- Planning inputs for the traffic-crash source (verified 2026-07-13), for `/speckit-plan`:
  dataset `h9gi-nx95`, primary key `collision_id` (present and unique across all 2,269,187
  rows), freshness field `crash_date` (min 2012-07-01, max 2026-06-11), native `borough`
  column populated on roughly 1.58M of 2.27M rows, with the remainder mapping to the Unknown
  bucket.
- Borough value domains verified aligned (2026-07-13). Both datasets emit the same uppercase
  set: BROOKLYN, QUEENS, BRONX, MANHATTAN, STATEN ISLAND. 311 additionally emits `Unspecified`
  and null, crashes emit null. All non-canonical values map to the Unknown bucket, so the
  date-and-borough join needs no borough recoding beyond that mapping.
- Clarified 2026-07-13 (`/speckit-clarify`): the crash contribution is count plus severity
  (daily persons injured and persons killed alongside the crash count). FR-002 and FR-003 were
  updated, including a severity-normalized measure (311 complaints per person injured).
- Clarified 2026-07-13 (`/speckit-clarify`): landing the traffic-crash source is in scope for
  this feature (factory source entry, Asset wiring, dbt DAG subscription, source-count test
  update), not a prerequisite. Recorded in the spec's Clarifications and Assumptions.
- Watch item for planning: the 311-vs-noise overlap (noise is a filtered subset of 311) is
  intentional and kept as separate columns. Confirm this framing survives into the data model
  so consumers do not read the two counts as additive.
- Separate pre-existing defect surfaced (not part of this feature): the taxi entry in
  `include/sources.yaml` is broken independently of the mart. Its `freshness_field`
  (`tpep_pickup_datetime`) and `primary_key` (`trip_id`) are not real columns on `gkne-dk5s`.
  Track as its own fix.
