# Feature Specification: Cross-Source Daily Mart

**Feature Branch**: `001-cross-source-mart`

**Created**: 2026-07-13

**Status**: Draft

**Input**: User description: "Cross-source joined mart" (selected from the README roadmap: a
downstream model that joins the sources the factory lands, putting a multi-source modeling
story on top of the multi-source orchestration already shipped). The second domain was
switched from taxi trips to traffic crashes during specification. The originally-configured
taxi dataset is a fixed 2014 snapshot with no date overlap with the live 311 feed, so a
date-and-borough join would always be empty. Traffic crashes are daily-fresh into 2026 and
carry a native borough field, giving a real cross-domain join.

## Clarifications

### Session 2026-07-13

- Q: Does this feature include landing the new traffic-crash source, or just the mart on top of
  it? → A: Include landing the source (factory source entry, Asset wiring, dbt DAG
  subscription, and source-count test update), so the mart runs end to end on first build.
- Q: What should the crash contribution to the mart be? → A: Count plus severity, carrying
  daily persons-injured and persons-killed alongside the crash count, enabling a
  severity-normalized cross-source measure.

## User Scenarios & Testing *(mandatory)*

### User Story 1 - Compare NYC domains in one daily table (Priority: P1)

An analytics consumer wants a single daily table that places 311 complaints, traffic crashes,
and noise complaints side by side for each borough on each day, so they can compare activity
across domains without hand-joining three separate landed datasets.

**Why this priority**: The co-located metrics alone deliver the core value, a one-stop
cross-domain table keyed by date and borough. It is the minimum viable slice: every other
story builds on this grain, and this story is independently useful even if nothing else ships.

**Independent Test**: Land the three sources, build the model, and confirm a single query
returns one row per borough per day carrying each source's daily metric, matching values
computed by hand from each source's own daily aggregate.

**Acceptance Scenarios**:

1. **Given** all three sources have activity for a borough on a day, **When** the model is
   built, **Then** exactly one row exists for that date and borough with each source's daily
   metric populated.
2. **Given** a consumer queries one borough over a date range, **When** the query runs against
   the model, **Then** it returns one row per day with all source metrics present and requires
   no manual join.

---

### User Story 2 - A measure that only exists across sources (Priority: P2)

An analytics consumer wants at least one measure that is only computable by combining sources,
for example 311 complaints per traffic crash for a borough on a day, so the model demonstrates
modeling value beyond simply co-locating counts.

**Why this priority**: The derived measure is the payoff that justifies joining, but the table
from Story 1 is already usable without it, so it ranks second.

**Independent Test**: Compute the derived measure by hand from the per-source daily aggregates
for a sample borough and day, then confirm the model's value matches within rounding.

**Acceptance Scenarios**:

1. **Given** a borough and day with both complaints and crashes, **When** the model is built,
   **Then** the derived intensity equals complaint count divided by crash count.
2. **Given** a borough and day with zero traffic crashes, **When** the model is built, **Then**
   the derived measure is null rather than an error, and the row is retained.

---

### User Story 3 - Partial-source days stay complete (Priority: P3)

An analytics consumer wants the model to keep a row for a date and borough even when only one
source reported that day, showing absent sources' counts as zero rather than dropping the
row, so real-world gaps are visible instead of silently disappearing.

**Why this priority**: Correctness under real partial-freshness matters, but the model is
demonstrable on full days without it, so it ranks third.

**Independent Test**: Provide data for only one source on a given day and confirm the model
still produces that date and borough row with the reporting source populated and the other
sources' counts zero.

**Acceptance Scenarios**:

1. **Given** only 311 has data for a borough on a day, **When** the model is built, **Then** a
   row exists with 311 counts populated and crash and noise counts zero.
2. **Given** a source reports a borough that the others do not for a day, **When** the model is
   built, **Then** that borough still appears for that day.

---

### Edge Cases

- A record with a missing, blank, or unrecognized borough is attributed to a single explicit
  "Unknown" borough bucket, never dropped.
- A crash record with a missing or blank borough (a sizable share of crash records) falls into
  the "Unknown" bucket rather than being dropped.
- A calendar day that appears in one source but in none of the others still yields rows for
  the reporting source.
- A derived measure whose denominator source reported zero activity resolves to null, not a
  divide-by-zero error.
- Late-arriving or duplicate source records for the same day are deduplicated on each source's
  natural key (311 and noise use the complaint unique key, crashes use collision_id). A later
  version of a record replaces the earlier one rather than adding a second contribution, so
  counts never double-count.
- 311 and noise overlap by design (noise is a filtered subset of 311), so the two counts are
  kept as separate columns and are not netted against each other.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The model MUST produce a combined daily result at the grain of one row per
  calendar date and borough. The calendar date MUST be derived from each source's event
  timestamp converted to America/New_York (US Eastern), so all domains bucket into the same
  civil day.
- **FR-002**: For each date and borough, the model MUST carry the daily 311 complaint count,
  the daily traffic crash count, the daily persons-injured and persons-killed totals from
  crashes, and the daily noise complaint count, each sourced from the corresponding landed
  dataset.
- **FR-003**: The model MUST expose at least one cross-source derived measure that is only
  computable by combining two or more sources. The flagship measure is 311 complaints per
  traffic crash. The crash contribution also carries severity, so the model MUST additionally
  expose a severity-normalized measure, 311 complaints per person injured.
- **FR-004**: The model MUST retain a date and borough row whenever at least one source
  reported activity, representing a non-reporting source's count as zero (and any derived
  measure it feeds as null) rather than omitting the row.
- **FR-005**: Before joining, each source's borough value MUST be normalized by trimming
  whitespace and standardizing casing to the canonical set (BROOKLYN, QUEENS, BRONX, MANHATTAN,
  STATEN ISLAND). Values that are missing, blank, or outside that set (for example 311's
  "Unspecified") MUST be attributed to a single explicit "Unknown" bucket, never silently
  discarded.
- **FR-006**: A derived measure with a zero or missing denominator MUST resolve to null
  rather than an error, and MUST NOT cause the row to be dropped.
- **FR-007**: Rebuilding the model against the same source snapshot MUST produce identical
  rows, with no duplicates and no drift, so reruns and backfills are safe. When a newer snapshot
  has arrived for a source, only the affected date and borough keys are refreshed, never
  duplicated.
- **FR-008**: The model MUST refresh after new data lands for any one of its contributing
  sources, without waiting for all sources to be fresh.
- **FR-009**: Each per-source metric in the model MUST equal that source's standalone daily
  aggregate for the same date and borough, so the combined view reconciles to its inputs.

### Key Entities *(include if feature involves data)*

- **Cross-Source Daily Record**: one observation of activity for a single date and borough.
  Attributes: date, borough, per-source daily metrics (311 complaint count, traffic crash
  count, crash persons-injured and persons-killed totals, noise complaint count), and one or
  more derived cross-source measures.
- **Borough**: the shared spatial dimension across all three sources, including an explicit
  "Unknown" member for records whose location cannot be resolved.
- **Source Contribution**: the per-source daily aggregate that feeds one metric column of the
  combined record, and the unit against which reconciliation is checked.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An analyst can answer "how did 311 complaints and traffic crashes move together
  in a given borough over a given date range?" from a single table with no manual joins.
- **SC-002**: The model includes 100% of the date and borough combinations for which any
  source reported activity, with zero dropped keys.
- **SC-003**: For 100% of sampled date and borough keys, each per-source metric equals the
  source's standalone daily aggregate (reconciliation passes).
- **SC-004**: Rebuilding any previously built date range against an unchanged source snapshot
  changes zero existing rows.
- **SC-005**: For 100% of borough and day cases where a denominator source reported zero
  activity, the derived measure is null rather than an error.
- **SC-006**: A reviewer can follow the multi-source modeling story end to end, from three
  landed sources to one combined model, in under 5 minutes using the repo documentation.

## Assumptions

- The shared join grain is calendar date and borough, with borough as the common spatial
  dimension across sources.
- All source timestamps are treated as America/New_York (US Eastern) when deriving the calendar
  date, matching the civil timezone of the NYC datasets.
- Traffic crashes carry a native borough field, so borough attribution is direct with no
  geolocation derivation. Crash records with no borough value are attributed to the "Unknown"
  bucket rather than dropped.
- The contributing domains are 311 complaints, traffic crashes, and noise complaints. Traffic
  crashes replace the originally-listed taxi source, which is a dead 2014 snapshot with no
  overlap with the live 311 range. Adding a further source is out of scope for this feature.
- Landing the traffic-crash source is part of this feature, not a prerequisite assumed to
  exist. The feature delivers the crash source's daily landing and refresh, the mart's
  dependency on it, and the source-count bookkeeping that adding a source requires. The mart
  runs end to end on first build.
- The flagship derived measure is 311 complaints per traffic crash. The crash contribution also
  includes severity (persons injured, persons killed), supporting a severity-normalized measure
  such as 311 complaints per person injured. Further cross-source measures may be added later
  without changing the grain.
- 311 complaints and noise complaints overlap by design, because noise is a filtered subset of
  311. The two are represented as separate metric columns and are not netted against each
  other.
- Historical coverage begins from the same start date as the existing pipelines. A separate
  backfill of older history is out of scope.
- Consumers read the model through the same local warehouse the existing marts use. No new
  serving layer, dashboard, or external delivery is in scope for this feature.
