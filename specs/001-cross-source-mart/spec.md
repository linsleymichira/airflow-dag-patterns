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
  subscription, and keeping the source-count test green), so the mart runs end to end on first
  build. Corrected 2026-07-14: this answer originally said "source-count test update". The
  repo's `test_total_dag_count` derives the expected count dynamically, so replacing a source
  needs no test edit. The intent (the count test stays green) is unchanged.
- Q: What should the crash contribution to the mart be? → A: Count plus severity, carrying
  daily persons-injured and persons-killed alongside the crash count, enabling a
  severity-normalized cross-source measure.

### Session 2026-07-14

- Q: How should the model bound late-arriving data, given traffic-crash batches publish about a
  month behind 311? → A: Reprocess the keys touched by source records loaded since the last
  build, rather than a fixed calendar window. A recency window anchored on the freshest source
  would never reach the lagging source's dates.
- Q: How should the model distinguish "a source has not published this date yet" from "a source
  published and saw no activity"? → A: Carry a per-source coverage indicator. A count of zero
  means published-and-empty. An unpublished date carries a null count and an uncovered flag.

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
source reported that day, labelling each absent source as either "published, saw nothing" or
"has not published yet" rather than dropping the row, so real-world gaps are visible instead
of silently disappearing or masquerading as zeros.

**Why this priority**: Correctness under real partial-freshness matters, but the model is
demonstrable on full days without it, so it ranks third.

**Independent Test**: Provide data for only one source on a given day and confirm the model
still produces that date and borough row with the reporting source populated, and each other
source labelled either zero-and-covered or null-and-uncovered according to whether it has
published that date.

**Acceptance Scenarios**:

1. **Given** 311 and crashes have both published a date, and a borough has 311 complaints but
   no crashes that day, **When** the model is built, **Then** a row exists with the 311 count
   populated, the crash count zero, and crashes marked covered.
2. **Given** 311 has published a date but crashes have not yet published it, **When** the model
   is built, **Then** a row exists with the 311 count populated, the crash count null, and
   crashes marked uncovered.
3. **Given** a source reports a borough that the others do not for a day, **When** the model is
   built, **Then** that borough still appears for that day.

---

### Edge Cases

- A record with a missing, blank, or unrecognized borough is attributed to a single explicit
  `Unknown` borough bucket, never dropped.
- A crash record with a missing or blank borough (a sizable share of crash records) falls into
  the `Unknown` bucket rather than being dropped.
- A calendar day that appears in one source but in none of the others still yields rows for
  the reporting source.
- A derived measure whose denominator source reported zero activity resolves to null, not a
  divide-by-zero error.
- Late-arriving or duplicate source records for the same day are deduplicated on each source's
  natural key: 311 and noise both use `unique_key` (both read the same 311 dataset, noise being
  the subset filtered to noise complaint types), and crashes use `collision_id`. A later version
  of a record replaces the earlier one rather than adding a second contribution, so counts never
  double-count.
- 311 and noise overlap by design (noise is a filtered subset of 311), so the two counts are
  kept as separate columns and are not netted against each other (FR-010).
- A source publishes records whose event dates are far older than the current date (traffic
  crashes lag 311 by roughly one month). Those records still reach the model and refresh their
  own date and borough keys, rather than being skipped for being outside a recency window.
- A date that a source has not yet published is distinguishable from a date the source
  published with no activity. The first carries a null count and an uncovered flag, the second
  carries zero and a covered flag.

## Requirements *(mandatory)*

### Functional Requirements

- **FR-001**: The model MUST produce a combined daily result at the grain of one row per
  calendar date and borough. The calendar date MUST be the America/New_York (US Eastern) civil
  day of each source's event timestamp, so all domains bucket into the same civil day. Source
  timestamps that are already floating-local NYC values satisfy this directly, with no timezone
  conversion step required.
- **FR-002**: For each date and borough, the model MUST carry the daily 311 complaint count,
  the daily traffic crash count, the daily persons-injured and persons-killed totals from
  crashes, and the daily noise complaint count, each sourced from the corresponding landed
  dataset. Persons-injured and persons-killed are reported severity metrics in their own right.
  Persons-killed is not required to feed a derived measure. Each severity total MUST be
  evaluated for completeness against its own field: the persons-injured total against
  persons-injured, and the persons-killed total against persons-killed, independently of each
  other. A total MUST be reported only when every contributing crash record for that date and
  borough reports that field, and MUST be null otherwise. A partial total MUST NOT be presented
  as the day's figure, because summing across unreported records understates the day while
  looking like a fact.
- **FR-003**: The model MUST expose both of these cross-source derived measures, each of which
  is only computable by combining two or more sources: 311 complaints per traffic crash (the
  flagship measure), and 311 complaints per person injured (the severity-normalized measure).
  Further cross-source measures MAY be added later without changing the grain.
- **FR-004**: The model MUST retain a date and borough row whenever at least one source
  reported activity, never omitting the row. Each per-source metric MUST be accompanied by a
  coverage indicator distinguishing two cases that MUST NOT be conflated:
  - **Published and empty**: the source has published data for that date and observed no
    qualifying events. The count MUST be zero and the source MUST be marked covered.
  - **Not yet published**: the source has not published data for that date. The count MUST be
    null and the source MUST be marked uncovered.

  Severity totals follow the same covered/uncovered rule, with the empty group defined
  explicitly: a covered date and borough with **no** contributing crash records MUST report a
  severity total of zero, because that zero is a fact about a published day. A covered date and
  borough **with** crash records MUST report null whenever any of those records omits the field
  (FR-002). An uncovered date MUST report null regardless.
- **FR-005**: Before joining, each source's borough value MUST be normalized by trimming
  surrounding whitespace and upper-casing, yielding the canonical set (BROOKLYN, QUEENS, BRONX,
  MANHATTAN, STATEN ISLAND). Values that are missing, blank, or outside that set (for example
  311's "Unspecified") MUST be attributed to a single explicit bucket whose literal value is
  `Unknown`, never silently discarded. The `Unknown` bucket is a full member of the borough
  dimension: its rows carry the same metrics and derived measures as any canonical borough.
- **FR-006**: A derived measure MUST resolve to null rather than an error, and MUST NOT cause
  the row to be dropped, when its denominator is zero, missing, or uncovered. A null measure MUST
  be disambiguated by reading the row's other reported columns, which MUST make these cases
  distinguishable for `311 complaints per person injured`:
  - **Uncovered**: `persons_injured` is null and the crash source is marked uncovered. The
    denominator is unknown because the date is unpublished.
  - **Covered, no crashes**: `persons_injured` is 0, `crash_count` is 0, and the crash source is
    marked covered. Nobody was injured because nothing happened.
  - **Covered, crashes but no injuries**: `persons_injured` is 0 with `crash_count` above 0 and
    the crash source marked covered. Crashes happened and injured nobody.
  - **Covered, severity unreported**: `persons_injured` is null while the crash source is marked
    covered. At least one crash record that day did not report severity, which MUST NOT be
    represented as 0 nor silently summed around.

  A null numerator MUST also null the measure: when the 311 source is uncovered for a date, both
  derived measures are null regardless of their denominators.
- **FR-007**: Rebuilding the model against the same source snapshot MUST produce identical
  rows, with no duplicates and no drift, so reruns and backfills are safe. When newer source
  records have landed, the model MUST refresh exactly the date and borough keys those records
  touch, never duplicating them. The set of refreshed keys MUST be derived from which landed
  records are new since the last build, and MUST NOT be bounded by a fixed recency window on
  the event date: a landed record whose event date is arbitrarily old (traffic crashes lag 311
  by roughly one month) MUST still reach the model. The model MUST also refresh a key whose
  coverage has changed, even when no new record touches that key, so a newly-published date
  flips from uncovered to a true zero. FR-007 governs the model layer and takes landing as
  given. Getting a late-published record landed is the extract layer's
  responsibility, and happens when that record's interval is run or re-run (see Assumptions).
- **FR-008**: The model MUST refresh after new data lands for any one of its contributing
  sources, without waiting for all sources to be fresh. A refresh triggered by one source MUST
  NOT wait for, fail on, or discard rows because another source is behind. A source that has
  published nothing new is not an error condition for the model. The observable behavior when a
  source's freshness check finds nothing new MUST be:
  - That source's own pipeline run waits and then stops without landing data. It MUST NOT land a
    partial or empty result that would move the source's coverage frontier forward.
  - The model still builds on any other source's arrival, and the stalled source's dates simply
    remain uncovered (FR-004). Processing continues, the source is not skipped in a way that
    changes its already-built rows.
  - The stall MUST be distinguishable from a genuine failure in whatever failure reporting the
    pipelines already use, so a source that is merely behind does not read as broken. A source
    that lags by design (traffic crashes lag roughly one month) would otherwise emit a failure
    signal on every forward run, training the reader to ignore it.
- **FR-009**: Each per-source metric in the model MUST equal that source's standalone daily
  aggregate for the same date and borough **wherever that source covers the date**, so the
  combined view reconciles to its inputs. Reconciliation is scoped to covered metrics because
  coverage is a property of a source on a key, not of the key: where a source does not cover the
  date, its metric MUST be null (FR-004) while a standalone aggregate over the unpublished date
  returns zero, so an equality check there would fail on correct data. The uncovered case is
  therefore verified as a null-and-uncovered assertion rather than by equality. Reconciliation is
  verified by sampling keys (see SC-003), not by an exhaustive automated comparison.
- **FR-010**: The 311 complaint count and the noise complaint count MUST be presented as
  independent columns and MUST NOT be summed or netted against each other. Noise complaints are
  a filtered subset of the same 311 dataset, so the two counts overlap by design and adding
  them would double-count.
- **FR-011**: The retired taxi source MUST be removed rather than left in place: its factory
  source entry, its landed-data dependency, and its staging model MUST NOT remain after this
  feature. Removal MUST NOT break the existing single-source mart, the DAG import contract, or
  the factory source-count test.
- **FR-012**: The repository documentation MUST describe the cross-source model: the three
  contributing sources, the combined grain, the derived measures, and the runnable path that
  produces it. SC-006 depends on this documentation existing.

### Key Entities *(include if feature involves data)*

- **Cross-Source Daily Record**: one observation of activity for a single date and borough.
  Attributes: date, borough, per-source daily metrics (311 complaint count, traffic crash
  count, crash persons-injured and persons-killed totals, noise complaint count), a coverage
  indicator per source, and the derived cross-source measures.
- **Borough**: the shared spatial dimension across all three sources, including an explicit
  `Unknown` member for records whose location cannot be resolved. `Unknown` is a full member,
  not a discard bucket.
- **Source Contribution**: the per-source daily aggregate that feeds one metric column of the
  combined record, and the unit against which reconciliation is checked.
- **Source Coverage**: whether a given source has published data for a given date. Coverage is
  what separates a true zero from an unpublished date, and it is the reason a null metric or a
  null derived measure is interpretable rather than ambiguous.

## Success Criteria *(mandatory)*

### Measurable Outcomes

- **SC-001**: An analyst can answer "how did 311 complaints and traffic crashes move together
  in a given borough over a given date range?" from a single table with no manual joins.
- **SC-002**: The model includes 100% of the date and borough combinations for which any
  source reported activity, with zero dropped keys.
- **SC-003**: Reconciliation passes for 100% of a defined sample: at least 10 date-and-borough
  keys, drawn to include at least two distinct boroughs, at least one `Unknown`-borough key, at
  least one key inside the fully-covered overlap region, at least one key where a source covers
  the date but reported nothing, and at least one key where a source does not cover the date. For
  each sampled key, each source is checked independently against its own coverage flag: where the
  source covers the date its metric equals that source's standalone daily aggregate (including
  zero where it published and saw nothing), and where it does not cover the date its metric is
  null and its flag is false. The two checks are not interchangeable, since a standalone
  aggregate over an unpublished date returns zero while the model correctly returns null, so
  demanding equality there would fail on correct data. The sample is specified so the criterion
  is falsifiable: "100% of sampled keys" with an undefined sample would pass on a sample of one.
- **SC-004**: Rebuilding any previously built date range against an unchanged source snapshot
  leaves every existing row value-identical across its reported columns (the date, borough,
  per-source metrics, coverage flags, and derived measures). "Changes zero rows" is measured by
  comparing those values before and after the rebuild, not by whether the rebuild physically
  rewrites storage. A model that deletes and reinserts a row with identical values satisfies
  this. Operational bookkeeping columns (load and build timestamps) are excluded from the
  comparison, since refreshing them is how late-arrival tracking works.
- **SC-005**: For 100% of borough and day cases where a denominator is zero, missing, or
  uncovered, the derived measure is null rather than an error, and the row is retained.
- **SC-006**: The repository documentation lets a reader who has not seen the repo before
  identify, in under 5 minutes and without reading model code, all of: the three contributing
  sources, the combined grain, both derived measures, and the command sequence that builds the
  model. Verified by a reader who meets that description working only from the documentation.

## Assumptions

- The shared join grain is calendar date and borough, with borough as the common spatial
  dimension across sources.
- All source timestamps are treated as America/New_York (US Eastern) when deriving the calendar
  date, matching the civil timezone of the NYC datasets.
- Traffic crashes carry a native borough field, so borough attribution is direct with no
  geolocation derivation. Crash records with no borough value are attributed to the `Unknown`
  bucket rather than dropped.
- The contributing domains are 311 complaints, traffic crashes, and noise complaints. Traffic
  crashes replace the originally-listed taxi source, which is a dead 2014 snapshot with no
  overlap with the live 311 range. Adding a further source is out of scope for this feature.
- Landing the traffic-crash source is part of this feature, not a prerequisite assumed to
  exist. The feature delivers the crash source's daily landing and refresh, the mart's
  dependency on it, and the source-count bookkeeping that adding a source requires. The mart
  runs end to end on first build.
- Two derived measures are required (FR-003): 311 complaints per traffic crash (flagship) and
  311 complaints per person injured (severity-normalized). Persons-killed is carried as a
  reported metric without a derived measure of its own, because daily borough-level fatalities
  are frequently zero and a ratio over them would be null on most rows. Further cross-source
  measures may be added later without changing the grain.
- 311 complaints and noise complaints overlap by design, because noise is a filtered subset of
  311. The two are represented as separate metric columns and are not netted against each
  other.
- Historical coverage begins from the same start date as the existing pipelines. A separate
  backfill of older history is out of scope. For the feature to be demonstrable, the built model
  must contain two concrete regions, since one alone cannot show both the cross-source measures
  and the coverage semantics:
  - **A fully-covered overlap region of at least 30 consecutive days**, where 311 and traffic
    crashes have both published, so `complaints_per_crash` and `complaints_per_person_injured`
    are non-null. It must contain at least one row for each of the five canonical boroughs.
  - **An uncovered region of at least 7 consecutive days** past the crash publication frontier,
    where 311 has published and crashes have not, so `crash_count` is null with
    `crashes_covered` false.
  The crash publication lag (roughly one month) is what makes the second region available, and
  bounds the recent edge of the first. Concrete dates for the current data are in
  [quickstart.md](./quickstart.md), which the lag will move over time. The durations above are
  the requirement, the dates are the fixture.
- Retiring the taxi source removes a pre-existing defect as a side effect: the taxi entry in the
  factory config names a freshness field and a primary key that do not exist on the taxi
  dataset. Fixing that defect is in scope for this feature only in the sense that the entry
  ceases to exist. No effort is spent making the taxi pipeline work first.
- FR-007's refresh reaches keys reachable from current source records. A source record whose own
  date or borough is later revised moves to a new key, and its contribution to the prior key is
  not recomputed, because the landing table overwrites the record in place and the prior key
  becomes unreachable. [Assumption] Source records are treated as grain-stable. NYC does not
  guarantee this (an ungeocoded crash can gain a borough later), so this is an accepted
  limitation of the ratified incremental design rather than a property of the data. A full
  rebuild would not have the failure mode. Recorded in the plan's Complexity Tracking.
- Coverage (FR-004) is derived from each source's furthest event date as a proxy for its
  publication frontier, and is scoped to sources satisfying two conditions rather than claimed as
  a general mechanism. [Assumption] Each contributing source (a) publishes contiguously up to
  that date rather than leaving interior holes, and (b) is dense enough that every published day
  carries at least one qualifying event citywide. The three sources qualify: 311 and traffic
  crashes both occur daily citywide, and noise takes 311's frontier directly rather than deriving
  its own, since noise is 311 filtered to noise complaint types. A source failing (a) would be
  wrongly marked covered for a skipped date. A source failing (b) would read its most recent
  published day as uncovered whenever that day had no events. Adding a source that fails
  **either** condition requires an explicit per-date publication manifest instead of a frontier.
  Both conditions must hold, so one failure is enough to make coverage wrong.
- The extract layer requests each source's records by event date for the interval being run, so
  a record NYC publishes long after its event date is landed when that record's interval is run
  or re-run, not by a forward run on the current date. FR-007's guarantee therefore starts once
  a record is landed. The documented run strategy (backfill over the overlap window) is what
  lands the lagging crash history.
- Consumers read the model through the same local warehouse the existing marts use. No new
  serving layer, dashboard, or external delivery is in scope for this feature.
