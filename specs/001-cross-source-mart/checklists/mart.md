# Cross-Source Mart Requirements Quality Checklist: Cross-Source Daily Mart

**Purpose**: Validate the quality of the requirements themselves (completeness, clarity,
consistency, measurability, coverage) for the cross-source mart, its orchestration, the taxi
retirement, and the portfolio narrative, before `/speckit-tasks` generates work.

**Created**: 2026-07-14

**Feature**: [spec.md](../spec.md)

**Scope note**: Items validate what is written in [spec.md](../spec.md), and flag where a
design decision in [plan.md](../plan.md), [research.md](../research.md), or
[data-model.md](../data-model.md) has outrun its backing requirement. Items test the
requirements, not the implementation and not the correctness of the design.

## Data Correctness & Semantics

- [x] CHK001 Is the `Unknown` borough bucket's exact literal value specified, given the canonical
  five are uppercase while the bucket is written in mixed case? [Consistency, Spec §FR-005]
- [x] CHK002 Does "standardizing casing" resolve to a named target casing, or is the target left
  to the implementer? [Clarity, Spec §FR-005]
- [x] CHK003 Is the meaning of a null `311 complaints per person injured` disambiguated between
  "no crashes occurred" and "crashes occurred but nobody was injured"? Both drive the
  denominator to zero and the measure to null, so one null carries two meanings.
  [Ambiguity, Spec §FR-003, §FR-006]
- [x] CHK004 Is a stated purpose or consuming measure defined for persons-killed? FR-002 mandates
  the metric, while FR-003 derives a measure only from persons-injured.
  [Completeness, Spec §FR-002, §FR-003]
- [x] CHK005 Does "at least one cross-source derived measure" establish whether both named
  measures are mandatory, or whether shipping one satisfies the requirement?
  [Ambiguity, Spec §FR-003]
- [x] CHK006 Is the "complaint unique key" used to deduplicate 311 and noise named as a specific
  verified field, in the way `collision_id` is named for crashes? [Clarity, Spec §Edge Cases]
- [x] CHK007 Is an enforcement expectation for reconciliation specified, or is it left to manual
  sampling? The mart contract lists reconciliation as a guarantee but names no enforcing test.
  [Gap, Spec §FR-009, §SC-003]
- [x] CHK008 Are requirements defined for whether derived measures apply to the `Unknown` borough
  bucket, or whether that bucket is excluded from them? [Gap, Spec §FR-005]
- [x] CHK009 Is "converted to America/New_York" clear about whether a conversion operation is
  required, given the sources already carry floating-local NYC timestamps and the design casts
  rather than converts? [Ambiguity, Spec §FR-001]
- [x] CHK010 Is the 311-and-noise overlap stated as a consumer-facing requirement, so the two
  counts cannot be read as additive? It currently appears only as an edge case and an
  assumption. [Completeness, Spec §Edge Cases, §Assumptions]

## Orchestration, Freshness & Idempotency

- [x] CHK011 Is a late-arrival reprocess window bounded by any requirement? The plan adopts a
  trailing 3-day window with no backing requirement, and the research decision concedes that
  records arriving later are missed. [Gap, Spec §FR-007]
- [x] CHK012 Does FR-007 define how far back a newer snapshot must refresh affected keys, or is
  the reprocess horizon left unspecified? [Clarity, Spec §FR-007]
- [x] CHK013 Is "changes zero existing rows" defined by observable value-identity or by physical
  no-op? A delete-and-insert over a trailing window rewrites rows with identical values.
  [Measurability, Spec §SC-004]
- [x] CHK014 Are requirements defined for expected behavior when a contributing source is
  chronically stale, including whether a timed-out freshness sensor is an acceptable outcome or
  a failure? The crash source lags roughly one month. [Gap, Spec §FR-008]
- [x] CHK015 Does FR-008 specify whether a refresh triggered by one source must recompute rows
  for dates the other sources have not yet reported? [Clarity, Spec §FR-008]
- [x] CHK016 Are the zero-versus-null requirements consistent between a source that has not yet
  reported and a source that reported genuine zero activity? FR-004 assigns zero to both.
  [Consistency, Spec §FR-004]
- [x] CHK017 Are data-coverage expectations specified in the requirements (which date range must
  contain co-occurring rows for the feature to be demonstrable), or does coverage live only in
  the plan's validation guide? [Gap, Spec §Assumptions]
- [x] CHK018 Is an acceptance expectation defined for how many rows may legitimately change on
  rebuild when a source publishes a correction? [Measurability, Spec §FR-007]

## Source Migration & Retirement

- [x] CHK019 Is any functional requirement defined for retiring the taxi source? The removal
  appears only in the spec's Assumptions and the plan's file-touch list, with no FR governing
  it. [Gap, Spec §Assumptions]
- [x] CHK020 Do the Clarifications and the plan conflict on the source-count test? The
  clarification lists a "source-count test update" as in scope, while the plan records that the
  count derives dynamically and needs no edit. The spec text remains uncorrected.
  [Conflict, Spec §Clarifications]
- [x] CHK021 Are requirements defined for what must NOT break when the taxi source is removed
  (existing marts, the DAG import contract, the source count)? [Gap, Completeness]
- [x] CHK022 Is the pre-existing broken taxi config explicitly placed in or out of this feature's
  scope in the spec itself? It is currently tracked only as a note in the spec quality
  checklist. [Clarity, Spec §Assumptions]

## Portfolio Narrative & Documentation

- [x] CHK023 Can "a reviewer can follow the multi-source modeling story end to end in under 5
  minutes" be objectively measured, or is it a subjective judgment? [Measurability, Spec §SC-006]
- [x] CHK024 Is any documentation deliverable required by a functional requirement? SC-006
  depends on "the repo documentation", yet no FR mandates producing or updating it and the
  plan's file-touch list includes no documentation file. [Gap, Spec §SC-006]
- [x] CHK025 Is the audience for SC-006 defined with enough precision to make the 5-minute target
  assessable? [Clarity, Spec §SC-006]

## Notes

- Check items off as resolved: `[x]`. A resolved item means the requirement was clarified,
  completed, or the gap was consciously accepted and recorded.
- Items are questions about the requirements, not tests of the implementation. Answering "the
  code does the right thing" does not resolve an item. Amending the spec does.
- Companion checklist: [requirements.md](./requirements.md) validates generic spec quality
  (16/16 passing). The present checklist validates domain-specific requirement quality and
  spec-to-plan drift.
- Highest-risk items on first read: CHK011 (an unbacked 3-day window), CHK024 (a success
  criterion with no producing requirement), CHK019 (an entire migration with no requirement),
  and CHK003 (one null value carrying two distinct meanings).

## Resolution log (2026-07-14)

All 25 items resolved by amending `spec.md`, then propagating to the plan artifacts so the
design no longer outruns the requirements. Nothing was waved through.

|Items|Resolution|
|---|---|
|CHK001, CHK002, CHK008|FR-005 now names the literal `Unknown`, mandates upper-casing, and states that `Unknown` is a full dimension member carrying the same measures|
|CHK003, CHK016|New FR-004 coverage semantics. A count is 0 only for published-and-empty, null for uncovered. FR-006 additionally enumerates the four distinguishable cases behind a null `311 complaints per person injured` (uncovered, covered-no-crashes, covered-crashes-no-injuries, covered-severity-unreported) and which columns tell them apart. ADR `2026-07-14-cross-source-mart-coverage-flags`|
|CHK004|FR-002 states persons-killed is a reported metric, not a denominator. Assumptions explain why (daily borough fatalities are usually 0, so a ratio would be null on most rows)|
|CHK005|FR-003 changed from "at least one" to mandating both named measures|
|CHK006|Edge case now names `unique_key`, verified against `nyc_311_pipeline.py` and `sources.yaml`|
|CHK007|FR-009 states reconciliation is verified by sampling, not by an exhaustive automated test, and SC-003 now defines the sample that makes it falsifiable: at least 10 keys spanning two or more boroughs, an `Unknown` key, a covered-overlap key, and an uncovered key. The contract, FR-009, and SC-003 all describe the same sampled guarantee|
|CHK009|FR-001 reworded from "converted to" to "is the ET civil day of", noting floating-local sources need no conversion step|
|CHK010|New FR-010 makes 311/noise non-additivity a consumer-facing requirement|
|CHK011, CHK012, CHK015, CHK018|FR-007 now bounds reprocessing by which source records are new, and explicitly forbids an event-date recency window. ADR `2026-07-14-cross-source-mart-incremental-strategy`|
|CHK013|SC-004 now defines "changes zero rows" as value-identity, not physical no-op|
|CHK014|FR-008 states a lagging source is not an error condition and must not block other sources, and now specifies the observable behavior when a freshness check finds nothing new: the stalled pipeline lands nothing (so its coverage frontier does not move), the model still builds on any other source's arrival with the stalled source's dates simply uncovered, and the stall must be distinguishable from a genuine failure so a by-design lag does not signal broken on every run|
|CHK017|Assumptions now require two concrete regions rather than a vague "overlap range": a fully-covered overlap of at least 30 consecutive days containing at least one row per canonical borough, and an uncovered region of at least 7 consecutive days past the crash frontier. Durations are the requirement, the quickstart's dates are the fixture (the lag moves them over time)|
|CHK019, CHK021|New FR-011 governs taxi removal and what must not break|
|CHK020|Spec Clarifications corrected in place. The plan's reconciliation note updated to match|
|CHK022|Assumptions now scope the pre-existing taxi config defect explicitly|
|CHK023, CHK025|SC-006 rewritten as a concrete, checkable test with a defined audience|
|CHK024|New FR-012 requires the documentation SC-006 depends on. `README.md` added to the plan's file-touch list|

Downstream edits made to keep the plan consistent with the amended spec: `research.md`
Decision 7 superseded and Decision 7a added, `data-model.md` mart shape and staging
obligations updated (`_loaded_at` now projected), `contracts/mart_cross_source_daily.md`
columns and guarantees rewritten, `quickstart.md` gained coverage and late-arrival checks, and
`plan.md` summary, structure, and reconciliation note updated.

### Review corrections (same day)

A CodeRabbit pass over the amendments caught three defects in the first draft of the new design,
all fixed before commit. They are recorded here because they are the kind of thing this
checklist exists to catch, and the first draft did not catch them:

- **Shared watermark was wrong.** A single `max(_source_loaded_at_hwm)` across all three sources
  would skip a crash batch landing with a `_loaded_at` older than 311's watermark. Watermarks
  are now per source.
- **Coverage transitions never reprocessed.** Flipping a previously-uncovered date to a true
  zero touches no source row, so the watermark filter alone left the flag stale forever. FR-007
  and the incremental filter now carry an explicit coverage-transition arm.
- **FR-007 was unsatisfiable as written.** Extraction is bounded by event date, so no forward
  run ever requests a month-old crash. FR-007 is now scoped to the model layer, with landing
  handled by running or re-running the record's interval (the documented backfill strategy), and
  the constraint is recorded as an assumption rather than left implied.

Also applied: the contiguous-publication assumption behind coverage is now explicit (a frontier
would wrongly mark an interior gap covered), Decision 4's stale unconditional coalesce-to-zero
text was corrected, SC-004's value-identity now excludes operational columns, and the sketch
gained tie-safe watermark comparison, coverage-normalized denominators, `coalesce(..., false)`
on empty-source coverage, and non-negativity plus `_loaded_at` not_null tests.

One finding was declined: a `not_null` test on the watermark column. It assumed a single shared
watermark. With per-source watermarks, a key reported by only one source legitimately has null
watermarks for the others, so the test would fail on correct data.

### Second review round (2026-07-14)

A further review pass produced eight findings. All were verified against the artifacts before
acting, and one was settled with live data rather than judgment.

**An assumption was refuted, not confirmed.** The design had summed severity as
`sum(coalesce(persons_injured, 0))`, with a documented assumption that `h9gi-nx95` omits the
field only when the true value is zero. Checking the live API disproved it: 18 of 2,269,187 rows
omit `number_of_persons_injured` and 8,913 omit `number_of_persons_killed`, and a sampled
omitting row (`collision_id` 4387369) carries `number_of_cyclist_injured: 1`, so a person was
injured while the field is simply absent. The coalesce would have fabricated an injury-free day
out of missing data, which is the same class of lie the coverage flags exist to prevent. Null now
means unreported and propagates to a null derived measure.

**The noise coverage flaw is fixed, not just documented.** Noise now derives its publication
frontier from 311's (noise is 311 filtered to noise complaint types, so 311's frontier is its
actual contract), instead of from `max(noise complaint_date)`, which would have read a citywide
zero-noise day as unpublished.

**A new limitation was accepted and recorded**: the incremental filter reaches only keys
reachable from current source records, so a record whose date or borough is revised leaves a
stale contribution on its prior key. Prior-key capture needs a snapshot, which exceeds the demo's
warrant. It is the sharpest cost of the ratified incremental design and is now named in the
spec's Assumptions and the plan's Complexity Tracking rather than left latent.

Also applied: FR-006 enumerates the four cases behind a null severity measure, SC-003 defines a
falsifiable sample, FR-008 specifies the observable stalled-source behavior, the CHK017
assumption states concrete region durations, and both derived measures gained
`expression_is_true` null-semantics tests.

### Third review round (2026-07-14)

**The severity semantics were verified properly and changed again.** The second round replaced a
`coalesce(severity, 0)` with a plain `sum()`, which still silently skipped unreported rows and
presented a partial total as the day's fact. Checking the live API settled it:

- NYC writes an explicit `0` when it means zero. Of the 903 demo-window rows reporting
  `number_of_persons_killed`, 895 are `0`. January 2020 has 14,349 explicit zeros and no nulls.
  A missing field is therefore a pending determination, not a sparse zero.
- The two severity fields diverge sharply. `number_of_persons_injured` has **zero** nulls in the
  demo window (18 in total, all 2016 to 2021). `number_of_persons_killed` is missing on
  **8,832 of 9,735 demo-window rows (90.7%)**, because recent fatality determinations lag.

Severity is now aggregated **all-or-null**: a day's total survives only when every crash row that
day reports the field. Recent borough-days will carry a null `persons_killed`, which is correct,
and independently vindicates the FR-002 decision not to derive any measure from persons-killed.
`complaints_per_person_injured` is unaffected, since injuries are always reported in the window.

**A derived-measure test was wrong.** `complaints_per_crash is null = (crash_count is null or
crash_count = 0)` fails on correct data when 311 is uncovered and crashes are covered: the ratio
is null via its numerator while the right side reads false. Both invariants now include the
numerator arm.

**FR-004's coverage is narrowed rather than generalized.** The frontier method is a proxy valid
only for sources that publish contiguously AND carry at least one event per published day
citywide. The spec now scopes FR-004 to those two conditions and names what a failing source
would look like, instead of implying a general mechanism. Landed-interval metadata would remove
the proxy, but Airflow owns which intervals ran and the DuckDB landing tables record only rows,
so a publication manifest stays deferred.

**One finding was skipped.** A request to capture prior grain keys before a revised `crash_date`
or `borough` is applied, so `delete+insert` can clear stale keys. It is not implementable where
it was asked: the landing table's `INSERT OR REPLACE` overwrites the record in place at load
time, so by the time dbt runs, the prior key is already destroyed and unrecoverable from the
model. Recovering it needs a dbt snapshot over the landing table, which is exactly the mutable-
grain limitation already recorded in the spec's Assumptions and the plan's Complexity Tracking,
and which the user ratified with that cost named.
