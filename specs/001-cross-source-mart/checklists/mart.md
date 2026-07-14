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

- [ ] CHK001 Is the `Unknown` borough bucket's exact literal value specified, given the canonical
  five are uppercase while the bucket is written in mixed case? [Consistency, Spec §FR-005]
- [ ] CHK002 Does "standardizing casing" resolve to a named target casing, or is the target left
  to the implementer? [Clarity, Spec §FR-005]
- [ ] CHK003 Is the meaning of a null `311 complaints per person injured` disambiguated between
  "no crashes occurred" and "crashes occurred but nobody was injured"? Both drive the
  denominator to zero and the measure to null, so one null carries two meanings.
  [Ambiguity, Spec §FR-003, §FR-006]
- [ ] CHK004 Is a stated purpose or consuming measure defined for persons-killed? FR-002 mandates
  the metric, while FR-003 derives a measure only from persons-injured.
  [Completeness, Spec §FR-002, §FR-003]
- [ ] CHK005 Does "at least one cross-source derived measure" establish whether both named
  measures are mandatory, or whether shipping one satisfies the requirement?
  [Ambiguity, Spec §FR-003]
- [ ] CHK006 Is the "complaint unique key" used to deduplicate 311 and noise named as a specific
  verified field, in the way `collision_id` is named for crashes? [Clarity, Spec §Edge Cases]
- [ ] CHK007 Is an enforcement expectation for reconciliation specified, or is it left to manual
  sampling? The mart contract lists reconciliation as a guarantee but names no enforcing test.
  [Gap, Spec §FR-009, §SC-003]
- [ ] CHK008 Are requirements defined for whether derived measures apply to the `Unknown` borough
  bucket, or whether that bucket is excluded from them? [Gap, Spec §FR-005]
- [ ] CHK009 Is "converted to America/New_York" clear about whether a conversion operation is
  required, given the sources already carry floating-local NYC timestamps and the design casts
  rather than converts? [Ambiguity, Spec §FR-001]
- [ ] CHK010 Is the 311-and-noise overlap stated as a consumer-facing requirement, so the two
  counts cannot be read as additive? It currently appears only as an edge case and an
  assumption. [Completeness, Spec §Edge Cases, §Assumptions]

## Orchestration, Freshness & Idempotency

- [ ] CHK011 Is a late-arrival reprocess window bounded by any requirement? The plan adopts a
  trailing 3-day window with no backing requirement, and the research decision concedes that
  records arriving later are missed. [Gap, Spec §FR-007]
- [ ] CHK012 Does FR-007 define how far back a newer snapshot must refresh affected keys, or is
  the reprocess horizon left unspecified? [Clarity, Spec §FR-007]
- [ ] CHK013 Is "changes zero existing rows" defined by observable value-identity or by physical
  no-op? A delete-and-insert over a trailing window rewrites rows with identical values.
  [Measurability, Spec §SC-004]
- [ ] CHK014 Are requirements defined for expected behavior when a contributing source is
  chronically stale, including whether a timed-out freshness sensor is an acceptable outcome or
  a failure? The crash source lags roughly one month. [Gap, Spec §FR-008]
- [ ] CHK015 Does FR-008 specify whether a refresh triggered by one source must recompute rows
  for dates the other sources have not yet reported? [Clarity, Spec §FR-008]
- [ ] CHK016 Are the zero-versus-null requirements consistent between a source that has not yet
  reported and a source that reported genuine zero activity? FR-004 assigns zero to both.
  [Consistency, Spec §FR-004]
- [ ] CHK017 Are data-coverage expectations specified in the requirements (which date range must
  contain co-occurring rows for the feature to be demonstrable), or does coverage live only in
  the plan's validation guide? [Gap, Spec §Assumptions]
- [ ] CHK018 Is an acceptance expectation defined for how many rows may legitimately change on
  rebuild when a source publishes a correction? [Measurability, Spec §FR-007]

## Source Migration & Retirement

- [ ] CHK019 Is any functional requirement defined for retiring the taxi source? The removal
  appears only in the spec's Assumptions and the plan's file-touch list, with no FR governing
  it. [Gap, Spec §Assumptions]
- [ ] CHK020 Do the Clarifications and the plan conflict on the source-count test? The
  clarification lists a "source-count test update" as in scope, while the plan records that the
  count derives dynamically and needs no edit. The spec text remains uncorrected.
  [Conflict, Spec §Clarifications]
- [ ] CHK021 Are requirements defined for what must NOT break when the taxi source is removed
  (existing marts, the DAG import contract, the source count)? [Gap, Completeness]
- [ ] CHK022 Is the pre-existing broken taxi config explicitly placed in or out of this feature's
  scope in the spec itself? It is currently tracked only as a note in the spec quality
  checklist. [Clarity, Spec §Assumptions]

## Portfolio Narrative & Documentation

- [ ] CHK023 Can "a reviewer can follow the multi-source modeling story end to end in under 5
  minutes" be objectively measured, or is it a subjective judgment? [Measurability, Spec §SC-006]
- [ ] CHK024 Is any documentation deliverable required by a functional requirement? SC-006
  depends on "the repo documentation", yet no FR mandates producing or updating it and the
  plan's file-touch list includes no documentation file. [Gap, Spec §SC-006]
- [ ] CHK025 Is the audience for SC-006 defined with enough precision to make the 5-minute target
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
