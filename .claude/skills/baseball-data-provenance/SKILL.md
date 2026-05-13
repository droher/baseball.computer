---
name: baseball-data-provenance
description: Use when Codex needs baseball.computer-specific source provenance, coverage, missingness, reliability, or data-quality context before analysis, SQL changes, statistical modeling, or imputation. Trigger for questions about source families, play-by-play versus box or gamelog coverage, source-block absence, parser/source artifacts, event observation ledgers, entity/personnel/context reliability, official scoring convention regimes, or how to classify missing baseball data before fitting models.
---

# Baseball Data Provenance

## Core Stance

Treat missing data as a provenance question before treating it as a modeling question. Identify the source grain, source family, authority order, missingness class, and artifact risk before proposing imputation, reconciliation, or statistical estimation.

Invariant: a missing or suspect value needs a source, method, and confidence or reliability flag before it can safely feed downstream analysis.

Use this skill with `bc-schema` for schema lookup, `sqlmesh` for model changes, and `baseball-statistical-modeling` when the provenance output becomes an input to a statistical model.

## Reference Map

Open only the files needed for the task:

- `references/source-surfaces.md`: source families, authoritative repo paths, and current coverage surfaces.
- `references/missingness-ledgers.md`: project missingness classes and recommended reliability ledger outputs.
- `references/reliability-workflow.md`: checklist for deciding whether a source field can train, constrain, weight, or mask an imputation model.

Project docs to inspect selectively:

- `notes/data-coverage-taxonomy-1910-2025.md`
- `notes/statistical-modeling-coverage-design.md`
- `notes/followups.md`
- `docs/llm/baseball.lsf`, using `grep` through the `bc-schema` skill rather than loading the whole file.

## Workflow

1. Name the target grain: season-team, game-team, player-game, event, event-player, or derived metric.
2. Identify the source family and authority surface: play-by-play, box score, gamelog, Databank-like supplement, seed taxonomy, derived model, or ML artifact.
3. Classify the missingness mechanism: structural absence, aggregate-only coverage, field-level unknown, selection-biased detail, cross-source disagreement, state reconstruction gap, official scoring convention gap, taxonomy collapse, or sparse-context estimation.
4. Separate source absence from source silence. A source-block absence is not an event-level missing field.
5. Separate observed-wrong risk from observed-missing risk. Known issue ledgers and audits should become masks, weights, or exception flags.
6. Check identity, personnel, context, and exposure reliability before using them as hard constraints.
7. Produce a ledger-shaped answer before a model-shaped answer.

## Output Shape

For analysis questions, return:

- Target grain.
- Candidate source surfaces.
- Missingness class.
- Authority order.
- Artifact or confounding risks.
- Recommended next validation checks.

For implementation planning, return:

- Proposed ledger table name.
- Grain and primary keys.
- Source columns.
- Reliability flags or probabilities.
- Downstream consumers.
- Conservation or audit checks.

## Project Rules

- Preserve raw source values separately from deterministic derivations and probabilistic estimates.
- Do not create hidden event facts from aggregate-only sources.
- Treat official scoring quantities as conventions when the rulebook or scorer judgment matters.
- Treat personnel eligibility as a hard constraint only after substitution and appearance reliability are checked.
- Treat park, handedness, weather, schedule, and game-completion fields as measured context, not automatically exact truth.
- Withhold or tag outputs as weakly identified when source, scorer, park, team, era, and league are inseparable in a slice.
