---
name: baseball-statistical-modeling
description: Use when Codex needs project-specific statistical model planning or implementation for baseball.computer coverage, imputation, scorer/source observation models, fielding credit allocation, batted-ball geometry, park factors, run values, runner or fielder advancement, pitch sequence models, machine-learning supplements, deep-learning supplements, posterior artifact design, validation backtests, or publication policy for probabilistic baseball estimates.
---

# Baseball Statistical Modeling

## Core Stance

Design the estimand, observation process, constraints, and validation path before choosing a model class. baseball.computer models should replace heuristic inference with explicit statistical assumptions while preserving official facts, deterministic derivations, and probabilistic estimates in separate namespaces.

Invariant: a posterior expected counter, a sampled imputed value, an official source value, and a deterministic derivation are different quantities.

Use this skill after `baseball-data-provenance` has classified the relevant source surfaces and missingness mechanism. Use it with `bayesian-hierarchical-modeling` for statistical design, `tabular-data-imputation` for missing-data strategy, `probabilistic-programming` for PyMC implementation, `sqlmesh` for pipeline integration, and `bc-schema` for schema lookup.

## Reference Map

Open only the files needed for the task:

- `references/model-surfaces.md`: existing heuristic surfaces, candidate model families, and readiness order.
- `references/implementation-contracts.md`: artifact boundaries, SQLMesh integration, validation gates, and publication policy.

Project docs to inspect selectively:

- `notes/statistical-modeling-coverage-design.md`
- `notes/data-coverage-taxonomy-1910-2025.md`
- `notes/measuring_fielding.md`
- `notes/followups.md`

## Workflow

1. Confirm the provenance inputs: source masks, artifact-risk flags, personnel reliability, context reliability, and exposure status.
2. Name the estimand and grain. Examples: event-dimension observation propensity, event-player official credit, event-position handler probability, park-season outcome effect, season-league run value, or event-runner advancement distribution.
3. Draw the process split: latent baseball event, official scoring process, observation/source process, and downstream metric process.
4. List constraints before likelihoods: outs, score transitions, box totals, eligible personnel, lineup state, source authority, rule regime, and additive counter conservation.
5. Decide whether the first output should be a deterministic ledger, posterior probability table, posterior draw table, calibrated ML proposal, or display summary.
6. Validate identifiability before fitting. Check whether era, league, source family, scorer, park, team, and roster context are connected enough to separate.
7. Start with the smallest module that has defensible anchors, then pass uncertainty forward as draws or probability vectors.

## Model Selection Defaults

| Problem | Default model family | Do not lead with |
| --- | --- | --- |
| Source and issue reliability | Deterministic ledgers plus hierarchical inclusion or artifact-risk models if needed. | Event-level value imputation. |
| Scorer/source missingness | Hierarchical logistic or multinomial observation model with era/source/scorer partial pooling. | Complete-case rates as truth. |
| Fielding credit allocation | Box-anchored constrained multinomial or Poisson allocation with eligibility masks. | Generic tabular imputation. |
| Handler and geometry | Hierarchical categorical model over broad positions and regions, with deterministic rules as measurements. | Detailed location labels before broad classes validate. |
| Park factors | Hierarchical park-season-league model with player/team/opponent/schedule and observation-bias controls. | Fixed pseudo-count park factors as final uncertainty. |
| Run values | Hierarchical run expectancy or linear weights with sparse season/league shrinkage. | Hard sample-size cutoffs with point fallback. |
| Advancement and responsibility | Hierarchical ordinal/categorical models conditioned on validated geometry and state. | Conditioning on post-treatment outcome labels as if causal. |
| Deep learning support | Calibrated proposals, embeddings, residual discovery, or sequence baselines. | Argmax labels published as facts. |

## Validation Gates

- Prior predictive checks on baseball-scale quantities.
- Simulation recovery for each latent estimand.
- Missingness backtests that mask observed data in realistic source, scorer, era, and block patterns.
- Calibration checks by era, league, scorer, source family, park, team, player role, and missingness regime.
- Conservation audits against game outs, score transitions, box totals, innings, line scores, and official aggregates.
- Sensitivity checks for MNAR mechanisms, source artifacts, scoring conventions, and weakly connected slices.

## Output Shape

For a model proposal, return:

- Estimand and unit.
- Observed data and masks.
- Latent variables.
- Likelihood and constraints.
- Pooling structure.
- Priors on interpretable baseball scales.
- Validation plan.
- Artifact tables and SQLMesh consumers.
- Assumptions that would invalidate the model.

For implementation planning, return:

- Build order and dependency DAG.
- Training frame contract.
- Posterior artifact contract.
- Smoke-run and full-run strategy.
- Acceptance criteria for publishing expected counters, probability tables, or draw tables.
