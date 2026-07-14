---
title: "Estimating the Unrecorded Game: Hierarchical Bayesian Coverage Models for a Century of Baseball Play-by-Play"
type: design-doc
status: draft
audience: sports-analytics researchers, applied Bayesian statisticians
last-verified: 2026-07-13
---

<!-- Structural deviation from tech-write templates: this is a research paper
(JQAS/AOAS register), not a design doc — sections follow the academic
convention (intro, data, methods, results, discussion) rather than a template.
<!-- src: ... --> comments are internal provenance scaffolding; strip before
external submission. Plan: notes/paper/OUTLINE.md. -->

# Estimating the Unrecorded Game: Hierarchical Bayesian Coverage Models for a Century of Baseball Play-by-Play

## Abstract

The play-by-play record of major-league baseball is nearly complete in
outcomes but selectively incomplete in detail: whether a scorer recorded a
ball's trajectory, location, or handler depends on era, scorer practice, and
the play's result, not on the play alone. Treating the record as the output of
two coupled processes — the game and its observation — we build hierarchical
Bayesian models over 18.1 million events (1910–2025) that estimate both the
propensity that each detail was recorded and posterior distributions over the
details themselves. Twelve probabilistic surfaces are defined — ten populated,
two deferred as typed zero-row frames — spanning observation propensity,
batted-ball geometry, fielding credit, park factors, run expectancy, base-out
transitions, and pitch summaries, each row carrying provenance and uncertainty.
Held-out calibration gates — expected calibration error ≤ 0.018 across
propensity dimensions — and posterior-predictive interval coverage gate
publication.

Three findings organize the paper. First, trajectory recording before 1988 is
missing not at random — ground balls appear at half their true share — and a
masked backtest refutes the natural learned correction: a coefficient on
observation propensity learns the selection effect with the wrong sign while
passing every convergence diagnostic. The identified fix is a fixed per-class
selection offset — a ground-ball selection log-odds of +1.24 before 1950,
anchored from the trajectory-deduction slice — published as a joint sensitivity
ribbon because its magnitude is unidentified: the pre-1950 unobserved
ground-ball share rises from 0.32 under MAR to 0.58 [0.56, 0.60] at the full
anchor. Masked backtests bound the correction — near-exact under class-only and
era-graded selection, partial under class-by-covariate selection, inert under
block absence. Second, shared entity-embedding pretraining over the full corpus
multiplies player-effect signal in downstream models by roughly 6×, under
cross-fitting and leakage gates that caught one real leak; an ablation shows the
deep covariate materially reshapes trajectory estimates yet is retained on
predictive grounds. Third, several natural estimands — scorer label confusion,
fielder responsibility, shift propensity — are unidentifiable from a
single-source record; we document why and publish nothing.

