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
Publication is gated on convergence and on held-out predictive lift over a
baseline; held-out calibration error and posterior-predictive interval coverage
are reported beside the gate as diagnostics.

Three findings organize the paper. First, trajectory recording before 1988 is
missing not at random: among pre-1950 batted balls whose trajectory the scorer
did not write, 763,993 are ground balls the fielding string alone identifies —
more than the whole recorded slice — putting a floor of 0.29 under the
unrecorded ground-ball share, while the missing-at-random fit scores those known
ground balls at 0.32. A masked backtest shows the natural learned correction — a
coefficient on observation propensity — is inert while passing every convergence
diagnostic; the correct form is a fixed per-class selection offset the observed
slice cannot identify, published as an assumed sensitivity band that the floor
constrains from below, not as a point. An earlier revision's data-anchored offset
is withdrawn as an identity of the observed slice. Second, shared
entity-embedding pretraining multiplies player-effect permutation importance in
the trajectory supplement by roughly 6× under a cross-fitting contract that
caught one real leak, and the deep proposal beats a class-prior baseline on
held-out log-loss for every dimension it feeds; a defect disclosed in this
revision — three location dimensions whose production rows carried no deep
prediction and were shifted by its absence — is corrected by publishing their
deep-free fits. Third, several natural estimands — scorer label confusion,
fielder responsibility, shift propensity — cannot be learned from a
single-source record; we document why and publish nothing.
