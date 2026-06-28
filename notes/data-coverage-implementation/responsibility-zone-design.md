---
title: Fielder Responsibility Via Zone Kernels (Model I, Parked)
type: design-doc
status: parked
audience: humans-and-agents
last-verified: 2026-06-10
---

# Fielder Responsibility Via Zone Kernels (Model I, Parked)

## Why the first implementation was removed

The parked implementation trained on `ball_handler_position` — the recorded fielder who handled the batted ball, restricted to range positions 3–9. That makes it Model D (ball-handler imputation) with a narrower position vocabulary, not the estimand the spec defines. The spec's Model I (`03-hierarchical-models.md` §Model I) is an analytical opportunity model: `P(responsible = k | geometry, alignment, …)` — who should have had a play given where the ball went, marginal over who actually got to it. The spec explicitly distinguishes the handler from responsibility, and the review (`implementation-review.md` T1.3) found the confound empirically: 28% of the training slice is hits, where the "handler" is just the retriever (89% of hit-handlers are outfielders).

The handler label cannot identify responsibility. The recorded handler is the outcome of fielder positioning, ball location, and the fielding choice made on the play. Conditioning on it collapses the counterfactual question — who should have been responsible for this ball — to who touched it. A model trained on that label, no matter its covariates, can only reproduce `P(handled = k | event)`, which Model D already publishes. There is no responsibility label anywhere in the source data; the handler is the only position-valued label on a batted ball, so no relabeling or filtering fixes this within a single-label fit.

## The real design

Responsibility decomposes into geometry and a zone kernel:

```
P(responsible = k | event) = Σ_location P(location | event) · π(k | location, alignment)
```

- `P(location | event)` comes from Model E's geometry posteriors (already published; the parked implementation never consumed them).
- `π(k | location, alignment)` is a Dirichlet zone-responsibility kernel: for each location bin under a given alignment, a probability vector over the range positions, under a fielder-positioning prior. This is the input that does not yet exist in the pipeline and the reason the model is blocked rather than refit.
- Alignment is shift-gated on Model K (shift propensity): use the Model K posterior when it is identified for the cell, and fall back to the era-normal alignment prior when it is not (eras before Model K's observation support always use the era-normal prior). Model K is itself blocked, so the fallback is the initial operating mode.
- Validation runs on the high-coverage outs slice against official putout/assist patterns — the kernel's responsibility shares on balls converted to outs should track who officially made those plays — without ever overwriting official putouts, assists, errors, or double plays.

## Name reservation

The `responsibility_artifact_id` column in `model_input_advancement.sql` stays as a reserved NULL pointer for this future model. Nothing named responsibility ships until the zone-kernel design above is buildable.
