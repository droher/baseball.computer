---
title: Response to reviewers — Estimating the Unrecorded Game
type: design-doc
status: draft
audience: referee, editor, David
last-verified: 2026-07-14
---

# Response to reviewers

The revision adds the inferential evidence the report found missing and reframes
what the report found overstated. Concretely: a data-anchored MNAR offset
estimator (`bc/python_models/statistical/mnar_anchor.py`) replaces the
simulation-tuned δ grid; the marginal ribbon is superseded by a joint anchored
sensitivity ribbon that ships in the body rather than being deferred; the single
masked backtest becomes a four-design robustness study
(`bc/python_models/statistical/backtests/mnar_masked.py`); linear-weights bands
gain finite-sample Dirichlet propagation; held-out calibration (ECE) and
posterior-predictive HDI coverage are wired into the acceptance gate and swept by
`just validate-gates`; the `gamma_dl` counterfactual ablation was actually run —
and it refuted the paper's own prior "<0.25 SD" claim for the trajectory
dimension, which the text now reports honestly; and the related-work and
statistical-vs-pipeline framing are rewritten. What was not done this round is
stated plainly per point and collected at the end.

Each response names the module, script, table, or revised section that carries
the change and the headline number where one exists.

---

## M1. No calibration evidence anywhere, yet "calibrated" is the paper's central selling point.

> The acceptance gate scores discrimination (AUC) and fit (RMSE), never
> calibration; there is no reliability diagram, ECE value, or posterior-predictive
> check in the manuscript.

**Response:** The primitives existed but were unread; the gate now consumes them.
`validation/held_out_metrics.json` (written by the Bernoulli branch with
`roc_auc` / `pr_auc` / `ece_held_out`) is now read by `validate.py` and
`publication.py`, and equivalent held-out reliability/ECE computation was added to
the multinomial branch (Models E, D, C). Model A's six Bernoulli propensity
dimensions report held-out ECE ≤ 0.0179 (worst: trajectory 0.0179; best:
location_edge 0.0054), well inside the 0.05 band (revised §7, §9). A new
posterior-predictive HDI-coverage validator grades aggregate surfaces on held-out
season folds: `state_transition` predictive coverage 0.9772 (in band),
`run_expectancy` 0.8774 (below the 0.88 floor, fired at `warn`). The whole sweep
runs read-only via `just validate-gates`; results are tabulated in
`notes/paper/tables/validation_gates.md` (18/23 targets pass). The table also
documents why raw parameter-HDI coverage collapses to ~0.21/0.32 on a dense
corpus (the parameter interval of the mean is not meant to contain a
finite-sample frequency) and why the predictive interval is the correct object.

## M2. The MNAR conclusions rest on one self-designed masked backtest; robustness is absent.

> "Refuted," "the offset works," and the ribbon width all come from a single
> masking design the authors chose; the sign-flip and ribbon coverage may be
> artifacts of that one mechanism.

**Response:** The harness (`backtests/mnar_masked.py`, `MaskConfig`) now carries
four registered designs, and the oracle-offset arm was run against all four at the
smoke budget (`notes/paper/tables/mnar_backtest_robustness.md`). The relative
focal-error reductions are 0.995 (`w_class_intensity`, the original per-class
form), 0.958 (`era_graded` — class-marginal selection graded by season), 0.537
(`covariate_joint` — selection depends on class × `batter_hand`, a covariate the
geometry model conditions on, so the marginal-offset assumption is genuinely
violated), and 0.015 (`scorer_blocked` — whole-game/scorer-bucket blocking,
class-independent, so the per-class offset is ≈ flat and the reweight is a
near-no-op). This establishes the correction's scope by construction: exact when
selection is per-class (with or without an unconditioned era grade), only partial
when selection interacts with a conditioned covariate, and inapplicable to
class-independent block absence. The four-design ordering is the honest scope
statement the report asked for; revised §5 now presents the robustness study
rather than the lone simulation.

## M3. The published sensitivity ribbon is marginal, but the phenomenon is joint — the shipped uncertainty is known to be too narrow.

> Ship the joint offset-vector sweep, not the one-class-at-a-time ribbon, since
> you have shown the shift is joint and the reweight is refit-free; and justify the
> grid width from something external to the single simulation.

**Response:** The joint anchored ribbon ships in the body (revised §5.4;
`notes/paper/tables/joint_ribbon_trajectory.md`). `mnar_anchor.py` derives a
data-anchored per-era selection offset by contrasting the observed slice
(`observed_status='observed'`) against the derived slice (`observed_status='derived'`)
of `main_models.model_input_geometry`, and `joint_sensitivity_ribbon()` sweeps
`t · δ_anchor` for `t ∈ {0, 0.25, …, 1.5}` with ±0.25-nat per-class perturbations
at `t=1`, all as closed-form per-event renormalization (no refit). Headline: for
the pre-1950 unobserved slice the MAR default puts the GroundBall share at 0.3204;
the full anchor raises it to 0.5818 with a perturbation band of [0.5627, 0.5984] —
the MAR default understates pre-1950 ground balls by nearly half, and the graded
surface across `t` is the published object. This also answers the grid-width
objection: the offset magnitudes (+1.241 / +0.917 / +0.835 nats per era) are read
off the data, not tuned so one simulated truth lands inside a chosen band.

**Partial-identification caveat (stated in the paper):** trajectory deduction from
fielding strings recovers only ground balls, so the derived slice is a single
class and the anchor is a one-dimensional GroundBall-only softmax direction, not a
full per-class offset vector. Non-ground trajectory classes and rows with
`observed_status ∈ {unknown_code, missing}` remain uncharacterized by the anchor.
This is a property of the record, and §5.4 says so.

## M4. The paper conflates its engineering pipeline with its statistical contribution.

> Separate the transferable statistical method from the system build; the two §6
> negatives are ML-workflow QA; and the "~6× embedding signal" is an internal
> permutation-importance diagnostic — connect it to a downstream estimand
> improvement or demote it.

**Response — framing:** The intro and §10 now lead with the statistical
contribution (the event-dimension missingness ontology and the selection-model
treatment) and the two §6 negatives are labeled as ML-workflow QA findings, not
statistical results.

**Response — the ablation the report specifically flagged.** The report is
correct that the "moves posteriors < 0.25 SD" claim was unverifiable — the
`gamma_dl_zero` counterfactual it depends on had never been fit for any of the
four DL-consuming geometry dimensions. Those four fits were run
(`scripts/gamma_dl_shift.py` / `bc/python_models/statistical/gamma_dl_shift.py`;
`notes/paper/tables/gamma_dl_ablation.md`), and the diagnostic **fails the paper's
prior claim for trajectory**: 69.6% of publication-tier effect cells shift by more
than 0.25 posterior SD between the zero and shrunk fits (mean 0.74 SD, max 3.27
SD). The blanket claim cannot stand. The shrunk flavor nonetheless wins held-out
log-loss (−0.094), top-1 (+4.0 points on 616K held-out events), and macro PR-AUC
(+0.114), and wins log-loss and PR-AUC on all four dimensions, so it remains
published on predictive grounds — the DL covariate carries real signal, not
distortion. The paper now reports the measured per-dimension shifts (trajectory
0.696, location_side 0.217, location_depth 0.272, location_edge 0.391 of cells
over threshold) in place of the blanket claim, states that the DL logit materially
reshapes the trajectory model's effects, and keeps the "<0.25 SD on most cells"
characterization only for the three location dimensions. The review caught a real
unverified claim; the revision reports what the fit actually shows.

## M5. The identification claims for B, I, K are argued at very unequal rigor, and K is not an identification result at all.

> Separate "unidentifiable from this record" (B, I) from "not yet built" (K); for
> B, lead with the absence of an independent second label and drop the
> "collapses onto the prior" gloss that conflates small effect with non-identified.

**Response:** Revised §8 separates the three by claim type up front: B is an
identification limit, I a data-availability limit, K unfinished scope. The Model B
argument now leads with the load-bearing point — the deduced class is not an
independent second label, so the confusion matrix Ω is thinly informed off the
outcome-anchored sliver (702 disagreements in ~6M) and is non-identified, which is
a distinct statement from "confusion is rare"; the "collapses onto the prior"
gloss is dropped. Model I is framed explicitly as data-availability (the
zone-responsibility kernel does not exist in the source), not an identification
proof. Model K is described as "designed, not built" with no identification or
data-availability claim attached.

## M6. The headline findings are unverifiable from the paper and supplements.

> Only the 11 descriptive published-table queries are provided; the backtest, the
> γ-refutation, the oracle-δ recovery, the permutation-importance gate, the
> ablation, and every diagnostic are stated but not shown.

**Response:** The inferential evidence now ships as supplement artifacts and
runnable recipes (revised §12). The backtest harness and its
`metrics.json`/`mask_summary.json` outputs are reproduced by
`just mnar-backtest --mask-design {w_class_intensity,covariate_joint,scorer_blocked,era_graded}`
and tabulated in `mnar_backtest_robustness.md`; the joint ribbon by
`scripts/mnar_anchor.py` + `just sensitivity-ribbon --joint <anchor-dir>`
(`joint_ribbon_trajectory.md`); the gate reports by `just validate-gates`
(`validation_gates.md`); the ablation by `scripts/gamma_dl_shift.py`
(`gamma_dl_ablation.md`); and the finite-sample band comparison by the snippet in
`linear_weights_width_comparison.md`. All are read-only against the published
pointers and write no `bc.db` state. The descriptive claims were already
checkable; the inferential ones now are too.

## M7. Every published surface is `exploratory`; none has cleared the paper's own gate.

> Reconcile shipping `exploratory` results with arguing the gate is the
> discipline; state precisely what fails the `passed` gate.

**Response:** Revised §7 and §9 reconcile this. `confidence_status` is stamped
once at publish time from the manifest's `validation_status` and does not update
retroactively; the gate suite that would move it postdates every current stamp, so
passing the suite (18/23 targets) is necessary but not sufficient — a table's
status only advances on re-publication, which has not happened for any table in
this paper. The two concrete gate results are stated: `geometry_location_depth`
fails on top-1 accuracy (0.5609 vs a 0.5611 majority baseline on a `Default`-
dominated dimension) but is dispositioned — its calibrated shares match held-out
empirical shares to a total-variation distance of 0.0051 with a +0.0403-nat
log-loss lift, so the block correctly warns against its arg-max while the shares
are fit for probabilistic use; and `run_expectancy` posterior-predictive coverage
is 0.8774, below the 94% target's 0.88 floor and flagged at `warn`
(`state_transition` is 0.9772, in band). The results are framed as provisional
pending the re-stamp.

## M8. Related work is inadequate for a journal and omits the literatures the paper rediscovers.

> The §5 selection process is Heckman's selection-model tradition; the ribbon is
> delta-adjusted / tipping-point pattern-mixture sensitivity analysis; the survey
> informative-nonresponse literature is on point. All uncited.

**Response:** §10 is rewritten to position the contribution against these
literatures. Added: Heckman (1976, 1979) for the selection-model formalization
of §5; Scharfstein–Rotnitzky–Robins (1999), Molenberghs & Kenward (2007), and
Cro–Morris–Kenward–Carpenter (2020) for the delta-adjustment / tipping-point /
pattern-mixture sensitivity lineage the ribbon instantiates; Little (1993)
repositioned within that lineage rather than cited alone; and Groves–Dillman–
Eltinge–Little, *Survey Nonresponse* (2002) for the informative-nonresponse
stance toward the scorer as an observation process. The section states what is
methodologically new (event-dimension-level missingness assignment plus a
data-anchored offset on an unusually vivid record) versus what is an application
of established delta-adjustment sensitivity analysis.

## M9. HDI coverage under sparsity is asserted, not validated.

> Wider intervals under less data are necessary but not sufficient; and the
> disclosed `linear_weights_estimated` bands that ignore finite-sample cell
> uncertainty are a concrete instance of intervals that are not honest under
> sparsity, contradicting the park-factor narrative.

**Response:** The coverage assertion is now validated by the same held-out
posterior-predictive machinery as M1 (revised §7). The `linear_weights_estimated`
gap is fixed at the source: `propagate_linear_weights_draws` now draws per-(season,
league, play) play-frequency vectors from Dirichlet(counts + 0.5) (Jeffreys), one
draw per RE-posterior draw, so sparse cells widen automatically while dense cells
are unchanged to first order. Band width is non-decreasing on 99.57% of cells
(0.43% narrowed), median width ratio 1.5588, p90 4.1936, with the largest
widenings on the smallest-n cells (10 largest at median n=21 vs overall median
n=544) — `notes/paper/tables/linear_weights_width_comparison.md`. §11 reconciles
this with the park-factor narrative. **This is validated in dev and has NOT yet
been restated into prod;** the published artifact still carries the old fixed-count
bands until a restate, and §11 says so.

---

## Minor comments

### Minor 1. Model counting is inconsistent.

**Response:** The intro now states the counting once and the paper uses it
consistently: twelve published surfaces, ten populated and two deferred as typed
zero-row frames; seven model letters published (A, C, D, E, F, G, J), three
withheld with documented identification failures (B, I, K), and Model H deferred
on upstream data (§11).

### Minor 2. Span inconsistency.

**Response:** The intro states the actual event universe — 18,141,020 events over
1910–2025 across 205,845 games — and distinguishes it from the season span of the
descriptive/pooled tables (park factors and Model G's 1901+ pooling), reconciling
the pre-1910 descriptive rows against the event universe.

### Minor 3. Garbled sentence.

**Response:** "Model D is Model D's first publication" is repaired in the revised
models section.

### Minor 4. Units.

**Response:** §5 states once that δ_c is a natural-log log-odds in nats and uses
that consistently for both the offset and the grid.

### Minor 5. 94% HDI.

**Response:** §7 adds one sentence stating why the 94% default is used, so the
convention is motivated rather than silently applied to every interval.

### Minor 6. Illustrative tables.

**Response:** §7 labels extreme-only tables (RE states, top/bottom park-seasons)
as illustrative and moves full surfaces to the supplement where a claim depends on
the whole surface (e.g. linear-weights agreement across all play types).

### Minor 7. Define s(c,x).

**Response:** §5 gives the range s(c,x) ∈ (0,1] and states explicitly that the
class-independent part of the selection logit cancels under the per-event softmax,
cross-referencing the same invariance that kills the scalar random effect.

### Minor 8. Tone.

**Response:** The editorializing asides ("exactly where a fan would expect it,"
"a lesson worth its own telling," "the offset is right, and the offset is
unidentified") are trimmed to journal register in the writing pass.

### Minor 9. Section length.

**Response:** "Published surfaces" is compressed to the tables that carry an
argument; the data-tour material moves to the supplement.

### Minor 10. Provenance comments.

**Response:** The `<!-- src: … -->` scaffolding and internal repo paths are
stripped in the submission build, so no uncheckable internal-path pseudo-citations
survive into the manuscript.

---

## Questions to the authors

Each restates a major; the concrete answer lives in the referenced response above.

1. **Calibration evidence** — see M1: held-out ECE ≤ 0.0179 (Model A), predictive
   coverage 0.9772 / 0.8774 (`validation_gates.md`).
2. **Does the MNAR result survive alternative masking designs** — see M2:
   four-design study, relative reductions 0.995 / 0.958 / 0.537 / 0.015.
3. **Joint offset-vector sweep** — see M3: joint anchored ribbon, pre-1950
   0.3204 → 0.5818 [0.5627, 0.5984], shipped in §5.4.
4. **Basis for the grid width / principled upper bound** — see M3: the offset is
   now the data-anchored derived-vs-observed contrast (+1.241 / +0.917 / +0.835
   nats), not a band tuned to one simulation; the four-design backtest bounds the
   correction's scope.
5. **Is Model B "Ω ≈ identity" or "Ω non-identified"** — see M5: non-identified,
   argued from the absence of an independent second label; the rarity gloss is
   dropped.
6. **Why is Model K grouped with B and I** — see M5: it no longer is; K is
   "designed, not built," separated from the identification/availability limits.
7. **What fails the `passed` gate; provisional framing** — see M7:
   `geometry_location_depth` top-1 (dispositioned) and `run_expectancy` predictive
   coverage 0.8774; status advances only on re-publication, which has not happened.
8. **Release backtest harness, gate reports, ablation tables** — see M6: shipped as
   supplement tables plus `just` recipes.
9. **Does the 6× translate into a measured estimand improvement** — see M4: the
   `gamma_dl` ablation was run; the shrunk fit wins held-out log-loss/PR-AUC on all
   four dimensions and top-1 on trajectory, but it moves trajectory
   publication-tier effects well past 0.25 SD on 69.6% of cells, refuting the
   paper's prior "<0.25 SD" claim, which the text now reports honestly.
10. **Reconcile park-factor "honest uncertainty" with the too-tight linear-weights
    bands** — see M9: Dirichlet(counts + 0.5) finite-sample propagation, median
    width ratio 1.5588, validated in dev, not yet restated in prod.

---

## Remaining limitations

Several items are honestly still open. The `confidence_status` re-stamp across
published artifacts has not run, so every published table still reads
`exploratory` even though 18/23 targets clear the gate suite; `run_expectancy`
posterior-predictive coverage (0.8774) sits just below the 94% target and is
flagged rather than resolved; the linear-weights Dirichlet bands are validated in
dev but not yet restated into prod; and the trajectory anchor is a partial-truth,
single-class (GroundBall-only) direction, so non-ground classes and
`unknown_code`/`missing` rows remain uncharacterized. Per REVISION-PLAN.md, the
Model H buildout, the error-credit and DP unblocks, a joint EM selection model,
and Statcast second-source ingestion are out of scope this round; the first would
need upstream SQL, the middle two need signal/truth columns that do not exist, and
the last two would either contradict the paper's stance or live in the parser repo.
