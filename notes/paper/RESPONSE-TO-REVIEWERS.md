---
title: Response to reviewers — Estimating the Unrecorded Game
type: design-doc
status: draft
audience: referee, editor, David
last-verified: 2026-09-04
---

# Response to reviewers

This letter has two layers. The first revision (2026-07-14) answered the
referee report with new modeling: a masked-backtest robustness study, held-out
calibration and posterior-predictive coverage checks, finite-sample propagation
in the linear-weights bands, and a counterfactual `gamma_dl` ablation. A
subsequent internal modeling review (2026-09-03) found that several of the
first revision's answers overstated what had been done — an MNAR "anchor" that
was an identity of the observed slice, an ablation whose artifacts were not on
disk, gate results quoted from smoke-budget runs without saying so, and three
published location surfaces shifted by a missing covariate. The second revision
corrects the code, refits the affected models, and rewrites the text; the
refits completed and the tables were restated on 2026-09-04, and every number
in the manuscript is read from the restated tables. Each response below says
what the paper now claims and what was withdrawn.

Each response names the module, script, table, or revised section that carries
the change.

---

## M1. No calibration evidence anywhere, yet "calibrated" is the paper's central selling point.

> The acceptance gate scores discrimination (AUC) and fit (RMSE), never
> calibration; there is no reliability diagram, ECE value, or posterior-predictive
> check in the manuscript.

**Response:** Held-out expected calibration error is computed for every
Bernoulli and multinomial target (`validation/held_out_metrics.json`, read by
`validate.py`), and a posterior-predictive HDI-coverage validator
(`hdi_coverage.py`) grades the two aggregate surfaces that have a coverage hook,
`state_transition` and `run_expectancy`, on the held-out game fold. Both are
reported in revised §7. Two things the first revision's letter implied are
corrected. Neither check gates publication: ECE and predictive coverage are
`warn`-only findings, and the paper now says so rather than calling them gates.
And the paper no longer sells itself as "calibrated": the abstract and §1 say
publication is gated on convergence and held-out predictive lift over a
baseline, with calibration error and coverage reported as diagnostics. The
previously published values (Model A ECE 0.0054–0.0179; predictive coverage
0.9772 for transitions, 0.8774 for run expectancy, the latter below the 0.88
floor) are quoted as the previous fits' numbers; the six propensity dimensions
and both Model G surfaces were refit, the latter on a corrected population with
a per-state dispersion, which the review traced part of the under-coverage to.
The refit values are Model A ECE 0.0049–0.0181, predictive coverage 0.9780 for
transitions and 0.9062 for run expectancy, both inside the band. Park factors have no coverage hook and
the paper says so (§7, §11).

## M2. The MNAR conclusions rest on one self-designed masked backtest; robustness is absent.

> "Refuted," "the offset works," and the ribbon width all come from a single
> masking design the authors chose; the sign-flip and ribbon coverage may be
> artifacts of that one mechanism.

**Response:** The harness (`backtests/mnar_masked.py`, `MaskConfig`) carries
four registered designs and the oracle-offset arm was run against all four
(`notes/paper/tables/mnar_backtest_robustness.md`). The first revision presented
the four relative reductions (0.995, 0.958, 0.537, 0.015) as a scope statement.
Revised §5 presents them with two caveats the first revision omitted. First,
three of the four designs cannot fail: when selection depends on class alone (or
on nothing), `P(c | R=0) ∝ P(c | R=1) · odds_mask(c)` holds at the marginal level
for any per-event shares, so the oracle offset reproduces the masked marginal by
construction; the ≈exact and ≈no-op rows are properties of those designs, and
only `covariate_joint` (0.537) tests the offset against a selection process it
does not encode. Second, every run is a `SMOKE_BUDGET` fit (50 draws × 50 tune ×
2 chains) whose convergence gates fail (ESS 20–47), and no run artifacts are
checked in; the reproducibility command now carries `--smoke`. The "sign-flip"
is also withdrawn: under the code's sign convention (`z` is the standardized
logit of P(observed), so masked events have low `z`) the learned
`gamma_GB = −0.083` points in the direction a correction needs; the arm is inert
(relative reduction −0.001), not wrong-signed. The refutation stands on the
inertness, not on a sign. Rerunning the four designs at full budget with
artifacts checked in is listed as open work (§11).

## M3. The published sensitivity ribbon is marginal, but the phenomenon is joint — the shipped uncertainty is known to be too narrow.

> Ship the joint offset-vector sweep, not the one-class-at-a-time ribbon, since
> you have shown the shift is joint and the reweight is refit-free; and justify the
> grid width from something external to the single simulation.

**Response:** The first revision answered this with a "data-anchored" per-era
offset (`+1.241 / +0.917 / +0.835` nats) and a joint ribbon along it, headlining a
pre-1950 unrecorded ground-ball share of 0.5818 [0.5627, 0.5984]. That answer is
withdrawn. The derived slice the anchor contrasted against the observed slice is
100% GroundBall, so the "anchor" `log(p_derived / p_obs)` was `−ln(p_obs_GB)` —
`−ln(0.289 / 0.400 / 0.434)` exactly — a function of the observed slice alone that
carried no information about the unrecorded slice; the 0.58 was `1/(2 − p_obs)`
up to renormalization, and the joint ribbon moved non-focal classes only through
renormalization of a single-class offset. `mnar_anchor.py` now computes what the
derived slice does support: a hard lower bound `P(GB | unrecorded) ≥ n_derived /
n_unrecorded` per era (0.292 / 0.339 / 0.400) and a known-truth diagnostic — the
MAR export's mean p(GB) on the derived rows (0.32 / 0.40 / 0.38 against a truth
of 1). Revised §5 publishes the marginal ribbon as an explicitly assumed ±1.0-nat
band, reports the offset at which the ribbon reaches the floor (−0.15 / −0.19 /
+0.04 nats: inside the grid in every era, and binding in 1988+ where MAR sits
0.008 below the floor), and states that the band is an assumption, not a
data-identified interval. On grid width: the width was checked once against the
synthetic mask's oracle correction, which lands inside ±1.0, and the paper says
that calibrates the width against one synthetic process and identifies nothing
about the real one. We do not have a joint data-identified sweep to offer; the
record supports a floor on one class, and the paper claims exactly that.

## M4. The paper conflates its engineering pipeline with its statistical contribution.

> Separate the transferable statistical method from the system build; the two §6
> negatives are ML-workflow QA; and the "~6× embedding signal" is an internal
> permutation-importance diagnostic — connect it to a downstream estimand
> improvement or demote it.

**Response — framing:** §1 leads with the statistical contributions and §10
positions them; the two §6 negatives are labeled in the text as ML-workflow
quality findings, not statistical results.

**Response — the ablation.** The first revision's letter reported four
`gamma_dl_zero` counterfactual fits and a per-cell shift diagnostic (trajectory
69.6% of cells over 0.25 SD, etc.). The review found that the zero-fit artifacts
and the `gamma_dl_shift.json` files those numbers came from are not on disk; only
the fit logs survive, and they record convergence and top-1, not log-loss or the
shift statistic. The numbers are therefore unreproducible and revised §6 says so
rather than quoting them. Two further corrections: `gamma_dl` is one scalar
shared across classes (the previous draft wrote `γ_c` per class), with a
`N(0, 0.5)` prior whose posterior sd is ~0.03 — the prior is inert, so "shrunk"
is a flavor name, not a mechanism; and the ablation did not guide the published
flavor, because the zero fits were run on 2026-07-14 after the shrunk fits had
been published. This revision refit both flavors for every dimension, and §6 reports the
held-out comparison and shift diagnostic from artifacts that are on disk
(`e-v12-noprop-*-zero`, each with `validation/gamma_dl_shift.json`). On the 6×: the deep proposals are now measured against a
baseline for the first time — TEST log-loss against a per-`result_family` class
prior is 1.229 vs 1.258 (trajectory), 0.996 vs 1.035 (side), 1.094 vs 1.114
(depth), 0.899 vs 0.922 (edge) — a real, modest downstream improvement, reported
in §6 next to the permutation-importance diagnostic rather than in place of it.

**Disclosed defect.** The review also found that the three location dimensions'
production rows carried no deep prediction (the location DL specs score only
observed rows), and that the covariate's centering turned that absence into a
constant per-class shift on every imputed logit; the published `location_edge`
`All` share was 0.173 against a training share of 0.009. The covariate now
contributes zero on rows without a prediction, a fit whose production slice has
none must publish the deep-free flavor, and the three dimensions are refit
deep-free. §6 reports this as a defect fixed in this revision.

## M5. The identification claims for B, I, K are argued at very unequal rigor, and K is not an identification result at all.

> Separate "unidentifiable from this record" (B, I) from "not yet built" (K); for
> B, lead with the absence of an independent second label and drop the
> "collapses onto the prior" gloss that conflates small effect with non-identified.

**Response:** Revised §8 separates the three by claim type up front and uses one
claim type for Model B throughout: the data are uninformative about Ω — the only
outcome anchors disagree with the recorded label on 702 of 6.0M events with no
era or scorer structure, so any fit returns the prior. The first revision called
this both a "formal identification limit" and "uninformative"; the paper now says
the latter only, and explicitly not a formal non-identification proof, since the
anchored sliver does move the posterior, just not by enough to matter. Model I is
a data-availability limit (the zone-responsibility kernel does not exist in the
source); Model K is "designed, not built" with no identification claim attached.
§4 adds the letter → estimand → published table → tier → status table the
report's Minor 1 asked for.

## M6. The headline findings are unverifiable from the paper and supplements.

> Only the 11 descriptive published-table queries are provided; the backtest, the
> γ-refutation, the oracle-δ recovery, the permutation-importance gate, the
> ablation, and every diagnostic are stated but not shown.

**Response:** The recipes ship (revised §12): `just validate-gates` for the gate
table, `scripts/mnar_anchor.py` plus `just sensitivity-ribbon --bound` for the
derived-slice bound and ribbon, `just mnar-backtest --smoke --mask-design ...`
for the backtest. What does not yet ship, stated plainly: the backtest run
artifacts (`metrics.json`, `mask_summary.json`) are not checked in, so the
robustness numbers cannot be re-checked without rerunning; the `gamma_dl`
ablation artifacts the first revision cited were not on disk and were
regenerated in this revision; and the four deep-proposal pointers are not validated by the gate
sweep (it resolves them under a layout they do not use and reports `missing`).
The permutation-importance gate is documented in
`notes/data-coverage-implementation/phase3-acceptance-gates-v6.md` with its log
paths; the linear-probe margins the previous draft quoted for the proxy-metric
negative result have no repository source and are marked `TODO: unverified`.

## M7. Every published surface is `exploratory`; none has cleared the paper's own gate.

> Reconcile shipping `exploratory` results with arguing the gate is the
> discipline; state precisely what fails the `passed` gate.

**Response:** Revised §9 gives the column's mechanics and its history. The stamp
is copied at materialization from the manifest's `validation_status`, which only
`just validate-gates --write` sets, and it publishes as `passed` only when the
gate version that graded it is the current one. The tables were restated to
`passed` on 2026-07-30 under the first sweep — the first revision's letter, which
said the re-stamp had not run, was overtaken — this revision's gate-version bump
reverted every table to `exploratory`; and after the version-2 sweep and the
restate of 2026-09-04 every populated table reads `passed` again. What changed
in the gate: the publish path refuses
smoke fits; a full-scale fit with no held-out evidence blocks; the multinomial
gate blocks on held-out log-loss against the pooled held-out marginal's entropy
(the `geometry_location_depth` top-1 block the report asked about was the gate
grading an argmax the policy bans, and it passes on log-loss by 0.040 nats);
and `weak_identification_flag` is derived from group-level diagnostics
(variables with at most 512 elements) and recomputed by the sweep, so the
run-expectancy table is no longer flagged on a per-cell element. The
park-factor table is not flagged either, on evidence: its non-centered
gap-aware refit mixes the persistence hyperparameters at bulk ESS 3,261 and
1,891. The flag is TRUE on `state_transition_summary`, on the assist rows of
`imputed_fielding_credit`, and on four of the six observation dimensions
(`ball_handler_position`, `location_depth`, `location_edge`, `trajectory`) in
`scorer_observation_propensities`, and FALSE elsewhere (§11).

## M8. Related work is inadequate for a journal and omits the literatures the paper rediscovers.

> The §5 selection process is Heckman's selection-model tradition; the ribbon is
> delta-adjusted / tipping-point pattern-mixture sensitivity analysis; the survey
> informative-nonresponse literature is on point. All uncited.

**Response:** §10 positions the contribution against these literatures: Heckman
(1976, 1979) for the selection-model formalization; Scharfstein–Rotnitzky–Robins
(1999), Molenberghs & Kenward (2007), and Cro–Morris–Kenward–Carpenter (2020)
for the delta-adjustment / tipping-point lineage the ribbon instantiates; Little
(1993) within that lineage; Groves–Dillman–Eltinge–Little (2002) and Rubin
(1977) for informative nonresponse. The sentence describing what the paper adds
now says a floor and a known-truth subslice, not an anchor, since the anchor is
withdrawn (M3).

## M9. HDI coverage under sparsity is asserted, not validated.

> Wider intervals under less data are necessary but not sufficient; and the
> disclosed `linear_weights_estimated` bands that ignore finite-sample cell
> uncertainty are a concrete instance of intervals that are not honest under
> sparsity, contradicting the park-factor narrative.

**Response:** Two corrections to the first revision's answer. The park-factor
interval-width narrative is not validated by the predictive-coverage machinery:
`park_factor_runs` has no coverage hook, only `state_transition` and
`run_expectancy` do, and revised §7 and §11 say that the width-versus-sparsity
pattern is a description of the posterior, not a validated coverage claim. The
`linear_weights_estimated` finite-sample gap is fixed as the first revision
described — `propagate_linear_weights_draws` draws per-cell Dirichlet(counts +
0.5) combination weights per RE-posterior draw, median width ratio 1.5588, p90
4.1936, widest on the smallest-n cells (`linear_weights_width_comparison.md`) —
and, contrary to the first revision's "not yet restated into prod," that
propagation has been the published one since 2026-07-14. This revision also
adds the deterministic sibling's occurrence floor (cells at or below 100
occurrences publish the pooled value with `is_imputed = True`, where the
previous surface published a 1924 NN1 `Triple` on one event) and drops
transitions whose start state has no posterior cell. The table was re-derived from
`re-full-eraregime-v4` on 2026-09-04; the restated table carries the pooled
values for its 827 floor cells but not yet the `is_imputed` column, which §11
records.

---

## Minor comments

### Minor 1. Model counting is inconsistent.

**Response:** §4 opens with a table mapping letter → estimand → published table
→ tier → status for A–K, and the paper counts from it: twelve tables, ten
populated; seven letters published, three withheld, one deferred.

### Minor 2. Span inconsistency.

**Response:** §2 states the event universe (18,141,020 events; 205,845
play-by-play games in the 1910–2025 span) and reconciles the pre-1910 rows: the
snapshot holds a further 41 play-by-play games from the 1900s decade, which is
why decade-keyed tables show a `1900` row, and season-keyed surfaces (park
factors, Model G cells) cover every season the source carries. The
`event_states_full` count is the whole table and includes those games' events.
§4's "pools 1901 through 2025" is replaced by "every season the source carries."

### Minor 3. Garbled sentence.

**Response:** Repaired in §4.

### Minor 4. Units.

**Response:** §5 states once, where the offset is introduced, that δ_c is a
natural-log log-odds in nats, and uses nats for the offset, the grid, and the
bound-offset throughout.

### Minor 5. 94% HDI.

**Response:** §7 opens with one sentence: 0.94 is the ArviZ default the fits
summarize with, kept unchanged so every published HDI is the library's native
summary rather than a probability re-chosen per table, and its unfamiliar width
is a standing reminder that the interval probability is a convention.

### Minor 6. Illustrative tables.

**Response:** §7 labels every extremes-only or single-cell table as illustrative
(the two RE states, the transition example, the park-factor extremes, the
five-play linear-weights comparison, the assist-count and pitch-summary
examples) and cites the full table under `notes/paper/tables/` wherever a claim
depends on the whole surface.

### Minor 7. Define s(c,x).

**Response:** §5 gives the range s(c,x) ∈ (0,1], states additive separability of
the masking log-odds as the assumption, and states that the class-independent
part cancels under the per-event softmax, cross-referencing the same invariance
that kills the scalar random effect.

### Minor 8. Tone.

**Response:** "Exactly where a fan would expect it" (§7) and "a lesson worth its
own telling" (§4) are removed; "the offset is right, and the offset is
unidentified" did not survive the §5 rewrite.

### Minor 9. Section length.

**Response:** §7 is compressed to the tables that carry an argument, but in
this revision it also carries the disclosed corrections and the refit
comparisons the review required, so it remains above the target length.

### Minor 10. Provenance comments.

**Response:** The `<!-- src: … -->` scaffolding is stripped in the submission
build. The five citations to memory files that did not exist are replaced with
repository sources or marked `TODO: unverified`.

---

## Questions to the authors

1. **Calibration evidence** — see M1: held-out ECE and predictive coverage are
   computed and reported; both are warn-only diagnostics; the refit values are ECE
   0.0049–0.0181 and predictive coverage 0.9780 / 0.9062.
2. **Does the MNAR result survive alternative masking designs** — see M2: one
   informative design (`covariate_joint`, 0.537); the other three are identities;
   all four are smoke-budget runs.
3. **Joint offset-vector sweep** — see M3: withdrawn; the record supports a floor
   on one class (0.292 / 0.339 / 0.400), published with an assumed ±1.0-nat band.
4. **Basis for the grid width / principled upper bound** — see M3: the width is
   checked against one synthetic mask; there is no data-identified upper bound
   and the paper does not claim one.
5. **Is Model B "Ω ≈ identity" or "Ω non-identified"** — see M5: neither
   phrasing; the data are uninformative about Ω and any fit returns the prior.
6. **Why is Model K grouped with B and I** — see M5: it is not; K is "designed,
   not built."
7. **What fails the `passed` gate; provisional framing** — see M7: the column's
   history is given; every populated table reads `passed` under the version-2
   sweep since 2026-09-04.
8. **Release backtest harness, gate reports, ablation tables** — see M6: recipes
   ship; the backtest run artifacts and the ablation artifacts do not yet.
9. **Does the 6× translate into a measured estimand improvement** — see M4: the
   deep proposals beat a class-prior baseline on TEST log-loss on every
   dimension (measured for the first time in this revision); the ablation was
   re-run and its artifacts are on disk.
10. **Reconcile park-factor "honest uncertainty" with the too-tight
    linear-weights bands** — see M9: the Dirichlet propagation is published; the
    park-factor width pattern is not a validated coverage claim.

---

## Remaining limitations

The refits that carry this revision's corrections into the tables — geometry
(both flavors, every dimension), run expectancy, state transition, pitch
summary, putout credit, the six observation-propensity targets, park factors —
completed on 2026-09-04, and every number in the manuscript is read from the
restated tables. What remains: the MNAR offset is
unidentified for every class and bounded from below for one; the backtest
robustness table is a smoke-budget run without checked-in artifacts; the deep
out-of-fold predictions carry a pretraining leak the paper quantifies as an
overlap (70.1% of Bayes held-out trajectory rows inside the pretrain's labeled
set) but not as a magnitude; the location deep specs score only observed rows,
so the deep supplement reaches one geometry dimension; the deep pointers sit
outside the gate sweep; and park factors have no coverage hook. Per
REVISION-PLAN.md, the Model H buildout, the error-credit and DP unblocks, a
joint EM selection model, and Statcast second-source ingestion remain out of
scope.
