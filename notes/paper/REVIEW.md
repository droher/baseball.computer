> Historical internal drafting record. The [September 13 revision](REVISION-2026-09-13.md) and [current evidence ledger](EVIDENCE.md) supersede its status claims. This is not evidence of external journal review or submission.

---
title: Review — "Estimating the Unrecorded Game"
type: review
status: complete
reviewer: R1 (Opus, tech-write review mode)
last-verified: 2026-07-13
---

# Review

**2 Block / 11 Push back / 6 Nit** — **Verdict: Block.**

Two block-level accuracy defects: a headline row-count total in §7 that contradicts
its own inventory table (~337M actual vs "257M" claimed), and a deep-learning
architecture description in §6 that documents a superseded design (13 player slots,
13 heads, freeze/unfreeze) contradicting the authoritative source. The empirical
results tables (§7) otherwise cross-check cleanly against `tables/*.md` — I verified
~50 individual figures across geometry, run expectancy, transitions, park factors,
linear weights, ground-ball MNAR, and coverage-by-decade, and all matched. The
statistical-core numbers in §4/§5 (Model A sweep, MNAR γ refutation, oracle δ,
structural-zeros transition fit, Model C/F/G/J diagnostics) all confirmed against
their memory-file and implementation-review sources. Citations are real (one year/id
mismatch). Fix the two blocks, reconcile the perm-importance provenance, and unify the
event count, and this ships.

---

## Block

### [Block] §7 — "roughly 257M total published rows" contradicts the inventory table
**Where:** §7 "Twelve tables, two of them empty on purpose", lines ~762–765:
"`imputed_batted_ball_geometry` alone accounts for 253M of the roughly 257M total
published rows".
**Why:** Summing the 12 row counts in the cited `tables/table_inventory.md` gives
**336,565,450** rows, not 257M. `scorer_observation_propensities` alone is 67.4M and
`imputed_ball_handler_probabilities` is 11.7M — both omitted from the "257M". The
"257M" figure equals only geometry (253.0M) + fielding-credit (4.3M). So geometry is
253M of **~337M** (about 75%), not "253M of ~257M" (which would be 98%). The claim
contradicts its own source and materially overstates geometry's share. This is the
kind of number an agent will quote confidently.
**Suggested fix:** Change to "253M of the ~337M total published rows (75%)", or scope
the sentence explicitly ("of the ~257M event-imputation rows in geometry and fielding
credit"), and re-check the "dwarfs the other eleven combined" phrasing against the
corrected total.

### [Block] §6 — pretraining architecture describes the superseded design
**Where:** §6 "Shared entity-embedding pretraining", line ~704: "Thirteen player-slot
inputs — batter, pitcher, and eight fielders and three runners — route through one
shared `embed_player` layer against thirteen pretext heads spanning plate-appearance
outcome, count/run outcomes, baserunner advancement, and batted-ball geometry, with
Kendall-Gal uncertainty weighting … and a two-stage freeze/unfreeze schedule that
trains the heads and embeddings against a frozen trunk before unfreezing the whole
network".
**Why:** This is the **v2** design, explicitly superseded. The authoritative
`memory/pretrain_architecture.md` states the current architecture: (invariant 4)
"Entity embedding = batter + pitcher only … Fielders and runners are NOT in the
embedding group — they were diluting batter/pitcher signal"; (invariant 3) "Heads
cover only imputation targets. 5 heads: trajectory_remapped, batted_location_general,
batted_location_depth, batted_location_edge, batted_to_fielder_class"; (invariant 2)
"Observed outcomes as inputs, not heads … [as heads] was the previous failure mode";
and the training scheme is a **residual two-stage decomposition** (stage-1 context-only
logits cached as a frozen additive offset, stage-2 adds embeddings), not
freeze/unfreeze. Even the paper's own primary cited source,
`04-deep-learning-supplements.md`, says at line 16 "The active pretrain ships 5
imputation-target heads … Fully-observed outcomes … are inputs, not heads" — the
13/13 wording the paper used comes from that file's stale "v2" section (lines 308–317),
which the file's own current-state invariant contradicts. As written, an agent would
believe the pretrain includes fielder/runner embeddings and outcome heads — precisely
the rejected design.
**Suggested fix:** Rewrite the paragraph to the current architecture: one shared
batter+pitcher embedding (park/scorer separate; fielders/runners deliberately
excluded), 5 imputation-target heads, fully-observed outcomes as inputs, residual
two-stage decomposition, focal loss, batted-ball row filter (~12M events, not the full
18.1M). Cite `memory/pretrain_architecture.md` and `04-deep-learning-supplements.md`
line 16, not the v2 section.

---

## Push back

### [Push back] §6 / Abstract — perm-importance multiplier cited to a source that publishes different numbers
**Where:** §6 line ~704 "pretraining lifts batter permutation-importance 8.4× over the
no-pretrain baseline and pitcher permutation-importance 5.8×"
`<!-- src: …/phase3-acceptance-gates-v6.md -->`; Abstract "multiplies player-effect
signal … by 5–8×".
**Why:** The cited `phase3-acceptance-gates-v6.md` shows the **published** gate at
batter ×6.4 / pitcher ×6.3 (lines 61–66) and labels ×8.4/×5.8 as "Smoke results
(pre-publish)" (lines 161–165). So the paper reports the pre-publish smoke numbers as
if they were the gate result, contradicting the published table in the same file. The
8.4×/5.8× figures *are* supported by `memory/pretrain_architecture.md` ("8.4× lift …
Pitcher 5.8×"), so this is a provenance/reconciliation problem, not a fabrication —
two internal sources disagree on the canonical value.
**Suggested fix:** Decide which is canonical. If 8.4×/5.8×, cite
`pretrain_architecture.md`; if the published gate 6.4×/6.3×, use those and update the
Abstract's "5–8×" accordingly.

### [Push back] §1/§2 vs §7 — the event count is stated as two different numbers
**Where:** §1 line ~52 and §2 lines ~167–168 "18,137,758 events"
(`<!-- src: …/README.md -->`); §7 lines ~726–727 and §7 geometry "18,141,020" events
(`<!-- src: tables/corpus_by_decade.md -->`).
**Why:** Both claim to be `COUNT(*)` of `main_models.event_states_full`. README says
18,137,758; the live query in `corpus_by_decade.md` says 18,141,020 — a 3,262-event
gap. The game counts (205,845 / 1,953 / 4) agree across both sources; only the event
count drifts, so README is almost certainly stale post-refresh. Presenting the same
quantity with two numbers is a cross-writer inconsistency.
**Suggested fix:** Unify to the live-query figure 18,141,020 across intro/data, and
either refresh README or note it lags the current DB.

### [Push back] §11 — Model A trajectory ROC-AUC stated as 0.897, source says 0.898
**Where:** §11 line ~1199 "ROC-AUC 0.897–0.973 held out"; §4 line ~396 correctly says
"0.898 on trajectory".
**Why:** `implementation-review.md` gives trajectory held-out ROC-AUC 0.898 (the range
minimum). §11's "0.897" contradicts both the source and §4's own value.
**Suggested fix:** Change §11 to "0.898–0.973".

### [Push back] Notation — δ is overloaded across the paper
**Where:** OUTLINE reserves δ_c for the MNAR per-class selection offset (used in §5,
§1). But §4 uses δ for unrelated fixed-effect coefficient blocks: Model A
"$\sum_j X^{(j)}_i \delta^{(j)}$, $\delta^{(j)} \sim ZeroSumNormal$" (line ~372) and
Model E/D "$\sum_j \delta^{(j)}_c[x_i]$" (line ~413).
**Why:** The canonical contract makes δ_c *the* selection offset; reusing δ for generic
softmax fixed effects invites a reader to conflate the two, especially since §5's δ_c
also enters a softmax logit. An outside statistician skimming §4 then §5 sees "δ"
meaning two different things.
**Suggested fix:** Rename the fixed-effect blocks (e.g. β^{(j)}), reserving δ for the
MNAR offset.

### [Push back] Terminology — "sensitivity ribbon" vs "sensitivity band" drift
**Where:** "ribbon" in §1 line ~87, §5 lines ~660/665; "band" in Abstract, §1 line
~142, §5 lines ~682/693. OUTLINE's canonical term is "sensitivity ribbon".
**Why:** The two terms are used interchangeably for one concept; the task explicitly
flags this as a canonical-term check.
**Suggested fix:** Standardize on "sensitivity ribbon" (or define "band ≡ ribbon" once
and stick to one).

### [Push back] Redundancy — the 34%/68% ground-ball MNAR fact is fully re-derived in four places
**Where:** Abstract; §1 lines ~63–77; §5 lines ~572–587; §7 lines ~826–844. All four
agree numerically (0.337/0.680 pre-1950; 0.44/0.78 for 1950–1987), so no contradiction
— but the *mechanism* (deduced rows as partial-truth peek, fires only on assisted
infield putouts, 78–95% unobserved slice) is spelled out at length three times.
**Why:** A paper can preview in the intro and deliver in results, but §5 re-derives the
same observed/deduced gap and unobserved-slice fractions that §1 and §7 already carry.
**Suggested fix:** Keep the full treatment in §7 (results) and the one-line hook in §1;
cut §5's re-statement down to a pointer ("the pre-1988 gap of §7") and go straight to
the correction math, which is §5's actual contribution.

### [Push back] §4 length — ~350 words over target; the canceling-RE vignette is the cut
**Where:** §4 "A scalar random effect under softmax is worth nothing" (lines ~547–567).
**Why:** The identity softmax(x + c·1) = softmax(x) and its consequence are restated in
§5 ("the same softmax invariance that killed the scalar random effects", line ~606).
The full vignette plus the structural-zeros vignette pushes §4 over budget.
**Suggested fix:** Compress the canceling-RE subsection to ~4 sentences (the identity,
the four-model impact numbers ≤0.0015/0.00004, the per-class fix) and let §5 carry the
"convergence is not sufficient" theme; keep the structural-zeros vignette at length
(it is the paper-worthy one).

### [Push back] §5 ethos — the ribbon's coverage claim is marginal but validated jointly
**Where:** §5 lines ~663–680: the ribbon "sweeps each class's offset alone over a grid"
(per-class-alone), yet the validation applies the *joint* oracle offset (+0.37
GroundBall and −0.09 others simultaneously) to get TV 0.073→0.033 and the [0.18, 0.59]
band, and concludes the ribbon is "calibrated to be wide enough to hold a realistic
MNAR shift and no wider."
**Why:** Real MNAR moves all classes jointly; a one-class-at-a-time ribbon can
understate joint uncertainty. The strongest counterargument — that the published
marginal ribbon may not cover joint shifts even though the joint oracle offset was what
validated it — is not engaged.
**Suggested fix:** State explicitly whether the coverage claim is marginal (per-class)
or joint, and either justify the marginal sweep or note the joint case as a limitation.

### [Push back] §4 vs §7 — Model J coverage arm presented as shipped in §4, empty in §7
**Where:** §4 line ~539 "[the coverage arm] clears its smoke gate at held-out ROC-AUC
0.995"; §7 table_inventory shows `pitch_count_coverage` at 0 rows / NULL provenance
(deferred).
**Why:** §4 reads as though the coverage arm is a published surface; §7 shows it
publishes nothing yet. Not a hard contradiction (fit but not published), but a reader
moving from §4 to §7 sees tension.
**Suggested fix:** Add a clause in §4 that the coverage arm is fit (smoke) but not yet
published — its table materializes a typed zero-row frame pending an artifact pointer,
per §7.

### [Push back] Abstract — "Twelve probabilistic surfaces are published" while two are empty
**Where:** Abstract "Twelve probabilistic surfaces are published"; §7 is careful
("two of them empty on purpose", zero-row typed frames).
**Why:** Two of the twelve (`imputed_advancement_probabilities`, `pitch_count_coverage`)
carry 0 rows and NULL provenance — deferred, not published. The Abstract overstates.
**Suggested fix:** "Twelve surfaces are defined; ten are populated and two are deferred
as typed zero-row frames," or similar.

### [Push back] §4 — "nine separate fits" is an imprecise count
**Where:** §4 line ~308 "reading them as a family — rather than nine separate fits".
**Why:** The shipped models are A, C (putout + assist + assist-count), D, E, F, G (RE +
transition), J (coverage + summary) — the fit count depends on how arms/submodels are
counted and isn't nine on any obvious reading.
**Suggested fix:** Drop the number ("rather than as separate fits") or state the exact
count with what's included.

---

## Nit

### [Nit] §10 — Betancourt & Girolami (2015) / arXiv:1312.0906 year–id mismatch
**Where:** References; "Betancourt, M., & Girolami, M. (2015). … arXiv:1312.0906."
**Why:** arXiv:1312.0906 is a December 2013 preprint. The work appeared in 2015 as a
book chapter (Current Trends in Bayesian Methodology with Applications). Citing (2015)
against the 2013 arXiv id is inconsistent. All other six citations verified correct
(Rubin 1976 Biometrika 63(3):581–592; Little 1993 JASA 88(421):125–134; Little & Rubin
2019; Gelman et al. 2013; Baumer et al. 2015 JQAS 11(2):69–84; Marchi & Albert 2014).
**Suggested fix:** Cite the 2015 book chapter, or change the year to 2013 to match the
arXiv id.

### [Nit] §7 — HDI credible mass labeled 94% in prose but 95% in the linear-weights table note
**Where:** §7 says "94% HDI" (re/park/linear-weights prose, e.g. line ~880, ~969);
`tables/linear_weights_compare.md` note says "95% HDI".
**Why:** arviz default HDI is 94%; the table note's "95%" is likely a loose label but
contradicts the paper's stated mass.
**Suggested fix:** Confirm the artifact's actual credible mass and make prose and the
table note agree.

### [Nit] Abstract — "5–8×" lower bound vs the 5.8× body figure
**Where:** Abstract "by 5–8×"; body 8.4× / 5.8×.
**Why:** The lower bound "5" is below the actual pitcher 5.8×; the range reads looser
than the data. Tie this to whichever multiplier is chosen in the Block-2 / perm-imp
reconciliation.
**Suggested fix:** Use a range that matches the reconciled figures.

### [Nit] §6 — "full 18.1M-event universe" overstates the pretrain training rows
**Where:** §6 line ~704 "over the full 18.1M-event universe".
**Why:** `pretrain_architecture.md` invariant 5 filters to batted-ball events (~12M,
"~33% row reduction"); the universe *table* is 18.1M but the pretrain trains on ~12M.
The cited `04-deep` line 308 also says 18.1M, so this rides along with the Block-2
rewrite.
**Suggested fix:** "over the full event universe, filtered to ~12M batted-ball events".

### [Nit] §4 line ~539 — Model J "roughly 53% of events lack an observed count" not found in cited source
**Where:** §4 "roughly 53% of events lack an observed count"
`<!-- src: docs/estimated-models.md -->`.
**Why:** Could not confirm the 53% figure in `docs/estimated-models.md` or the checked
memory/t2 files. It may live elsewhere (model code / dataset stats).
**Suggested fix:** Point the src at the file that actually carries the 53%, or soften to
"about half".

### [Nit] §7 line ~969 — src comment placed mid-sentence
**Where:** "falls inside or almost against the 94% HDI
`<!-- src: …linear_weights_estimated.py -->` everywhere".
**Why:** The provenance comment interrupts the clause; other src comments sit at
sentence end.
**Suggested fix:** Move the comment to the end of the sentence.
