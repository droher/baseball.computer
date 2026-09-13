## Limitations and open problems

The principal limitation is evidentiary. The September 13 work completed and
reconciled a full-history PBP candidate, while the retained September 4 model
artifacts fail the current scientific publication contract. These statements can
both be true because coverage validation and model validation answer different
questions.

**The legacy statistical family does not pass gate version 3.** All 24 migrated
pointers have overall status `failed`; provenance, identification, and transport
are unsupported for every pointer. The 19 Bayesian artifacts lack their declared
`prior_predictive.nc` files. The four geometry deep proposals fail current
predictive and calibration evidence, and the event-universe pretrain is unsupported
on every evidence dimension. The strict pointer checker passes the exploratory
wrappers because their identities, reasons, failed verdicts, and retained-byte
bindings are internally consistent. That is an integrity result, not scientific
authorization.
<!-- src: docs/modeling-evidence-contract.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/migration_summary.json -->

**The full-history candidate is a rough reconstruction of the acquired PBP
population.** It covers 205,886 games and 18,141,020 events from 1903--2025 and
maps all 116 declared completion targets. Its isolated checks establish artifact
identity, source preservation, row coverage, schema compatibility, probability
normalization, and defined conservation rules. They do not show that an
unrecorded temperature, pitch, fielder, trajectory, official, or run value was
historically correct. Games without an acquired PBP event spine, including
aggregate-only games, remain outside the estimand.
<!-- src: docs/pbp-imputation.md -->

**Missingness remains unidentified for the naturally unrecorded class.** A model
fit on recorded labels estimates $P(Y\mid X,R=1)$, not
$P(Y\mid X,R=0)$. Marginal observation propensity does not recover the
class-dependent selection odds. Rule-derived ground-ball rows provide at most a
conditional lower bound and a selected diagnostic subset; they do not prove MNAR
or identify the other trajectory classes. The September 13 candidate uses broad
empirical fallbacks rather than claiming a fitted MNAR correction.

**The location target correction changes the meaning of old results.** The legacy
six-class `location_side` artifact contains within-zone angle modifiers and is
now exposed as `location_angle`. Its historical predictive scores do not validate
global field-side reconstruction. The new PBP interface derives global side from
general location and keeps angle, depth, and edge distinct, but the fallback
accuracy of those historical estimates is not confirmed.
<!-- src: docs/modeling-evidence-contract.md -->
<!-- src: artifacts/imputation/legacy-publication-candidate-v1/README.md -->

**Airborne standardization is assumption labelled.** The 2009--2019 pipeline has
useful development support. The 2020-onward held-out screen failed under its
two-season design, and pre-2009 translation assumes a modern true-band mix and a
relationship to the 2020-onward vocabulary. Its nominal intervals omit uncertainty
in those assumptions. The PBP candidate carries the nearest 1989 translation
backward for earlier seasons with weak-identification flags. This coverage decision
was explicitly accepted as an estimate; it is not a passed transport result.
<!-- src: docs/geometry-modeling-handoff.md -->
<!-- src: docs/geometry-air-translation-estimate-2026-09-11.md -->

**The confirmation reserves remain sealed.** The 121 deferred modern-angle games
and 6,105 historical component-local reserve games were not opened for this paper
revision or the PBP candidate. No new model was fit and no new confirmation data
were acquired. The air-research decision remains closed under its stated
assumptions; the later failed screens and partial-identification findings are
reported without reopening the reserve.
<!-- src: docs/geometry-modeling-handoff.md -->

**Legacy deep evidence is incomplete and partly contaminated.** Shared pretraining
could encode a downstream row's geometry label before nominal cross-fitting, and
the Bayesian and deep partitions were not nested. An earlier one-fold trajectory
configuration also labelled full-fit predictions as out of fold. The strongest
ablation files quoted by an older draft are not retained, so their exact effects
cannot be reproduced. A future deep supplement would need target-free outer
folds across every supervised stage, explicit dependency lineage, aligned baseline
probabilities, calibration, and transport tests before it could support a
scientific claim.

**Reported uncertainty is conditional on method and fallback.** Donor dispersion
measures empirical variation within the selected donor pool. It does not include
uncertainty that the fallback hierarchy, historical transport assumption, or source
measurement is wrong. Pre-2009 airborne intervals omit the error in their two main
transport assumptions. Legacy run-expectancy and transition interval diagnostics
use approximations rather than retained joint posterior draws, and park factors
lack a held-out coverage hook. A narrow conditional interval can therefore coexist
with weak historical identification.
<!-- src: docs/pbp-imputation.md -->
<!-- src: docs/modeling-evidence-contract.md -->

**Conservation does not resolve source conflict.** Pitch and fielding reconciliation
ensures that the candidate's defined counters and allocations add up. The builder
retains parser statuses, raw sequences, and explicit conflict dispositions because
many source sequences violate modern or internal boundaries. A reconstructed legal
sequence is one coherent completion of the retained evidence, not proof of the
historical pitch order. Likewise, exact fielding capacity can constrain a credit
allocation without independently identifying the responsible fielder.

**The available context stress test shows material roughness.** In the retained
1,000-game joint holdout, mean absolute error was 7.53 °F for temperature, 10,227
for attendance, 4.32 mph for wind speed, 25.67 minutes for duration, and 234.34
clock minutes for start time; categorical exact agreement ranged from 19.7% for
wind direction to 94.0% for precipitation. This is a test against recorded values
under an artificial joint mask, not a calibrated study of historical missingness,
but it demonstrates why completed context fields should retain their methods and
donor dispersion.
<!-- src: artifacts/imputation/20260913-context-holdout-v1/report.json -->

**Several latent baseball quantities remain outside the supported source.** The
record does not supply an independent event-level contact label for a structured
scorer confusion matrix or a positioning/opportunity source for analytical fielder
responsibility. Shift propensity was designed but not fit. The legacy advancement
and pitch-count-coverage consumers remain typed empty because they have no retained
validated pointer. The broader PBP interfaces may still expose rough completed
values where the registry declares a fallback, but that does not resolve these
scientific estimands.

Future scientific promotion would require new artifact IDs, complete retained
lineage and prior-predictive outputs, leakage-free grouped holdouts, calibration,
source-block and era transport tests, and validation against data not used during
development. The current paper instead reports the completed PBP candidate as an
exploratory, assumption-labelled analysis layer and dates older model results to
the evidence that produced them.
