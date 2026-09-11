# Grounding the airborne label translation for seasons without references

Research review, 2026-09-11. Question: how to estimate the recorded-label to standardized-band mapping for seasons with no Statcast reference, with the least bias and uncertainty that reflects what the data cannot pin down. Sources in the first four sections passed a three-vote adversarial check against the primary text. The sabermetric and evaluation sections are search results that were found but not verified the same way; treat them as leads.

## What the literature agrees on

Every method that carries a validated mapping to unvalidated strata assumes the observation process P(recorded label | true band) is the same in both, conditional on covariates the validation data covers. That includes regression calibration, multiple imputation for misclassification, label-shift estimation, and Bayesian misclassification models. The 2015 to 2025 window is an external validation study relative to earlier seasons, and season is a covariate the validation sample does not cover, so the transport assumption cannot be checked from the reference data. Simulations show that a stable-mapping correction applied when the mapping actually differs can be worse than no correction (Ross et al. 2024, Epidemiology, PMC10841744; STRATOS measurement-error guidance part 1, Keogh et al. 2020, Stat Med).

Two consequences for this project:

- Label-shift and prior-shift methods (RIME in epidemiology, Edwards, Cole, Fox 2020 AJE, PMC7608057; BBSE, Lipton et al. ICML 2018) transport P(recorded | true) and let the true-band prevalence change. They handle hitter drift with a fixed scorer, and nothing else. They give one end of a sensitivity analysis, the hitter-only end, not the answer.
- Multi-category misclassification is a full matrix, not a sensitivity and specificity pair (STRATOS part 1; Ling et al. 2018 PLOS ONE). The Ling model assumes one matrix for all units and does not identify it without a gold-standard subsample. It is a likelihood skeleton only; the matrix has to be indexed by regime or season to be useful here.

## Two principled routes for pre-reference seasons

**Partial identification with a posterior over the identified set.** Molinari (J. Econometrics 2008; Cornell CAE working paper 05-10) writes P(recorded) = Π P(true) and turns validation knowledge into inequality restrictions on Π for the unvalidated period. Identification regions are sharp given the restrictions. With only a lower bound on per-category correct-report probability, the region is the Horowitz and Manski (1995) contaminated-sampling interval. Gustafson (Int. J. Biostatistics 2010) shows that Bayesian inference over such a model gives a posterior whose support converges to the identification region and whose shape inside it comes from the prior on the non-identified part. That is the object the user asked for: a distribution to sample from whose spread does not collapse with more data. The shape is prior-driven and must be reported together with the region and the prior that produced it (Moon and Schorfheide 2012, Econometrica).

Molinari's Health and Retirement Study application is a direct precedent. Applying the 1992 validation matrix to 1998 reports produced an implied true-share vector with a negative entry and an entry above one, rejecting no-drift, while a same-year no-selection assumption survived. The remedy was monotone and directional restrictions read off the validation table, estimated by nonlinear programming. Two transfer points: the invalid-simplex check is a cheap drift diagnostic for this data (invert the late-regime matrix against early-regime or pre-2015 recorded-label marginals and see whether a valid probability vector results); and Molinari's monotonicity runs forward in time, so backward extrapolation from a recent window only yields upper bounds on earlier correct-classification probabilities unless a different directional assumption about scorer practice is credible.

**An explicit dynamic model of the mapping.** A multinomial logistic-normal dynamic linear model (Saxena, Chen, Silverman, AISTATS 2025, arXiv 2410.05548) puts a Gaussian random walk on the additive-log-ratio transform of each category vector. At time points with no observations the filter skips the update, so the state covariance grows by the evolution variance per step. That is the mechanism for uncertainty that widens with distance from the reference window. The evolution variance is itself only weakly identified from an eleven-season window, which is why it must be declared as an assumption rather than presented as estimated. DynIBCC (Simpson, Roberts, Psorakis, Smith 2012, arXiv 1206.1831) does the same for per-annotator confusion matrices, indexed by each annotator's own sequence of classifications. Before 2015 there is no gold standard, so DynIBCC reduces to unsupervised Dawid-Skene with drift and is identified only if a second source labels the same plays.

## Relation to the current design

The current regime model already has the right skeleton: a per-cell mapping with a regime index, scored against references inside the window. What it lacks for seasons outside the window is a declared drift model. The research supports:

- Keeping the recorded label as the primary fact and modeling the translation, not replacing labels.
- Making the drift term explicit: a random walk on the log-ratio scale, or equivalently a season level inside each regime whose variance is a stated assumption sized from the one observed regime difference. The literature does not offer a way to estimate that variance from the window alone.
- Reporting the identification region and the prior that shapes the posterior inside it, rather than a single point limit.
- Running the invalid-simplex check and the same-period no-selection check on the window data before any backward extrapolation.
- Treating the hitter-only bound (label shift with a fixed scorer matrix) and the scorer-only bound as the two ends of a sensitivity analysis.

## Unverified leads: sabermetric evidence on the labels themselves

These were found by search and not adversarially checked.

- MLBAM's no-nulls policy (Tango, Statcast Lab, 2017) imputes launch angle and exit velocity for untracked batted balls from the stringer's batted-ball type plus outcome, applied from 2017 with retroactive correction to 2015. Some reference launch angles are therefore functions of the scorer label. Those rows make the mapping partly circular and should be excluded or down-weighted. This matches the existing note in the handoff that reference angle origin is unknown by row.
- Statcast batted-ball type is assigned by a human stringer, not the published launch-angle bands; about a quarter of balls labeled line drives fall outside the glossary range, and year-over-year line-drive rate correlation is about 0.42 against about 0.8 for ground balls and fly balls (Andrews, FanGraphs, July 2024).
- Park and scorer biases in trajectory shares persist year to year and correlate with press-box height (Wyers, Baseball Prospectus, 2010). Supports a park or scorer effect rather than one global mapping.
- The human label is outcome-dependent: hits are called fly balls at a lower arc than outs in 2015 to 2016 data (Daley-Harris, Hardball Times, 2016). Label error is differential on outcome, which is why the result-family cell is in the candidate arm.
- Band standardization depends on exit velocity (FanGraphs Community, 2018), another channel where hitter behavior and scorer practice mix.

## Unverified leads: evaluating calibrated estimates under shift

- Vaicenavicius et al. (AISTATS 2019) frame calibration evaluation as a hypothesis test with a null distribution built by resampling labels from the model's own probabilities at the actual test size and binning, so a screen is sized to sampling noise instead of a fixed threshold. A debiased ECE confidence interval exists (arXiv 2408.08998).
- Simulation-based calibration (Modrák et al., Bayesian Analysis 2023) checks that posterior intervals reach nominal coverage under the model's own prior, which is how to validate a drift prior before backcasting.
- Ovadia et al. (NeurIPS 2019) report calibration and coverage stratified by shift severity, here distance in seasons from the reference window, rather than pooled.
- Weighted conformal prediction under covariate shift (Tibshirani et al. 2019) and label shift (Podkopaev and Ramdas 2021) keep coverage when the shift is of the assumed kind, at the cost of reduced effective sample size.

## Open questions the review did not settle

- How much of the early-to-late shift inside recorded fly balls is stringer practice, press-box scorer practice, or hitters. No source separates these.
- Whether any pre-2015 source labels the same plays a second time (BIS, HitTracker, PITCHf/x-era hit locations, Retrosheet hit-location codes). A second annotator would make drift estimation identified before 2015.
- Which directional restriction on the pre-2015 matrix is credible, given that forward monotonicity does not apply backward.
- How to validate the backward extrapolation with no pre-2015 truth. The obvious protocol is to hold out the earliest reference seasons and extrapolate backward from the rest.
