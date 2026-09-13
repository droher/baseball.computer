## Related work

The framing rests on the missing-data literature. Rubin (1976) established the
distinction between missing completely at random, missing at random, and missing
not at random, and the conditions under which the missingness mechanism can be
ignored for likelihood and Bayesian inference. Our observation models are a
direct application: the default assumption is missingness at random conditional
on scorer, source, result, and era, and the contribution is to make that
conditioning set explicit rather than assumed. Little and Rubin (2019) give the
methodology this paper follows for handling incomplete data through models rather
than deletion or single imputation.

Where ignorability fails, two traditions divide the territory, and this paper
draws on both. The first is the selection-model tradition. Heckman (1976, 1979)
formalized sample selection as a two-equation system — an outcome process and a
selection process whose errors are correlated — and showed that selection bias
is a specification error correctable by modeling the selection equation jointly
with the outcome. §5 uses a categorical selection formulation: a
latent class is drawn, then recorded with probability s(c, x), and Bayes' rule
turns the selection process into a per-class reweight of the fitted
distribution — the offset δ_c is the per-class selection log-odds, a
selection-model parameter acting on the recorded class before the softmax
normalizes. The difference from Heckman's program is what we ask of the
parameter. Heckman buys identification with parametric structure — joint
normality of the two equations' errors, or an exclusion restriction that moves
selection without moving the outcome. We have not established a credible exclusion restriction in this study.
Scorer, source, and era covariates can also proxy the game process. We therefore
treat δ_c as unidentified from the recorded labels alone and report sensitivity
to assumed values. A partial-truth slice constrains the missing ground-ball
share from below; it does not independently identify δ_c.

That sweep places the paper in the second tradition: pattern-mixture models and
delta-adjustment sensitivity analysis. Little (1993) specified the distribution
of missing values directly through identifying restrictions rather than a
mechanism, and observed that such models are chronically underidentified —
without additional restrictions the observed data do not identify the
missing stratum's distribution. Inference across alternative restrictions
therefore makes the unresolved assumption visible. Scharfstein, Rotnitzky, and Robins (1999)
made the same move inside the selection formulation: fix a nonidentified
selection-bias parameter at each value in a plausible range, estimate under
each, and report the trajectory — sensitivity analysis in place of an
untestable assumption. That template became standard practice in the
clinical-trials missing-data literature as delta adjustment and tipping-point
analysis, where a fixed offset δ is added to values imputed under a
missing-at-random fit and swept until the conclusion changes; Molenberghs and
Kenward (2007) treat the framework at book length, and Cro, Morris, Kenward,
and Carpenter (2020) give a practical guide to controlled multiple
imputation in this style. The sensitivity analysis of §5 is this device
transplanted: δ_c is a delta adjustment on the class logits, the
missing-at-random fit is the δ = 0 point, and the ribbon is the tipping-point
trajectory, reported per class. What the paper adds to the template is a hard
constraint on the sweep — a deduced-trajectory partial-truth slice internal to
the record that places a floor under one class's share on the missing stratum,
and a known-truth subslice on which the missing-at-random fit can be scored —
and historical smoke-budget masking diagnostics that illustrate the offset's
behavior under a selection process it does not nest. An earlier revision read the same slice as
an estimate of δ_c; §5 explains why it is a floor and not an estimate. The structure of the treatment — a selection-model
parameterization with pattern-mixture-style sensitivity reporting — reflects
the standard observation that the two factorizations describe the same joint
distribution and can be mixed rather than chosen between.

The observation process studied here is informative nonresponse in the survey
statistician's sense: the probability an item is recorded depends on the value
the item would have taken. Rubin (1977) formalized subjective assumptions about
nonrespondents in exactly the spirit adopted here — a parameter the data cannot
estimate, elicited and varied rather than ignored. The survey-methodology
literature collected in Groves, Dillman, Eltinge, and Little (2002) treats
nonresponse as a process with its own covariates and propensities, which is
Model A's stance toward scorers: the observation-propensity surfaces are
response-propensity models with scorers in the role of respondents, and the
event-dimension grain of §3's taxonomy is item nonresponse rather than unit
nonresponse — one event can respond on batting and not on trajectory.

The fitting methodology follows the applied Bayesian workflow. Gelman et al.
(2013) is the reference for the hierarchical models, partial pooling, prior and
posterior predictive checking, and the practice of treating a model as a
hypothesis to be checked against held-out data. Betancourt and Girolami (2013)
document the pathologies that hierarchical models present to Hamiltonian Monte
Carlo and explain why a non-centered parameterization can improve sampling
when data are sparse — the regime of most of our early-era cells — which is why
every model in the family is written non-centered.

In sports analytics, the play-by-play data itself comes from the Retrosheet
archive. The closest methodological predecessor is openWAR (Baumer, Jensen, and
Matthews, 2015), an open, reproducible player-value system that propagates
uncertainty rather than reporting only point values. Marchi and Albert (2014)
is an applied introduction to baseball data analysis. Measurement choices in
the baseball record have also been studied directly: Kalist and Spurr (2006)
find evidence of home-team bias in official scorers' hit-versus-error calls.
That result motivates treating scorer behavior as part of the observation
process, without presuming that a scorer decision can be retrospectively
corrected. Acharya et al. (2008) show that conventional park-factor formulas
can be distorted by unbalanced schedules and inflationary bias, and propose an
ANOVA weighted fixed-effects estimator. It is relevant as an example of a
baseball quantity whose estimate depends on an explicit model for the recording
and game context, rather than as evidence for this paper's hierarchical model.

Our target differs from these studies: we characterize the observation process
for event details and report uncertainty in its fitted coverage surfaces. The
full-history play-by-play output remains an imputed candidate, not a validated
historical reconstruction; the paper does not claim to recover the unrecorded
details as ground truth.
