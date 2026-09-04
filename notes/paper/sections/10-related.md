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
with the outcome. §5's derivation is a selection model in exactly this sense: a
latent class is drawn, then recorded with probability s(c, x), and Bayes' rule
turns the selection process into a per-class reweight of the fitted
distribution — the offset δ_c is the per-class selection log-odds, a
selection-model parameter acting on the recorded class before the softmax
normalizes. The difference from Heckman's program is what we ask of the
parameter. Heckman buys identification with parametric structure — joint
normality of the two equations' errors, or an exclusion restriction that moves
selection without moving the outcome. Our record offers no credible exclusion
restriction — no covariate moves a 1930s scorer's recording behavior without
also proxying the play itself — and we decline the distributional route: δ_c is
treated as unidentified from the observed slice, bounded from below where a
partial-truth slice internal to the record supplies a floor, and otherwise
swept, not solved for.

That sweep places the paper in the second tradition: pattern-mixture models and
delta-adjustment sensitivity analysis. Little (1993) specified the distribution
of missing values directly through identifying restrictions rather than a
mechanism, and observed that such models are chronically underidentified —
there is no information in the observed data about the distribution of the
missing stratum, so the honest output is inference across a range of
restrictions rather than a point. Scharfstein, Rotnitzky, and Robins (1999)
made the same move inside the selection formulation: fix a nonidentified
selection-bias parameter at each value in a plausible range, estimate under
each, and report the trajectory — sensitivity analysis in place of an
untestable assumption. That template became standard practice in the
clinical-trials missing-data literature as delta adjustment and tipping-point
analysis, where a fixed offset δ is added to values imputed under a
missing-at-random fit and swept until the conclusion changes; Molenberghs and
Kenward (2007) treat the framework at book length, and Cro, Morris, Kenward,
and Carpenter (2020) give the current practical guide to controlled multiple
imputation in this style. The published object of §5 is this device
transplanted: δ_c is a delta adjustment on the class logits, the
missing-at-random fit is the δ = 0 point, and the ribbon is the tipping-point
trajectory, reported per class. What the paper adds to the template is a hard
constraint on the sweep — a deduced-trajectory partial-truth slice internal to
the record that places a floor under one class's share on the missing stratum,
and a known-truth subslice on which the missing-at-random fit can be scored —
and a masked backtest that tests the offset's functional form against a
selection process it does not nest. An earlier revision read the same slice as
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
Carlo and show that a non-centered parameterization restores efficient sampling
when data are sparse — the regime of most of our early-era cells — which is why
every model in the family is written non-centered.

In sports analytics, the play-by-play data itself comes from the Retrosheet
archive, and the closest methodological predecessor is openWAR (Baumer, Jensen,
and Matthews, 2015), which built an open, reproducible player-value system and
carried uncertainty through to its estimates rather than reporting point values —
the same commitment this paper makes for coverage surfaces. Marchi and Albert
(2014) is the standard applied treatment of baseball data analysis in this
register. Measurement problems in the baseball record itself have drawn less
attention, but not none. Kalist and Spurr (2006) document a home-team bias in
official scorers' hit-versus-error calls — direct evidence that the human
observation layer this paper models imprints itself on the record, on a field
this paper treats as official rather than corrects. Acharya et al. (2008)
treat park-factor estimation as a small-sample shrinkage problem, the
instability that Model F's hierarchical pooling addresses. A peer-reviewed
literature on measurement error in modern tracking data has not yet formed,
and we cite none. Our work differs from
all of these in target: rather than estimating player value or correcting a
single statistic, we model the process that decided which event details were
recorded at all, and publish the resulting uncertainty as the estimand.

### References

Acharya, R. A., Ahmed, A. J., D'Amour, A. N., Lu, H., Morris, C. N., Oglevee,
B. D., Peterson, A. W., & Swift, R. N. (2008). Improving Major League Baseball
park factor estimates. *Journal of Quantitative Analysis in Sports*, 4(2),
Article 4.

Baumer, B. S., Jensen, S. T., & Matthews, G. J. (2015). openWAR: An open source
system for evaluating overall player performance in Major League Baseball.
*Journal of Quantitative Analysis in Sports*, 11(2), 69–84.

Betancourt, M., & Girolami, M. (2013). Hamiltonian Monte Carlo for hierarchical
models. arXiv:1312.0906.

Cro, S., Morris, T. P., Kenward, M. G., & Carpenter, J. R. (2020). Sensitivity
analysis for clinical trials with missing continuous outcome data using
controlled multiple imputation: A practical guide. *Statistics in Medicine*,
39(21), 2815–2842.

Gelman, A., Carlin, J. B., Stern, H. S., Dunson, D. B., Vehtari, A., & Rubin,
D. B. (2013). *Bayesian Data Analysis* (3rd ed.). Chapman & Hall/CRC.

Groves, R. M., Dillman, D. A., Eltinge, J. L., & Little, R. J. A. (Eds.)
(2002). *Survey Nonresponse*. Wiley.

Heckman, J. J. (1976). The common structure of statistical models of
truncation, sample selection and limited dependent variables and a simple
estimator for such models. *Annals of Economic and Social Measurement*, 5(4),
475–492.

Heckman, J. J. (1979). Sample selection bias as a specification error.
*Econometrica*, 47(1), 153–161.

Kalist, D. E., & Spurr, S. J. (2006). Baseball errors. *Journal of
Quantitative Analysis in Sports*, 2(4), Article 3.

Little, R. J. A. (1993). Pattern-mixture models for multivariate incomplete
data. *Journal of the American Statistical Association*, 88(421), 125–134.

Little, R. J. A., & Rubin, D. B. (2019). *Statistical Analysis with Missing
Data* (3rd ed.). Wiley.

Marchi, M., & Albert, J. (2014). *Analyzing Baseball Data with R*. Chapman &
Hall/CRC.

Molenberghs, G., & Kenward, M. G. (2007). *Missing Data in Clinical Studies*.
Wiley.

Rubin, D. B. (1976). Inference and missing data. *Biometrika*, 63(3), 581–592.

Rubin, D. B. (1977). Formalizing subjective notions about the effect of
nonrespondents in sample surveys. *Journal of the American Statistical
Association*, 72(359), 538–543.

Scharfstein, D. O., Rotnitzky, A., & Robins, J. M. (1999). Adjusting for
nonignorable drop-out using semiparametric nonresponse models. *Journal of the
American Statistical Association*, 94(448), 1096–1120.
