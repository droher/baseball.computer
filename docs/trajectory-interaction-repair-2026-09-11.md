# Trajectory interaction sampling repair

Frozen after diagnosing `artifacts/statistical/backtests/trajectory_interaction/20260911-full-v1` and before any repair fit. TEST scores did not determine this repair.

The first full fit had zero divergences, zero maximum-depth transitions, BFMI from 0.969 to 1.143, stable step sizes from 0.064 to 0.082, median tree acceptance 0.964, and maximum energy error 1.25. Its interaction surface reached R-hat 1.026 and minimum bulk ESS 125.8. The numerical gate failed in the slower intercept and batter-hand direction: `alpha_class[Fly]` had R-hat 1.071 and bulk ESS 93.7; `delta_batter_hand[__MISSING__, Fly]` had R-hat 1.073 and bulk ESS 83.8. Those coordinates had posterior correlation 0.838 and visible within-chain mean movement.

The repair changes only the number of saved draws from 1,000 to 4,000 per chain. It retains the exact frozen source bytes, selected 100,000 TRAIN events, model terms, train-only encoders, priors, four chains, 1,000 warmup draws, nutpie backend, target acceptance 0.95, maximum tree depth 12, and seed 20260911. More warmup, higher target acceptance, and a model change are not indicated by the observed sampler statistics.

The repair must use a new immutable artifact directory and must archive the exact implementation used before sampling. It passes numerically only if R-hat is at most 1.05, bulk and tail ESS are at least 100, and divergences are zero. Predictive comparisons remain descriptive until the repair passes this gate. No additional model or sampler change may depend on TEST performance.
