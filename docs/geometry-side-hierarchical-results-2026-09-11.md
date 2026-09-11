# Hierarchical global-side recorded-label control

September 11, 2026. Both frozen full scenarios completed with four chains, 1,000 warmup draws, 4,000 saved draws, nutpie, target acceptance 0.95, maximum tree depth 12, and seed 20260911. Both converged. The candidate failed the development screen because its historical calibration did not meet the frozen cohort requirements.

This is iterative development on primary TRAIN labels whose internal folds have already been inspected. It estimates the corrected recorded global-side label. It is not confirmation, a naturally missing-outcome validation, or evidence for physical trajectory standardization. The 6,105-game component-local reserve remained sealed and had zero overlap.

## Results

Lower log loss and Brier are better. Positive gain intervals favor the hierarchical model over the add-one contextual baseline fitted on the same retained rows.

| Scenario | Evaluation | Model / baseline log loss | Log-loss gain 95% game bootstrap | Model / baseline Brier | Brier gain 95% game bootstrap | Model ECE | Max share bias | Decision |
|---|---:|---:|---:|---:|---:|---:|---:|---|
| Game split | 700,718 events, 24,306 games | 1.093128 / 1.103680 | [0.010180, 0.010955] | 0.643887 / 0.651277 | [0.007169, 0.007631] | 0.012021 | 0.002442 | Failed: historical decades |
| Game split, pre-1988 | 55,270 events, 12,015 games | 1.012557 / 1.052836 | [0.038570, 0.041904] | 0.595198 / 0.629052 | [0.032449, 0.035026] | 0.038020 | 0.005974 | Aggregate slice passed |
| Fit 1988+, evaluate pre-1988 | 55,270 events, 12,015 games | 1.078069 / 1.125810 | [0.046103, 0.049398] | 0.636836 / 0.670281 | [0.032375, 0.034485] | 0.074334 | 0.120830 | Failed |

The all-era game split passes every overall predictive and calibration check. Its aggregate pre-1988 slice also passes. That aggregate hides failures in every supported decade from the 1910s through the 1970s. The 1910s, 1920s, and 1930s exceed the 0.02 class-share-bias ceiling; the 1940s through 1970s exceed the 0.05 ECE ceiling, with the 1950s failing both. Decades from the 1980s onward pass.

Backward extrapolation is decisive. Although the hierarchy improves both losses over the contextual fallback, it fails the absolute calibration requirements overall and in every supported historical decade. It overpredicts Middle by 0.120830 of all events while underpredicting Left by 0.063174 and Right by 0.053608. The worst decade share biases are 0.185838 in the 1940s, 0.183317 in the 1950s, and 0.178375 in the 1970s. Retaining persistent result-family signal and population priors improves ranking and probability loss, but it does not transport the recorded historical side distribution.

## Numerical and boundary evidence

The game split had maximum R-hat 1.00559, minimum bulk ESS 2,017.88, minimum tail ESS 3,298.03, and zero divergences. Backward extrapolation had maximum R-hat 1.00160, minimum bulk ESS 4,306.35, minimum tail ESS 6,739.92, and zero divergences. Numerical failure does not explain the scientific result.

The game fit used 80,450 selected inner-fit TRAIN events from 43,455 games. Backward extrapolation used 74,104 post-1988 selected inner-fit TRAIN events from 38,017 games. Fit and evaluation games were disjoint. Both scenarios reported zero reserve overlap. No TEST or VALIDATE labels were read.

The result rejects this model as a reliable historical global-side solution. It remains useful as a converged recorded-label control. Further work needs an explicit historical measurement/source model or a target whose era transport is independently anchored; recalibrating on these exposed development folds would not provide independent evidence. Aggregate posterior uncertainty, observation-process validation, and confirmation remain unsupported.

The exact machine-readable result and artifact digests are in [geometry-side-hierarchical-results-2026-09-11.json](geometry-side-hierarchical-results-2026-09-11.json). The immutable local artifact is `artifacts/statistical/backtests/geometry_reliability/20260911-location-side-full-v1` and includes source rows, predictions, posterior draws, prior checks, diagnostics, code, protocol, exposure ledger, and logs.
