# Airborne recording-regime development results

The `geometry-air-regime-posterior-v1` development screen did not pass. At the primary concentration of 30, the recorded-subtype by result-family candidate beat both reference arms in every split family, met every per-class calibration limit, and met the necessary posterior predictive coverage screen. It failed one prespecified limit: the leave-one-season-out 2015 slice has a LineDrive class-share bias of -0.0203 against the 0.02 limit. The concentration-3 sensitivity passed every limit and the concentration-300 sensitivity failed several, so the verdict depends on the prior strength. The protocol treats that as a robustness failure requiring investigation, not as license to adopt concentration 3. Nothing here is promoted, published, or used for historical reconstruction.

## Evidence integrity

The scored run is `artifacts/statistical/backtests/geometry_reliability/20260911-air-regime-development-full-v2`, manifest SHA-256 `573d02c17f79cc71eeb84c5c91105872cbc2b25fc30969467eb00a0b75d24beb`, 355 manifest entries. The report is `20260911-air-regime-report-full-v2`, manifest SHA-256 `42372f39bf0de6f5455903c1f2dffef7b27c5093aa3c83022b8164447119060f`. An earlier run of the same experiment from commit `34b75e4` (`20260911-air-regime-development-full-v1`, report `20260911-air-regime-report-full-v1`) produced identical metrics, calibration, coverage, sensitivity, and decision tables; the rerun exists only because review fixes changed the runner and reporter after that run. The reporter verified every run file against the run manifest, every frozen source copy against the run bindings, and its own loaded dependencies against those frozen copies before computing anything. It reproduced the run's metrics table and paired bootstrap exactly.

The run bound the protocol at SHA-256 `7a99736d5ad90069df515a10115557ca25c074a68f3ca47a15143d72edd8bd46`, the runner at `738248cda43d2cd489af5b067a7eac0dbd16d63c139349f53dfa06e98ebec9b5`, and every declared source module, with NumPy 2.4.6, Polars 1.40.1, SciPy 1.17.1, Pydantic 2.13.3, and Python 3.13.5. Source commit `50699f6`. The reporter's own hash is `83e94607fb6cec9907169053436478b00653bf4558a20313753684974ffee84e`.

The input frame is the bound 373-game development frame (SHA-256 `cfdf29…`), 19,436 events, 9,869 eligible local-Air events. Eligible events come from 367 of the 373 games. An operational smoke on 12 metadata-selected games (`20260911-air-regime-development-smoke-v2`, report `20260911-air-regime-report-smoke-v2`) ran first and checked schemas, count conservation, and report compatibility. It made no acceptance decision.

## Numerical status

All 168 fits (three concentrations, fourteen folds, four arms) produced 798 fitted cells. Every cell passed the 64/128 moment comparison on the first pair and used order 128. No fit needed a higher order and none failed. Every held-out eligible event in every arm received a `posterior_cell` prediction; no prior-only cells occurred. Fits used 4,096 shared draws with root seed 20260911. Training sets ranged from 7,068 to 8,604 eligible events.

## Prespecified screen at concentration 30

Overall metrics for the candidate arm:

| Split family | Log loss | Brier | Classwise ECE | Largest absolute class-share bias |
| --- | ---: | ---: | ---: | ---: |
| Game | 0.5157 | 0.2975 | 0.0056 | 0.0005 |
| Park | 0.5153 | 0.2978 | 0.0074 | 0.0007 |
| Leave one season out | 0.5161 | 0.2977 | 0.0052 | 0.0003 |

Every season slice is supported (at least 30 games and 500 events). Season slices at concentration 30:

| Split | Season | Events | Classwise ECE | Fly bias | LineDrive bias | PopUp bias |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Game | 2015 | 2,333 | 0.0090 | +0.0087 | -0.0115 | +0.0028 |
| Game | 2019 | 2,496 | 0.0142 | -0.0028 | +0.0067 | -0.0039 |
| Game | 2023 | 2,523 | 0.0073 | -0.0022 | +0.0038 | -0.0016 |
| Game | 2025 | 2,517 | 0.0044 | -0.0011 | -0.0013 | +0.0025 |
| Park | 2015 | 2,333 | 0.0124 | +0.0086 | -0.0122 | +0.0036 |
| Park | 2019 | 2,496 | 0.0142 | -0.0025 | +0.0058 | -0.0033 |
| Park | 2023 | 2,523 | 0.0076 | -0.0032 | +0.0042 | -0.0009 |
| Park | 2025 | 2,517 | 0.0085 | -0.0017 | -0.0013 | +0.0030 |
| Season | 2015 | 2,333 | 0.0155 | +0.0140 | **-0.0203** | +0.0063 |
| Season | 2019 | 2,496 | 0.0156 | -0.0076 | +0.0151 | -0.0075 |
| Season | 2023 | 2,523 | 0.0094 | -0.0047 | +0.0072 | -0.0025 |
| Season | 2025 | 2,517 | 0.0084 | -0.0016 | -0.0036 | +0.0052 |

The pooled model's season biases were 0.05 to 0.09 with opposite signs by era. The regime model removes that pattern: game and park season slices are within 0.013 and the leave-one-season-out slices within 0.021.

Paired 500-replicate whole-game bootstrap gain intervals are positive everywhere. Against the result-only arm, log-loss gains are 0.3660 `[0.3480, 0.3867]`, 0.3662 `[0.3493, 0.3862]`, and 0.3662 `[0.3470, 0.3857]` for game, park, and season splits; Brier gains are 0.2435 `[0.2308, 0.2590]`, 0.2433 `[0.2312, 0.2582]`, and 0.2440 `[0.2310, 0.2582]`. Against the recorded-only arm, log-loss gains are 0.0416 `[0.0363, 0.0468]`, 0.0420 `[0.0366, 0.0468]`, and 0.0421 `[0.0364, 0.0468]`; Brier gains are 0.0129 `[0.0110, 0.0152]`, 0.0127 `[0.0107, 0.0144]`, and 0.0127 `[0.0106, 0.0147]`.

Against the archived pooled candidate on identical events, the regime candidate gains 0.0128 `[0.0095, 0.0161]`, 0.0131 `[0.0097, 0.0164]`, and 0.0223 `[0.0179, 0.0269]` in log loss and 0.0090 `[0.0066, 0.0113]`, 0.0089 `[0.0065, 0.0112]`, and 0.0158 `[0.0126, 0.0188]` in Brier for game, park, and season splits.

## Per-class calibration

The pre-execution clarification requires each class's 15-bin ECE at most 0.05 on supported slices. At concentration 30 the largest per-class ECE is 0.0126 on whole-family slices and 0.0230 on season slices. At concentrations 3 and 300 the season-slice maxima are 0.0250 and 0.0299. Every per-class check passes at every concentration; averaging did not hide a poorly calibrated class.

## Posterior predictive count coverage

Nominal 95% whole-game interval coverage for the candidate at concentration 30, all seasons, 367 games per family:

| Split family | Fly | LineDrive | PopUp |
| --- | ---: | ---: | ---: |
| Game | 0.9646 `[0.9428, 0.9809]` | 0.9782 `[0.9646, 0.9918]` | 0.9700 `[0.9495, 0.9837]` |
| Park | 0.9619 `[0.9373, 0.9796]` | 0.9728 `[0.9564, 0.9864]` | 0.9700 `[0.9495, 0.9837]` |
| Season | 0.9673 `[0.9455, 0.9837]` | 0.9755 `[0.9591, 0.9891]` | 0.9700 `[0.9510, 0.9864]` |

Mean interval widths are 7.4 counts for Fly and 5.5 for the other classes. The necessary screen (at least 90% coverage for nominal 95% intervals on every supported class-by-family and class-by-season slice) passes at every concentration; the lowest supported 95% slice is 0.9247 (2019 Fly, park split at 30 and game split at 3). Nominal 90% intervals are less clean: 0.8222 for 2015 Fly in the season split at concentration 300 and 0.8710 for 2019 PopUp in the park split at concentration 3. Monte Carlo split-batch endpoint spans never exceed 2 counts and interval-flag disagreement fractions never exceed 0.039.

Held-out season totals are shown separately because four seasons cannot establish a rate. At concentration 30, 123 of 132 season cohorts fell inside nominal 95% intervals and 113 of 132 inside nominal 90% intervals. The leave-one-season-out LineDrive totals were covered in 3 of 4 seasons. The mean 95% width for a leave-one-season-out total is 76 to 100 counts against about 2,400 events.

These intervals reflect the model's own conditional independence assumption within cells. Recovery simulation supports the implementation; empirical coverage on this sample cannot establish nominal calibration under real recording practice.

## Concentration sensitivity

| Concentration | Score and previous calibration screen | Per-class ECE | Coverage screen | Combined |
| ---: | --- | --- | --- | --- |
| 30 (primary) | fail (season split, 2015 bias 0.0203) | pass | pass | fail |
| 3 | pass (season split, 2015 bias 0.0185) | pass | pass | pass |
| 300 | fail (every split family) | pass | pass | fail |

Only the score-and-previous-calibration criterion changes across sensitivities. The failing quantity is monotone in the concentration: the 2015 leave-one-season-out LineDrive bias is -0.0185, -0.0203, and -0.0278 at 3, 30, and 300, and the 2015 Fly bias is +0.0116, +0.0140, and +0.0294. Stronger pooling toward the shared global composition pulls the early regime toward the late mapping, and the early regime is represented by 2019 alone when 2015 is held out. The 2015 and 2019 game and park slices, where both seasons train, stay within 0.013. The same two seasons therefore differ from each other by about one to two LineDrive share points in the direction of a within-regime effect that the two-regime structure does not carry.

## Reference sensitivity

Unresolved local-Air references remain in the denominators. By regime, the resolved class-share bounds are:

| Regime | Local Air | Unresolved | Fly | LineDrive | PopUp |
| --- | ---: | ---: | ---: | ---: | ---: |
| Early (2015, 2019) | 5,445 | 616 | 0.363 to 0.476 | 0.381 to 0.494 | 0.143 to 0.256 |
| Late (2023, 2025) | 5,323 | 283 | 0.392 to 0.445 | 0.392 to 0.446 | 0.163 to 0.216 |

Fixed-prediction adversarial relabeling at concentration 30 in the game split: the candidate's log-loss gain over the recorded-only arm is erased by relabeling 121 events (1.23%) overall, 78 (1.62%) in the early regime, and 51 (1.01%) in the late regime; the Brier gain by 147 (1.49%), 114 (2.36%), and 60 (1.19%). The gain over the result-only arm needs 442 (4.48%) relabelings for log loss and 1,100 (11.15%) for Brier. Park and season splits are within 0.3 points of these. The margin over the recorded-only arm is fragile to a one to two percent adversarial reference error. These fractions are assumptions, not estimated error rates; row-level measurement origin remains unknown.

## Consequence

The regime structure fixed the failure that ended the pooled experiment. The remaining failure is a threshold-scale within-regime season difference between 2015 and 2019, and the verdict's dependence on the concentration. Under the protocol, that requires investigation rather than threshold relaxation or picking concentration 3. The next specification should decide, before scoring, whether the estimand includes a season level inside each regime, and should examine whether the 2015-to-2019 difference tracks reference construction (2015 was the first Statcast season, with lower exact `bb_type` agreement) rather than recording practice. That specification must be frozen before any further predictive scoring.

Independent modern confirmation, genuinely missing-label validity, historical transport, reference measurement quality, scorer authorship, and the separate side/location model remain unaddressed. The 121 deferred modern-angle games and the 6,105-game historical reserve stay sealed. Machine-readable results are in `docs/geometry-air-regime-development-results-2026-09-11.json`.
