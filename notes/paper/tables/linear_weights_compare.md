Comparison of Bayesian-estimated run values (`main_models.linear_weights_estimated`, propagated from `re-full-eraregime-v4`) against the deterministic empirical run values (`main_models.linear_weights`) for the 2015 NL season, joined on `(season, league, play)`; deviations are small (all but one under 0.016 runs, centered near zero rather than systematically negative) and the deterministic value falls inside the 94% HDI for every play type.

| league | play | play_category | deterministic_value | estimated_mean | hdi_low | hdi_high | diff |
|--------|------|----------------|----------------------:|-----------------:|---------:|----------:|-------:|
| NL | HomeRun | BATTING | 1.393 | 1.395 | 1.370 | 1.420 | 0.002 |
| NL | Triple | BATTING | 1.077 | 1.062 | 0.999 | 1.121 | -0.015 |
| NL | Double | BATTING | 0.747 | 0.749 | 0.721 | 0.776 | 0.002 |
| NL | ReachedOnError | BATTING | 0.476 | 0.497 | 0.469 | 0.524 | 0.021 |
| NL | Single | BATTING | 0.436 | 0.440 | 0.426 | 0.456 | 0.004 |
| NL | HitByPitch | BATTING | 0.318 | 0.323 | 0.302 | 0.344 | 0.005 |
| NL | Walk | BATTING | 0.299 | 0.302 | 0.285 | 0.318 | 0.003 |
| NL | WildPitch | BASERUNNING | 0.217 | 0.209 | 0.184 | 0.236 | -0.008 |
| NL | PassedBall | BASERUNNING | 0.200 | 0.199 | 0.197 | 0.202 | -0.001 |
| NL | IntentionalWalk | BATTING | 0.188 | 0.181 | 0.151 | 0.212 | -0.007 |
| NL | OtherAdvanceSafe | BASERUNNING | 0.175 | 0.181 | 0.150 | 0.210 | 0.006 |
| NL | StolenBase | BASERUNNING | 0.179 | 0.179 | 0.154 | 0.205 | 0.000 |
| NL | SacrificeFly | BATTING | -0.032 | -0.036 | -0.074 | 0.002 | -0.004 |
| NL | SacrificeHit | BATTING | -0.183 | -0.185 | -0.221 | -0.149 | -0.002 |
| NL | InPlayOut | BATTING | -0.230 | -0.233 | -0.238 | -0.228 | -0.003 |
| NL | StrikeOut | BATTING | -0.256 | -0.259 | -0.264 | -0.254 | -0.003 |
| NL | CaughtStealing | BASERUNNING | -0.405 | -0.414 | -0.440 | -0.387 | -0.009 |
| NL | OtherAdvanceOut | BASERUNNING | -0.441 | -0.442 | -0.445 | -0.440 | -0.001 |
| NL | PickedOff | BASERUNNING | -0.442 | -0.455 | -0.489 | -0.416 | -0.013 |
| NL | DoublePlay | BATTING | -0.769 | -0.781 | -0.810 | -0.752 | -0.012 |

Note: season 2015 is present in both tables so no substitution was needed. The join key used `(season, league, play)` since `linear_weights` does have a `league` column. The result is filtered to `league = 'NL'` (matching the documented example) to keep the table under the row-count limit. Two of the 20 cells, `PassedBall` (96 events) and `OtherAdvanceOut` (23 events), sit at or below the 100-occurrence floor in both tables and carry the corpus-pooled per-play value on each side, which is why their intervals are far narrower than their neighbours' and their diffs are near zero. Across the whole estimated table, 827 of 5,036 cells sit at or below the floor and share one pooled value per play (HDI width 0.001 to 0.009, median 0.0045) against widths of 0.009 to 0.216 (median 0.061) for the 4,209 cells above it; the model's column contract declares `is_imputed` for the floor cells, but the table restated on 2026-09-04 carries the pooled values without that column, so `n_events <= 100` is the working test until the column publishes. Three cells are at the floor in the estimated table but not in the deterministic one, because the estimated propagation drops transitions whose start state has no posterior cell.
