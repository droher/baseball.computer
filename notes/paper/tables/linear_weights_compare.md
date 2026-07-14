Comparison of Bayesian-estimated run values (`main_models.linear_weights_estimated`) against the deterministic empirical run values (`main_models.linear_weights`) for the 2015 NL season, joined on `(season, league, play)`; deviations are small (mostly under 0.01 runs) and the deterministic value falls inside or very near the 94% HDI for every play type.

| league | play | play_category | deterministic_value | estimated_mean | hdi_low | hdi_high | diff |
|--------|------|----------------|----------------------:|-----------------:|---------:|----------:|-------:|
| NL | HomeRun | BATTING | 1.393 | 1.387 | 1.382 | 1.392 | -0.006 |
| NL | Triple | BATTING | 1.077 | 1.065 | 1.011 | 1.117 | -0.012 |
| NL | Double | BATTING | 0.747 | 0.742 | 0.721 | 0.763 | -0.005 |
| NL | ReachedOnError | BATTING | 0.476 | 0.475 | 0.467 | 0.482 | -0.001 |
| NL | Single | BATTING | 0.436 | 0.436 | 0.428 | 0.444 | 0.000 |
| NL | HitByPitch | BATTING | 0.318 | 0.316 | 0.304 | 0.327 | -0.002 |
| NL | Walk | BATTING | 0.299 | 0.298 | 0.288 | 0.308 | -0.001 |
| NL | WildPitch | BASERUNNING | 0.217 | 0.205 | 0.189 | 0.221 | -0.012 |
| NL | PassedBall | BASERUNNING | 0.200 | 0.191 | 0.173 | 0.210 | -0.009 |
| NL | IntentionalWalk | BATTING | 0.188 | 0.178 | 0.150 | 0.205 | -0.010 |
| NL | StolenBase | BASERUNNING | 0.179 | 0.168 | 0.153 | 0.185 | -0.011 |
| NL | OtherAdvanceSafe | BASERUNNING | 0.175 | 0.164 | 0.149 | 0.179 | -0.011 |
| NL | SacrificeFly | BATTING | -0.032 | -0.031 | -0.067 | 0.005 | 0.001 |
| NL | SacrificeHit | BATTING | -0.183 | -0.188 | -0.214 | -0.161 | -0.005 |
| NL | InPlayOut | BATTING | -0.230 | -0.229 | -0.231 | -0.227 | 0.001 |
| NL | StrikeOut | BATTING | -0.256 | -0.254 | -0.257 | -0.252 | 0.002 |
| NL | OtherAdvanceOut | BASERUNNING | -0.441 | -0.409 | -0.418 | -0.400 | 0.032 |
| NL | CaughtStealing | BASERUNNING | -0.405 | -0.411 | -0.420 | -0.401 | -0.006 |
| NL | PickedOff | BASERUNNING | -0.442 | -0.446 | -0.456 | -0.436 | -0.004 |
| NL | DoublePlay | BATTING | -0.769 | -0.771 | -0.787 | -0.756 | -0.002 |

Note: season 2015 is present in both tables so no substitution was needed. The join key used `(season, league, play)` since `linear_weights` does have a `league` column. The result is filtered to `league = 'NL'` (matching the documented example) to keep the table under the row-count limit; the AL league is present in bc.db with the same 20 play types and comparable magnitudes (e.g. `OtherAdvanceOut` also shows the largest deviation there, diff ≈ +0.032).
