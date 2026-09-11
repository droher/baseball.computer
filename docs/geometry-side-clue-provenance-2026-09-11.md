# Historical location-side fielder-clue provenance

This diagnostic uses all 8,434,463 `location_side` rows in the corrected geometry ledger's PRIMARY TRAIN partition. It retains observed, derived, and naturally unknown rows, including all result families and training weights. The sealed 6,105-game confirmation reserve has zero overlap; no TEST or VALIDATE target labels were read or scored. The exact aggregate query and 1,368 era/result/status cells are archived in [`20260911-side-clue-provenance-v1`](../artifacts/statistical/backtests/geometry_reliability/20260911-side-clue-provenance-v1/).

## Provenance finding

`batted_location_general`, `batted_to_fielder`, and `fielder_chain` are separate parser outputs from one Retrosheet play record, not independent measurements. General location is parsed from slash-delimited contact-description modifiers. For hits, `batted_to_fielder` is the first position after the hit indicator; for non-strikeout batting outs, it is the first fielder in the main fielding play, with a runner-advance fallback. The fielding chain is the ordered `fielders_data` built from that same parsed play. The exact branches are in [`play.rs`](</Users/davidroher/Repos/baseball.computer.rs/src/event_file/play.rs>) at lines 374–383, 1527–1578, and 2162–2221, [`game_state.rs`](</Users/davidroher/Repos/baseball.computer.rs/src/event_file/game_state.rs>) at lines 667–705 and 978–992, and [`schemas.rs`](</Users/davidroher/Repos/baseball.computer.rs/src/event_file/schemas.rs>) at lines 199–242 and 370–395.

The corrected ledger maps the explicit general-location modifier to the observed target. When that modifier is unavailable, [`calc_batted_ball_type.sql`](../bc/models/intermediate/event_level/calc_batted_ball_type.sql) maps `batted_to_fielder` through `seed_hit_to_fielder_categories`, and [`event_observation_geometry.sql`](../bc/models/intermediate/coverage/event_observation_geometry.sql) stores that result as `deduced_value`. Every one of the 4,279,727 derived rows has a known `batted_to_fielder` from 1 through 9. Using this field as an ordinary predictor for those rows would reproduce the existing deterministic deduction.

## Availability

| Target status | Events | `batted_to_fielder` 1–9 | Any chain | Chain with a known 1–9 position |
| --- | ---: | ---: | ---: | ---: |
| Observed | 3,539,235 | 3,242,353 (91.61%) | 2,312,758 (65.35%) | 2,312,396 (65.34%) |
| Derived | 4,279,727 | 4,279,727 (100%) | 3,345,504 (78.17%) | 3,345,414 (78.17%) |
| Naturally unknown | 615,501 | 0 | 321,734 (52.27%) | 9 (0.0015%) |

The apparently high chain coverage among naturally unknown rows does not provide a usable fielder: 321,725 of those chains contain only parser position `0` (unknown), 293,767 have no chain, and just 9 contain any known position. Those nine span 1910–1959 and retain `batted_to_fielder=0`; a later fielder in a play chain can be a relay, assist, or base putout and is not physical evidence of where the ball landed.

All ledger rows in this diagnostic have `source_acquisition_status=observed`. The ledger is driven by parsed batted-ball events, so whole play-by-play source-block loss removes the event record and therefore removes location, `batted_to_fielder`, and the fielding chain together. The empirical survival shown here concerns field-level omission within an available play record.

## Decision

No new historical side fit using `batted_to_fielder` or `fielder_chain` as ordinary covariates is scientifically justified. A location-modifier-only mask may retain `batted_to_fielder` as an explicitly shared-source weak measurement, but that experiment would evaluate recovery of the recorded convention and must model or label the deterministic fielder channel separately. Any mask described as play-by-play or source-block loss must mask location, `batted_to_fielder`, and fielding-chain fields jointly.

For the naturally unknown population, neither candidate carries meaningful known-position coverage. The next side candidate needs an independently sourced measurement or a joint observation model that distinguishes recorded location from fielder-derived convention; fielders must not be treated as physical side truth.

The first artifact validation expected an unused `missing` status and failed after writing the aggregate. The failure is retained in `run.log`; final validation resumed from that checkpoint and confirmed the actual status domain is `observed`, `derived`, and `unknown_code`.
