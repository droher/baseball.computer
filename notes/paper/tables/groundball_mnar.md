Ground share among batted-ball events with directly recorded trajectory (observed) vs. including trajectory deduced from the fielding record (observed + derived), by era, from `main_models.model_input_geometry` (trajectory dimension) — derived rows act as a partial-truth peek at the population scorers left unrecorded.

| era       | n_observed | n_derived | ground_share_observed | ground_share_obs_plus_derived | gap    |
|-----------|-----------:|----------:|-----------------------:|--------------------------------:|-------:|
| pre-1950  |    712,469 |   763,993 |                  0.3368 |                           0.6800 | 0.3432 |
| 1950-1987 |    699,353 | 1,057,776 |                  0.4410 |                           0.7777 | 0.3367 |
| 1988+     |  4,759,451 |    53,922 |                  0.4495 |                           0.4557 | 0.0062 |

Note: broad class is ground = {GroundBall, GroundBallBunt}, air = {Fly, LineDrive, PopUp, AirBall, PopUpBunt, LineDriveBunt}; the ambiguous/unclassifiable bunt labels UnspecifiedBunt and FoulBunt are excluded from both numerator and denominator. All derived rows carry `deduced_value = GroundBall` by construction (the deduction only fires when a ground ball is implied by the fielding record), so the observed+derived ground share pre-1988 is pulled sharply upward relative to the observed-only share — direct evidence that pre-1988 scorers under-recorded trajectory selectively rather than at random. The gap collapses to near zero (0.6 points) once Retrosheet-era play-by-play (1988+) recorded trajectory comprehensively.
