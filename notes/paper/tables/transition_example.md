Full posterior end-class distribution for the runner-on-first, 1-out start state (`start_state = '1_1'`; the state key is outs then bit-coded bases), 2015 NL, from `main_models.state_transition_summary` (`state-transition-v5`); all 25 possible end classes are shown (including near-zero and structurally-zero ones) and `prob_mean` sums to 1.0000 across them, illustrating the reachability mask. The eight 0-out end classes carry exactly zero because the mask pins them; the five classes that are reachable by out count but impossible from this base state in one event (`1_7`, `2_3`, `2_5`, `2_6`, `2_7`) are free parameters the posterior leaves near 1.5e-6, which rounds to 0.0000 below.

| end_class  | prob_mean | prob_hdi_lower | prob_hdi_upper |
|------------|----------:|----------------:|----------------:|
| 2_1        | 0.3898    | 0.3779          | 0.4017          |
| 1_3        | 0.1873    | 0.1776          | 0.1977          |
| inning_end | 0.1204    | 0.1118          | 0.1286          |
| 1_2        | 0.0910    | 0.0836          | 0.0982          |
| 2_2        | 0.0853    | 0.0780          | 0.0925          |
| 1_5        | 0.0400    | 0.0350          | 0.0447          |
| 2_0        | 0.0277    | 0.0236          | 0.0318          |
| 1_0        | 0.0232    | 0.0196          | 0.0269          |
| 1_6        | 0.0183    | 0.0150          | 0.0214          |
| 1_4        | 0.0140    | 0.0113          | 0.0169          |
| 2_4        | 0.0028    | 0.0019          | 0.0038          |
| 1_1        | 0.0003    | 0.0002          | 0.0005          |
| 0_7        | 0.0000    | 0.0000          | 0.0000          |
| 0_6        | 0.0000    | 0.0000          | 0.0000          |
| 0_5        | 0.0000    | 0.0000          | 0.0000          |
| 0_4        | 0.0000    | 0.0000          | 0.0000          |
| 1_7        | 0.0000    | 0.0000          | 0.0000          |
| 0_3        | 0.0000    | 0.0000          | 0.0000          |
| 0_2        | 0.0000    | 0.0000          | 0.0000          |
| 0_1        | 0.0000    | 0.0000          | 0.0000          |
| 2_3        | 0.0000    | 0.0000          | 0.0000          |
| 2_5        | 0.0000    | 0.0000          | 0.0000          |
| 2_6        | 0.0000    | 0.0000          | 0.0000          |
| 2_7        | 0.0000    | 0.0000          | 0.0000          |
| 0_0        | 0.0000    | 0.0000          | 0.0000          |

Note: the query previously used `start_state = '0_0'`; on the corrected population (real regular-season events in innings 1 through 8) the `0_0 → 0_0` self-transition that carried 0.31 of the mass in the earlier surface (substitutions and no-play rows) is gone, and the one-out start was chosen because it shows both the out-count mask and a legal inning-ending double play. The self-transition `1_1 → 1_1` (batter reaches first while the runner scores from first) carries 0.0003.
