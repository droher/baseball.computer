# Trajectory internal development diagnosis

This research-only check used an 80/20 game-disjoint split inside the frozen 100,000-row primary TRAIN selection. It did not read primary TEST or VALIDATE labels.

| Model | All log loss | All Brier | All ECE | Pre-1988 log loss |
|---|---:|---:|---:|---:|
| marginal | 1.352610 | 0.707154 | 0.002880 | 1.560370 |
| decade_result | 1.204729 | 0.654616 | 0.004992 | 1.383802 |
| additive | 1.206605 | 0.653408 | 0.012725 | 1.415614 |
| era_result_interaction | 1.194619 | 0.648405 | 0.006630 | 1.369338 |

The era-by-result interaction changed log loss relative to the additive logit by +0.011986 overall and +0.046276 before 1988. This isolates one structural hypothesis under deterministic MAP-style regularized logistic fits. It does not diagnose Bayesian sampling, prove that an interaction caused the primary TEST result, or justify tuning on primary TEST.
