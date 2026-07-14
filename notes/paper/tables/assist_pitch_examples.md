# assist_pitch_examples

## (a) `assist_count_distribution`: bases-empty vs. runner-on-first double-play cell

Comparing a routine bases-empty groundout to a runner-on-first/0-out ball-in-play out shows multi-assist mass jumping from 0.8% to 36.6%, consistent with the double-play states carrying the model's multi-assist probability.

| result_family | base_state_start | outs_start | assist_count_class | prob_mean |
| --- | ---: | ---: | --- | ---: |
| out_in_play | 0 | 0 | 1 | 0.992 |
| out_in_play | 0 | 0 | 2 | 0.008 |
| out_in_play | 0 | 0 | 3 | 0.000 |
| out_in_play | 0 | 0 | 4 | 0.000 |
| out_in_play | 1 | 0 | 1 | 0.631 |
| out_in_play | 1 | 0 | 2 | 0.366 |
| out_in_play | 1 | 0 | 3 | 0.002 |
| out_in_play | 1 | 0 | 4 | 0.000 |

## (b) `pitch_summary_distribution`: strikeouts, 2015 NL (all 12 final-count classes)

Every strikeout ends on two strikes: the eight non-two-strike classes carry zero posterior mass, recovering the structural constraint without hard-coding it.

| final_count_class | balls | strikes | prob_mean |
| --- | ---: | ---: | ---: |
| b1_s2 | 1 | 2 | 0.341 |
| b2_s2 | 2 | 2 | 0.283 |
| b0_s2 | 0 | 2 | 0.226 |
| b3_s2 | 3 | 2 | 0.150 |
| b0_s0 | 0 | 0 | 0.000 |
| b0_s1 | 0 | 1 | 0.000 |
| b1_s0 | 1 | 0 | 0.000 |
| b1_s1 | 1 | 1 | 0.000 |
| b2_s0 | 2 | 0 | 0.000 |
| b2_s1 | 2 | 1 | 0.000 |
| b3_s0 | 3 | 0 | 0.000 |
| b3_s1 | 3 | 1 | 0.000 |
