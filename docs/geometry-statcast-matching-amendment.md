# Statcast matching amendment, September 11, 2026

This amendment follows the failed `20260911-statcast-mechanics-v1` experiment: 10 of 12 games matched completely. It changes the plate-appearance counter, not the frozen game selection, angle target, or mechanics acceptance thresholds. No modern evaluation angles have been acquired.

In Baltimore on August 30, 2023, and Washington on June 7, 2023, a runner made the third out during an unfinished plate appearance. Statcast assigned that partial appearance a number. Counting only completed Retrosheet plate appearances caused subsequent numbers to lag by one. The diagnosis used event order, inning/frame, players, outs, pitch counts, and terminal-result presence, without using batted-ball types or angles.

For the next mechanics artifact, increment the within-game counter for a completed plate appearance, or a real nonterminal event that ends the half-inning: no plate-appearance result, `no_play_flag` false, and starting outs plus outs on play at least three. Completed appearances still increment only once. Preserve exact player identity, inning/frame, and one-to-one PA-key checks. If this rule fails on another game, retain its failure and diagnose it; do not force a match using trajectory agreement.

Read-only replay on the two affected training games gave 57 of 57 and 58 of 58 exact identity/inning matches. This is a data-connection repair, not validation of a statistical model. The failed artifact remains in the evidence.

The next run will reuse verified raw acquisitions in `20260911-statcast-mechanics-v1` and create `20260911-statcast-mechanics-v2`. Acceptance remains 12 complete game matches and at least 95% angle availability with all missing values accounted for. Full fitting acquisition additionally requires a reviewed, explicitly bound mechanics manifest and recomputation of its pairing and availability evidence. The raw crosswalk ZIP is bound directly in the tracked inputs. The 121 modern-angle evaluation games and the historical confirmation reserve remain unopened.
