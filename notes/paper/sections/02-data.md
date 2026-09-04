## The record and its gaps

The corpus is a DuckDB database built from several historical baseball source
families, each recorded at a different grain and playing a different role
<!-- src: notes/data-coverage-implementation/README.md -->. Play-by-play sources
carry event and event-player detail — base-out state, batting, pitching,
fielding, baserunning, batted-ball clues, and pitch sequences — and are the only
source from which event-level quantities can be estimated. Box scores carry
official aggregate totals at the game-player and game-team grain: batting lines,
pitching lines, fielding lines, line scores, decisions, and earned runs. Gamelog
and schedule sources establish that a game happened and how it ended. Season
supplements carry season-player and season-team totals that fill in where event
or box coverage is coarser <!-- src: notes/data-coverage-implementation/README.md -->.
The invariant that organizes all of them is that a box-score putout, a
rule-derived batted-ball location, an estimated expected putout, and a
sampled synthetic event are different quantities and are never stored in one
column <!-- src: notes/data-coverage-implementation/README.md -->.

Within the 1910–2025 target span the source mix is dominated by play-by-play but
not exclusively so. The snapshot holds 205,845 play-by-play games in the span, 1,953
box-score-only games, and 4 gamelog-only games <!-- src: notes/data-coverage-implementation/README.md -->,
plus 41 play-by-play games from the 1900s decade that lie before the target
span and surface as a `1900` row wherever a table is keyed by decade
<!-- src: tables/corpus_by_decade.md -->. The event table, `event_states_full`,
contains 18,141,020 events, all of them from play-by-play games; that count is
the whole table and so includes whatever the 41 early games contribute
<!-- src: tables/corpus_by_decade.md -->. Surfaces keyed by season — park
factors and Model G's cells — cover every season the source carries, which is
why a few descriptive rows fall before 1910.
Event-level estimation is scoped to those games; the box-score-only and
gamelog-only rows stay at aggregate grain and are not given fabricated event
records <!-- src: notes/data-coverage-implementation/README.md -->. At the
coarser season-team grain the same split appears as 2,929 play-by-play
team-seasons against 269 box-score team-seasons
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->,
but source absence is not uniform within a season, so the modeling layer works
from a game-and-dimension ledger rather than a season-level flag.

Having event rows is not the same as having every field on them. The gaps that
this paper's models target are field-level, and they are large. Of 12,038,982
batted-ball rows in the deterministic derivation table, 3,992,018 retain an
unknown final trajectory even after rule-based inference has recovered every
broad class the fielding evidence supports, and 6,989,832 have no recorded
location <!-- src: notes/data-coverage-implementation/README.md -->. On the
fielding side, 463,102 events carry unknown putouts within the span
<!-- src: notes/data-coverage-implementation/README.md -->. An unknown putout is
worse than a single missing field because it usually means the assist chain is
also unrecorded: the record may know that an out occurred without knowing who
recorded it or whether an assist was involved
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
These counts are not acceptance thresholds; they size the first modeling targets
<!-- src: notes/data-coverage-implementation/README.md -->.

Missingness is also multi-dimensional within a single event, which is why no
one completeness flag can describe a row. The database already exposes coverage
along separate axes — trajectory, general location, batted-to-fielder,
depth, angle, and strength for batted balls; count, pitch sequence, pitch
results, and strike types for pitches; putouts, assists, and errors for fielding
credit <!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
A row can be complete for basic batting, incomplete for batted-ball trajectory,
complete for team fielding, incomplete for player fielding credit, and unusable
for pitch-sequence metrics all at once
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
The coverage of each axis, in turn, is stratified by result: batted-ball
trajectory and location are recorded more often on outs than on hits, and the
metric layer already tracks known-trajectory rates separately for the two
because the missingness mechanism is result-dependent
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
Known location among hits is not a random sample of all hit locations; it is a
scorer-selected subset <!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.

The one dimension that dominates all of these is era. Coverage of trajectory,
location, pitch sequence, and fielding credit is sparse and heavily selected in
the early decades, improves through the middle of the century, and reaches
modern batted-ball and location fidelity only in the most recent seasons; the
defensive-shift era at the end of the span then changes the meaning of a
fielder's position as a proxy for location
<!-- src: notes/data-coverage-implementation/data-coverage-taxonomy-1910-2025.md -->.
The full decade-by-dimension coverage structure — observation propensity by era
and dimension from Model A — is an empirical result rather than a fixed corpus
statistic, and we present it as a table in the Results section rather than
restating raw counts here. What the counts above establish is only the shape of
the problem: complete outcomes, field-level gaps that run into the millions, and
a missingness pattern that tracks the scorer and the era rather than the game.

The modeling datasets carry two game-level partitions, and the paper's
held-out numbers come from one or the other, never both. The datasets stamp
`primary_fold` from `HASH(game_id) % 100` — buckets 0–69 `TRAIN`, 70–84
`VALIDATE`, 85–99 `TEST` — and the deep supplements of §6 train, early-stop,
and report on it <!-- src: bc/models/intermediate/modeling_datasets/model_input_event_universe.sql -->.
Every Bayesian fit, and every coverage gate in §7, instead holds out fold 0
of a ten-fold BLAKE2s hash of `game_id`, removed before any subsampling
<!-- src: bc/python_models/statistical/splits.py -->. The two hashes are
unrelated, so the Bayes holdout is a 10% sample of games that cuts across all
three deep partitions; §6 states what that means for the deep covariate.
