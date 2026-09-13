## The record and its gaps

The database combines source families recorded at different grains. PBP carries
event and event-player detail; box scores carry official game-player and
game-team totals; gamelogs establish game occurrence and outcomes; season
supplements carry season aggregates. The project builds its model layer through
SQLMesh over source Parquet files. An official total, a deterministic deduction,
an expected credit, and a sampled sequence remain distinct quantities.

The current source snapshot contains 205,886 PBP games and 18,141,020 events
spanning 1903-2025. Before 1910 there are 41 games and 3,262 events. The historical manuscript's 205,845-game count covered
1910 onward while its event count included the early games; this revision uses
the same full PBP population for both counts. Games represented only by box,
gamelog, or season records receive no generated event rows. Available box totals
may constrain attribution within an existing PBP game without bringing box-only
games into the target population.

The field registry classifies 179 columns across ten source relations: 116
imputation targets and 63 bookkeeping or derived duplicates. In the baseline,
28 targets contain null or unspecified values; accounting for whole missing
pitch blocks raises that count to 35. A target can be complete in the source and
still belong in the registry because preserving its observed value is part of
the output contract. Coverage is reconciled by era, league, source type, and
game type, rather than inferred from a single season-level flag.

A missing value does not have one meaning. An absent pitch block differs from an
unknown token within an otherwise recorded sequence. A fielding event may have
known total outs but unknown player attribution. A missing secondary umpire
identity may mean the role did not exist or was not recorded. A contact-strength
code of `Default` is unspecified or neutral; treating it as an unobserved
`Hard` or `Soft` category would impose a false taxonomy. The imputation layer
preserves these distinctions in source values and method or disposition fields.

Geometry requires particular care. Earlier artifacts called six within-zone
angle modifiers `location_side`; they do not encode global field side. The
legacy consumer now exposes that vocabulary as `location_angle`. The full PBP
candidate derives global side, depth, and edge jointly from general location,
with separate treatment of angle modifiers. Counts and performance scores for
the old target cannot be read as evidence for global-side reconstruction.

The older modeling datasets contain two unrelated game partitions. Deep
supplements use DuckDB `HASH(game_id) % 100`: buckets 0-69 for training, 70-84
for validation, and 85-99 for testing. Bayesian fits and coverage gates hold out
fold 0 of a ten-fold BLAKE2s hash, removed before subsampling. The deep pipeline
also uses five-fold BLAKE2s out-of-fold predictions within its training set.
These are different boundaries, not a common outer holdout. Section 6 explains
why cross-fitting within one stage is insufficient to certify the whole
composed predictor as free of leakage.

Population and registry evidence is retained in the September 13 release
manifest, grouped coverage report, and completion registry. The evidence ledger
accompanying this paper binds these reports to their file hashes and separates
them from the older geometry dataset and its descriptive tables.
