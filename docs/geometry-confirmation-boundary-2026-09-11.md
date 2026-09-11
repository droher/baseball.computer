# Geometry confirmation boundary

No current primary `VALIDATE` game can honestly be called globally unused across the recorded modeling history.

The current corrected snapshot contains 26,106 `VALIDATE` games eligible for recorded location-side scoring and 30,631 eligible for trajectory. Every one of those games appeared with a non-null corresponding target in the legacy event-universe pretrainer's `VALIDATE` partition. That pretrainer used `VALIDATE` targets for training callbacks and model selection. The legacy geometry deep runner likewise supplied its `VALIDATE` frame to every fold and full fit for early stopping and exported predictions on it.

Legacy Bayesian geometry provides a second, less complete exposure path. Its data preparation did not honor `primary_fold`; it applied `game_hash_fold(game_id, fold_count=10)` to all eligible geometry rows. Among today's eligible primary `VALIDATE` games, 23,496 location-side games and 27,545 trajectory games entered the pre-subsampling training pools. The remaining 2,610 and 3,086 games, respectively, entered the internal held-out pools. The artifacts used a 10,000-row fit limit and do not retain exact selected-game evidence, so the first pair cannot be described as actually fitted. None were absent from the legacy input dataset. The pretrainer exposure independently establishes the global boundary.

The recent no-learned-input reference fits have a narrower and cleaner boundary. Their frozen queries selected only primary `TRAIN` and `TEST`, their lineages state `uses_validate_partition=false`, and the trajectory interaction reused those exact extracts. All current primary `VALIDATE` games are unused by this component family. They may therefore support a **prospective component-local confirmation**, provided no further stress fit, calibration fit, model choice, diagnostic, or human label inspection touches the reserved games before the final locked score. They cannot support a globally virgin or never-before-seen claim.

## Prospective reservation

The frozen source is the read-only corrected geometry database at `artifacts/statistical/research/geometry-v2-global-side-20260911/bc.db`, schema `main_models__geometry_v2_research_20260911`, source snapshot `geometry-v2-20260911`. Eligibility uses only game ID, partition, target dimension, observedness, positive weight, and season; no `VALIDATE` target class was selected or scored.

The candidate pool is the union of games with an eligible recorded location-side or trajectory event in primary `VALIDATE`: 30,681 games, including 26,056 eligible for both targets. Its sorted length-prefixed UTF-8 game-ID digest is `7b78c61dbd39e17916377d7272fe5bb9cb72d7afdcaf1fa00b026c8477895f68`.

Reserve `geometry-confirmation-reserve-v1` takes a deterministic nominal 20%: compute SHA-256 over UTF-8 `geometry-confirmation-reserve-v1:` followed by each game ID, interpret the first eight bytes as unsigned big-endian, and retain values whose remainder modulo 5 is zero. This freezes 6,105 games. The sorted length-prefixed UTF-8 reserve digest is `5ac641fe78b19e98ec5cb576ebb85e652ff289e445e0f6e54e109c523a614dba`.

The reserve contains 5,183 location-side games and 6,094 trajectory games. Its pre-1988 coverage is 2,557 location-side games with 11,954 eligible events and 3,468 trajectory games with 42,106 eligible events. Post-1987 coverage is 2,626 games for each target, with 137,626 location-side and 140,860 trajectory events.

Every new stress fit must regenerate the game list from the exact frozen eligibility query and verify both digests before excluding it. Fit, tuning, masking, calibration, and model-selection inputs must anti-join the reserve by `game_id` before reading target labels. The final scorer may read reserve labels once after the stress protocol, model specification, sampler budget, calibration rule, metrics, slices, and decision thresholds are immutable. Any earlier use downgrades it to development evidence.

The machine-readable evidence, exact query, counts, digests, artifact references, and claim limits are in [geometry-confirmation-boundary-2026-09-11.json](geometry-confirmation-boundary-2026-09-11.json).
