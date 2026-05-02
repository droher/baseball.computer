MODEL (
  name main_models.stg_box_score_pinch_hitting_lines,
  kind FULL,
  description 'Box score batting lines that are specific to stats accumulated while a player is pinch hitting.',
  grain (game_id, pinch_hitter_id, inning),
  column_descriptions (
    game_id = @doc('game_id'),
    inning = @doc('inning'),
    side = @doc('side'),
    at_bats = @doc('at_bats'),
    runs = @doc('runs'),
    hits = @doc('hits'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    hit_by_pitches = @doc('hit_by_pitches'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    strikeouts = @doc('strikeouts'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    grounded_into_double_plays = @doc('grounded_into_double_plays')
  ),
  audits (
    not_null(columns := (game_id, pinch_hitter_id, inning)),
    unique_grain(columns := (game_id, pinch_hitter_id, inning)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := pinch_hitter_id, to_model := main_models.people, to_column := player_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_pinch_hitting_lines.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_pinch_hitting_lines
),

renamed AS (
    SELECT
        game_id,
        pinch_hitter_id,
        inning,
        side,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        rbi,
        sacrifice_hits,
        sacrifice_flies,
        hit_by_pitch AS hit_by_pitches,
        walks,
        intentional_walks,
        strikeouts,
        stolen_bases,
        caught_stealing,
        grounded_into_double_plays,
        reached_on_interference

    FROM source
)

SELECT * FROM renamed
