MODEL (
  name main_models.stg_box_score_team_batting_lines,
  kind FULL,
  description 'Team-level batting lines from box score accounts. These are generally rare and are only present in certain files.',
  grain (game_id, side),
  columns (
    game_id VARCHAR,
    side VARCHAR,
    at_bats BIGINT,
    runs BIGINT,
    hits BIGINT,
    doubles BIGINT,
    triples BIGINT,
    home_runs BIGINT,
    rbi BIGINT,
    sacrifice_hits BIGINT,
    sacrifice_flies BIGINT,
    hit_by_pitches BIGINT,
    walks BIGINT,
    intentional_walks BIGINT,
    strikeouts BIGINT,
    stolen_bases BIGINT,
    caught_stealing BIGINT,
    grounded_into_double_plays BIGINT,
    reached_on_interference BIGINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
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
    not_null(columns := (game_id, side)),
    unique_grain(columns := (game_id, side)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_team_batting_lines.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_team_batting_lines
),

renamed AS (
    SELECT
        game_id,
        side,
        at_bats::BIGINT AS at_bats,
        runs::BIGINT AS runs,
        hits::BIGINT AS hits,
        doubles::BIGINT AS doubles,
        triples::BIGINT AS triples,
        home_runs::BIGINT AS home_runs,
        rbi::BIGINT AS rbi,
        sacrifice_hits::BIGINT AS sacrifice_hits,
        sacrifice_flies::BIGINT AS sacrifice_flies,
        hit_by_pitch::BIGINT AS hit_by_pitches,
        walks::BIGINT AS walks,
        intentional_walks::BIGINT AS intentional_walks,
        strikeouts::BIGINT AS strikeouts,
        stolen_bases::BIGINT AS stolen_bases,
        caught_stealing::BIGINT AS caught_stealing,
        grounded_into_double_plays::BIGINT AS grounded_into_double_plays,
        reached_on_interference::BIGINT AS reached_on_interference
    FROM source
)

SELECT * FROM renamed
