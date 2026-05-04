MODEL (
  name main_models.stg_box_score_batting_lines,
  kind FULL,
  description 'Aggregate offensive data for each player in each game, derived from Retroshet box score data.',
  grain (game_id, side, batter_id, lineup_position, nth_player_at_position),
  columns (
    game_id VARCHAR,
    batter_id VARCHAR,
    side SIDE,
    lineup_position UTINYINT,
    nth_player_at_position UTINYINT,
    at_bats UTINYINT,
    runs UTINYINT,
    hits UTINYINT,
    doubles UTINYINT,
    triples UTINYINT,
    home_runs UTINYINT,
    runs_batted_in UTINYINT,
    sacrifice_hits UTINYINT,
    sacrifice_flies UTINYINT,
    hit_by_pitches UTINYINT,
    walks UTINYINT,
    intentional_walks UTINYINT,
    strikeouts UTINYINT,
    stolen_bases UTINYINT,
    caught_stealing UTINYINT,
    grounded_into_double_plays UTINYINT,
    reached_on_interferences UTINYINT,
    singles UTINYINT,
    total_bases UTINYINT,
    plate_appearances UTINYINT,
    on_base_opportunities UTINYINT,
    on_base_successes UTINYINT,
    batting_outs UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    batter_id = @doc('batter_id'),
    side = @doc('side'),
    lineup_position = @doc('lineup_position'),
    nth_player_at_position = 'The nth player to occupy this spot in the order during the game',
    at_bats = @doc('at_bats'),
    runs = @doc('runs'),
    hits = @doc('hits'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    runs_batted_in = @doc('runs_batted_in'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    hit_by_pitches = @doc('hit_by_pitches'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    strikeouts = @doc('strikeouts'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    grounded_into_double_plays = @doc('grounded_into_double_plays'),
    reached_on_interferences = @doc('reached_on_interferences'),
    singles = @doc('singles'),
    total_bases = @doc('total_bases'),
    plate_appearances = @doc('plate_appearances'),
    on_base_opportunities = @doc('on_base_opportunities'),
    on_base_successes = @doc('on_base_successes'),
    batting_outs = @doc('batting_outs')
  ),
  audits (
    not_null(columns := (game_id, side, batter_id, lineup_position, nth_player_at_position)),
    unique_grain(columns := (game_id, side, batter_id, lineup_position, nth_player_at_position)),
    relationships(column := batter_id, to_model := main_models.people, to_column := player_id),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_box_score_batting_lines.parquet'
  ),
);







WITH source AS (
    SELECT * FROM box_score.box_score_batting_lines
),

renamed AS (
    SELECT
        game_id,
        batter_id,
        side,
        lineup_position,
        nth_player_at_position,
        at_bats,
        runs,
        hits,
        doubles,
        triples,
        home_runs,
        -- TODO: Change in source
        rbi AS runs_batted_in,
        sacrifice_hits,
        sacrifice_flies,
        -- TODO: Change in source
        hit_by_pitch AS hit_by_pitches,
        walks,
        intentional_walks,
        strikeouts,
        stolen_bases,
        caught_stealing,
        grounded_into_double_plays,
        -- TODO: Change in source
        reached_on_interference AS reached_on_interferences,
        -- TODO: Fix rows with 0 hits but 1+ XBH and where hits > AB
        CASE WHEN hits = 0 AND doubles + triples + home_runs > 0 THEN NULL 
            ELSE hits - home_runs - triples - doubles
        END::UTINYINT AS singles,
        (singles + doubles * 2 + triples * 3 + home_runs * 4)::UTINYINT AS total_bases,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0)
        + COALESCE(sacrifice_hits, 0) + COALESCE(reached_on_interferences, 0))::UTINYINT
        AS plate_appearances,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0))::UTINYINT
        AS on_base_opportunities,
        (hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0))::UTINYINT AS on_base_successes,
        CASE WHEN hits > at_bats THEN NULL
            ELSE at_bats - hits + COALESCE(grounded_into_double_plays, 0)
        END::UTINYINT AS batting_outs,
    FROM source
)

SELECT * FROM renamed
