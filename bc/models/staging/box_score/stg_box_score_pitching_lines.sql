MODEL (
  name main_models.stg_box_score_pitching_lines,
  kind FULL,
  description 'Aggregate pitching data for each player in each game, derived from Retroshet box score data.',
  column_descriptions (
    game_id = @doc('game_id'),
    pitcher_id = @doc('pitcher_id'),
    side = @doc('side'),
    nth_pitcher = 'Nth player to pitch for this team in this game. Differentiates appearances by the same player who switches between pitcher and other positions.',
    outs_recorded = @doc('outs_recorded'),
    no_out_batters = 'Number of batters faced by this pitcher in his final inning of work, if he did not record an out in that inning.',
    batters_faced = @doc('batters_faced'),
    hits = @doc('hits'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    runs = @doc('runs'),
    earned_runs = @doc('earned_runs'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    strikeouts = @doc('strikeouts'),
    hit_by_pitches = @doc('hit_by_pitches'),
    wild_pitches = @doc('wild_pitches'),
    balks = @doc('balks'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    singles = @doc('singles'),
    total_bases = @doc('total_bases'),
    on_base_opportunities = @doc('on_base_opportunities'),
    on_base_successes = @doc('on_base_successes'),
    games_started = @doc('games_started'),
    games_relieved = @doc('games_relieved'),
    games_finished = @doc('games_finished')
  ),
  audits (
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := pitcher_id, to_model := main_models.people, to_column := player_id)
  ),
);






WITH source AS (
    SELECT * FROM box_score.box_score_pitching_lines
),

renamed AS (
    SELECT
        game_id,
        pitcher_id,
        side,
        nth_pitcher,
        outs_recorded,
        no_out_batters,
        batters_faced,
        hits,
        doubles,
        triples,
        home_runs,
        runs,
        earned_runs,
        walks,
        intentional_walks,
        strikeouts,
        -- TODO: Change in original
        hit_batsmen AS hit_by_pitches,
        wild_pitches,
        balks,
        sacrifice_hits,
        sacrifice_flies,
        -- TODO: Fix rows where XBH > H
        CASE WHEN hits::INT - (home_runs + triples + doubles) < 0 THEN NULL
            ELSE hits - (home_runs + triples + doubles)
        END AS singles,
        singles + doubles * 2 + triples * 3 + home_runs * 4 AS total_bases,
        -- This is a different formula vs batting lines because we don't have at bats.
        -- The one missing piece is catcher's interferfence, which is extremely rare 
        -- if not non-existent in box-score only games.
        batters_faced - COALESCE(sacrifice_hits, 0) AS on_base_opportunities,
        hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) AS on_base_successes,
        CASE WHEN nth_pitcher = 1 THEN 1 ELSE 0 END AS games_started,
        CASE WHEN nth_pitcher != 1 THEN 1 ELSE 0 END AS games_relieved,
        CASE WHEN nth_pitcher = MAX(nth_pitcher) OVER (PARTITION BY game_id, side)
                THEN 1
            ELSE 0
        END AS games_finished,
    FROM source
)

SELECT * FROM renamed
