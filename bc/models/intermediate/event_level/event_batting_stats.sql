MODEL (
  name main_models.event_batting_stats,
  kind FULL,
  description 'This model calculates various batting statistics for each event which ended in a plate appearance. It is designed to capture the "core" hitting stats - stats about pitch sequences, baserunning, and batted balls are captured in other models, which are then joined downstream.',
  grain (event_key),
  columns (
    game_id VARCHAR,
    event_key UINTEGER,
    batter_id VARCHAR,
    pitcher_id VARCHAR,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    batter_lineup_position UTINYINT,
    plate_appearances UTINYINT,
    at_bats UTINYINT,
    hits UTINYINT,
    singles UTINYINT,
    doubles UTINYINT,
    triples UTINYINT,
    home_runs UTINYINT,
    total_bases UTINYINT,
    infield_hits UTINYINT,
    strikeouts UTINYINT,
    walks UTINYINT,
    intentional_walks UTINYINT,
    hit_by_pitches UTINYINT,
    sacrifice_flies UTINYINT,
    sacrifice_hits UTINYINT,
    reached_on_errors UTINYINT,
    reached_on_interferences UTINYINT,
    ground_rule_doubles UTINYINT,
    inside_the_park_home_runs UTINYINT,
    on_base_opportunities UTINYINT,
    on_base_successes UTINYINT,
    runs_batted_in UTINYINT,
    grounded_into_double_plays UTINYINT,
    double_plays UTINYINT,
    triple_plays UTINYINT,
    batting_outs UTINYINT,
    outs_on_play UTINYINT,
    left_on_base UTINYINT,
    left_on_base_with_two_outs UTINYINT
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    event_key = @doc('event_key'),
    batter_id = @doc('batter_id'),
    pitcher_id = @doc('pitcher_id'),
    batting_team_id = @doc('team_id'),
    fielding_team_id = @doc('team_id'),
    batter_lineup_position = @doc('lineup_position'),
    plate_appearances = @doc('plate_appearances'),
    at_bats = @doc('at_bats'),
    hits = @doc('hits'),
    singles = @doc('singles'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    total_bases = @doc('total_bases'),
    infield_hits = @doc('infield_hits'),
    strikeouts = @doc('strikeouts'),
    walks = @doc('walks'),
    intentional_walks = @doc('intentional_walks'),
    hit_by_pitches = @doc('hit_by_pitches'),
    sacrifice_flies = @doc('sacrifice_flies'),
    sacrifice_hits = @doc('sacrifice_hits'),
    reached_on_errors = @doc('reached_on_errors'),
    reached_on_interferences = @doc('reached_on_interferences'),
    ground_rule_doubles = @doc('ground_rule_doubles'),
    inside_the_park_home_runs = @doc('inside_the_park_home_runs'),
    on_base_opportunities = @doc('on_base_opportunities'),
    on_base_successes = @doc('on_base_successes'),
    runs_batted_in = @doc('runs_batted_in'),
    grounded_into_double_plays = @doc('grounded_into_double_plays'),
    double_plays = @doc('double_plays'),
    triple_plays = @doc('triple_plays'),
    batting_outs = @doc('batting_outs'),
    outs_on_play = @doc('outs_on_play'),
    left_on_base = @doc('left_on_base'),
    left_on_base_with_two_outs = @doc('left_on_base_with_two_outs')
  ),
  audits (
    not_null(columns := (event_key)),
    unique_values(columns := (event_key)),
    relationships(column := batter_id, to_model := main_models.people, to_column := player_id),
    relationships(column := batting_team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    relationships(column := event_key, to_model := main_models.stg_events, to_column := event_key),
    relationships(column := fielding_team_id, to_model := main_seeds.seed_franchises, to_column := team_id),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id),
    relationships(column := pitcher_id, to_model := main_models.people, to_column := player_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_batting_stats.parquet'
  ),
);







WITH sacs AS (
    -- TODO: Investigate single sac hit dedupe (BOS194606040-37)
    SELECT DISTINCT event_key
    FROM main_models.stg_event_flags
    WHERE flag IN ('SacrificeFly', 'SacrificeHit')
),

final AS (
    SELECT
        pa.game_id,
        pa.event_key,
        CASE WHEN result_types.plate_appearance_result = 'StrikeOut'
                THEN COALESCE(pa.strikeout_responsible_batter_id, pa.batter_id)
            ELSE pa.batter_id
        END AS batter_id,
        CASE WHEN result_types.plate_appearance_result IN ('Walk', 'IntentionalWalk')
                THEN COALESCE(pa.walk_responsible_pitcher_id, pa.pitcher_id)
            ELSE pa.pitcher_id
        END AS pitcher_id,
        pa.batting_team_id,
        pa.fielding_team_id,
        pa.batter_lineup_position,
        1::UTINYINT AS plate_appearances,
        (result_types.is_at_bat AND sacs.event_key IS NULL)::UTINYINT AS at_bats,
        result_types.is_hit::UTINYINT AS hits,
        (result_types.total_bases = 1)::UTINYINT AS singles,
        (result_types.total_bases = 2)::UTINYINT AS doubles,
        (result_types.total_bases = 3)::UTINYINT AS triples,
        (result_types.total_bases = 4)::UTINYINT AS home_runs,
        result_types.total_bases::UTINYINT AS total_bases,

        CASE WHEN pa.batted_to_fielder BETWEEN 1 AND 6 THEN hits ELSE 0 END::UTINYINT AS infield_hits,

        (result_types.plate_appearance_result = 'StrikeOut')::UTINYINT AS strikeouts,
        (result_types.plate_appearance_result IN ('Walk', 'IntentionalWalk'))::UTINYINT AS walks,
        (result_types.plate_appearance_result = 'IntentionalWalk')::UTINYINT AS intentional_walks,
        (result_types.plate_appearance_result = 'HitByPitch')::UTINYINT AS hit_by_pitches,
        (result_types.plate_appearance_result = 'SacrificeFly')::UTINYINT AS sacrifice_flies,
        (result_types.plate_appearance_result = 'SacrificeHit')::UTINYINT AS sacrifice_hits,
        (result_types.plate_appearance_result = 'ReachedOnError')::UTINYINT AS reached_on_errors,
        (result_types.plate_appearance_result = 'Interference')::UTINYINT AS reached_on_interferences,
        (result_types.plate_appearance_result = 'GroundRuleDouble')::UTINYINT AS ground_rule_doubles,
        (result_types.plate_appearance_result = 'InsideTheParkHomeRun')::UTINYINT AS inside_the_park_home_runs,

        result_types.is_on_base_opportunity::UTINYINT AS on_base_opportunities,
        result_types.is_on_base_success::UTINYINT AS on_base_successes,
        COALESCE(pa.runs_batted_in, 0)::UTINYINT AS runs_batted_in,
        COALESCE(double_plays.is_ground_ball_double_play, 0)::UTINYINT AS grounded_into_double_plays,
        COALESCE(double_plays.is_double_play, 0)::UTINYINT AS double_plays,
        COALESCE(double_plays.is_triple_play, 0)::UTINYINT AS triple_plays,
        -- The extra out from GIDPs is attributed to the batter,
        -- but for other types of double plays, the other out
        -- is considered to be a baserunning out (for now)
        result_types.is_batting_out::UTINYINT + grounded_into_double_plays AS batting_outs,
        pa.outs_on_play,
        -- We're assuming that ROEs and similar plays do not count as stranding runners
        -- Also require it to be an AB to leave out sac flies and bunts
        CASE WHEN result_types.is_batting_out AND pa.outs_on_play > 0 AND result_types.is_at_bat
                THEN pa.runners_count - pa.outs_on_play - pa.runs_on_play + 1
            ELSE 0
        END::UTINYINT AS left_on_base,
        CASE WHEN pa.outs + pa.outs_on_play = 3
                THEN left_on_base
            ELSE 0
        END::UTINYINT AS left_on_base_with_two_outs,

    FROM main_models.stg_events AS pa
    INNER JOIN main_seeds.seed_plate_appearance_result_types AS result_types
        USING (plate_appearance_result)
    LEFT JOIN main_models.event_double_plays AS double_plays USING (event_key)
    LEFT JOIN sacs USING (event_key)
    WHERE pa.plate_appearance_result IS NOT NULL
)

SELECT * FROM final
