MODEL (
  name main_models.event_baserunning_stats,
  kind FULL,
  grain (event_key, baserunner),
  columns (
    event_key UINTEGER,
    game_id VARCHAR,
    batting_team_id TEAM_ID,
    fielding_team_id TEAM_ID,
    runner_id VARCHAR,
    current_pitcher_id VARCHAR,
    baserunner BASERUNNER,
    runner_lineup_position UTINYINT,
    reached_on_event_key UINTEGER,
    charge_event_key UINTEGER,
    explicit_charged_pitcher_id VARCHAR,
    runs UTINYINT,
    outs_on_basepaths UTINYINT,
    times_reached_base UTINYINT,
    times_lead_runner UTINYINT,
    times_force_on_runner UTINYINT,
    times_next_base_empty UTINYINT,
    stolen_base_opportunities UTINYINT,
    stolen_base_opportunities_second UTINYINT,
    stolen_base_opportunities_third UTINYINT,
    stolen_base_opportunities_home UTINYINT,
    stolen_bases UTINYINT,
    stolen_bases_second UTINYINT,
    stolen_bases_third UTINYINT,
    stolen_bases_home UTINYINT,
    caught_stealing UTINYINT,
    caught_stealing_second UTINYINT,
    caught_stealing_third UTINYINT,
    caught_stealing_home UTINYINT,
    picked_off UTINYINT,
    picked_off_first UTINYINT,
    picked_off_second UTINYINT,
    picked_off_third UTINYINT,
    picked_off_caught_stealing UTINYINT,
    advances_on_wild_pitches UTINYINT,
    advances_on_passed_balls UTINYINT,
    advances_on_balks UTINYINT,
    advances_on_unspecified_plays UTINYINT,
    advances_on_defensive_indifference UTINYINT,
    advances_on_errors UTINYINT,
    plate_appearances_while_on_base UTINYINT,
    balls_in_play_while_running UTINYINT,
    balls_in_play_while_on_base UTINYINT,
    batter_total_bases_while_running UTINYINT,
    batter_total_bases_while_on_base UTINYINT,
    extra_base_advance_attempts UTINYINT,
    bases_advanced TINYINT,
    bases_advanced_on_balls_in_play TINYINT,
    surplus_bases_advanced_on_balls_in_play TINYINT,
    outs_on_extra_base_advance_attempts UTINYINT,
    outs_avoided_on_errors UTINYINT,
    unforced_outs_on_basepaths UTINYINT,
    extra_base_chances UTINYINT,
    extra_bases_taken UTINYINT
  ),
  column_descriptions (
    event_key = @doc('event_key'),
    game_id = @doc('game_id'),
    batting_team_id = @doc('team_id'),
    fielding_team_id = @doc('team_id'),
    runner_id = @doc('runner_id'),
    current_pitcher_id = 'Retrosheet person ID of the pitcher on the mount at the end of the event',
    baserunner = @doc('baserunner'),
    runner_lineup_position = '1-9 lineup position of the baserunner',
    reached_on_event_key = @doc('reached_on_event_key'),
    charge_event_key = @doc('charge_event_key'),
    explicit_charged_pitcher_id = @doc('explicit_charged_pitcher_id'),
    runs = @doc('runs'),
    outs_on_basepaths = @doc('outs_on_basepaths'),
    times_reached_base = @doc('times_reached_base'),
    times_lead_runner = @doc('times_lead_runner'),
    times_force_on_runner = @doc('times_force_on_runner'),
    times_next_base_empty = @doc('times_next_base_empty'),
    stolen_base_opportunities = @doc('stolen_base_opportunities'),
    stolen_base_opportunities_second = @doc('stolen_base_opportunities_second'),
    stolen_base_opportunities_third = @doc('stolen_base_opportunities_third'),
    stolen_base_opportunities_home = @doc('stolen_base_opportunities_home'),
    stolen_bases = @doc('stolen_bases'),
    stolen_bases_second = @doc('stolen_bases_second'),
    stolen_bases_third = @doc('stolen_bases_third'),
    stolen_bases_home = @doc('stolen_bases_home'),
    caught_stealing = @doc('caught_stealing'),
    caught_stealing_second = @doc('caught_stealing_second'),
    caught_stealing_third = @doc('caught_stealing_third'),
    caught_stealing_home = @doc('caught_stealing_home'),
    picked_off = @doc('picked_off'),
    picked_off_first = @doc('picked_off_first'),
    picked_off_second = @doc('picked_off_second'),
    picked_off_third = @doc('picked_off_third'),
    picked_off_caught_stealing = @doc('picked_off_caught_stealing'),
    advances_on_wild_pitches = @doc('advances_on_wild_pitches'),
    advances_on_passed_balls = @doc('advances_on_passed_balls'),
    advances_on_balks = @doc('advances_on_balks'),
    advances_on_unspecified_plays = @doc('advances_on_unspecified_plays'),
    advances_on_defensive_indifference = @doc('advances_on_defensive_indifference'),
    advances_on_errors = @doc('advances_on_errors'),
    plate_appearances_while_on_base = @doc('plate_appearances_while_on_base'),
    balls_in_play_while_running = @doc('balls_in_play_while_running'),
    balls_in_play_while_on_base = @doc('balls_in_play_while_on_base'),
    batter_total_bases_while_running = @doc('batter_total_bases_while_running'),
    batter_total_bases_while_on_base = @doc('batter_total_bases_while_on_base'),
    extra_base_advance_attempts = @doc('extra_base_advance_attempts'),
    bases_advanced = @doc('bases_advanced'),
    bases_advanced_on_balls_in_play = @doc('bases_advanced_on_balls_in_play'),
    surplus_bases_advanced_on_balls_in_play = @doc('surplus_bases_advanced_on_balls_in_play'),
    outs_on_extra_base_advance_attempts = @doc('outs_on_extra_base_advance_attempts'),
    outs_avoided_on_errors = @doc('outs_avoided_on_errors'),
    unforced_outs_on_basepaths = @doc('unforced_outs_on_basepaths'),
    extra_base_chances = @doc('extra_base_chances'),
    extra_bases_taken = @doc('extra_bases_taken')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_event_baserunning_stats.parquet'
  ),
);







WITH joined AS (
    SELECT
        event_key,
        b.baserunner,
        b.game_id,
        e.batting_team_id,
        e.fielding_team_id,
        e.base_state,
        b.runner_id,
        e.pitcher_id AS current_pitcher_id,
        b.runner_lineup_position,
        b.reached_on_event_key,
        b.charge_event_key,
        b.explicit_charged_pitcher_id,
        b.baserunner != 'Batter' AS is_on_base,
        b.attempted_advance_to_base IS NOT NULL AS is_advance_attempt,
        part.plate_appearance_result IS NOT NULL AS is_plate_appearance,
        b.is_out,
        b.is_advance_attempt AND NOT b.is_out AS is_successful,
        COALESCE(b.advanced_on_error_flag, FALSE) AS advanced_on_error_flag,
        COALESCE(b.explicit_out_flag, FALSE) AS explicit_out_flag,
        baserunner_meta.numeric_value AS number_base_from,
        bases_meta.numeric_value AS number_base_to,
        COALESCE(part.is_in_play, FALSE) AS is_in_play,
        COALESCE(part.is_hit, FALSE) AS is_hit,
        COALESCE(b.baserunning_play_type, 'None') AS baserunning_play_type,
        COALESCE(part.total_bases, 0)::UTINYINT AS batter_total_bases,
        CASE WHEN b.baserunner = 'Batter'
                THEN e.base_state & 1 = 0
            WHEN b.baserunner = 'First'
                THEN e.base_state >> 1 & 1 = 0
            WHEN b.baserunner = 'Second'
                THEN e.base_state >> 2 & 1 = 0
            ELSE FALSE
        END AS is_next_base_empty,
        CASE WHEN b.baserunner = 'Batter'
                -- By convention we say that the batter is never the lead runner
                THEN FALSE
            WHEN b.baserunner = 'First'
                THEN e.base_state = 1
            WHEN b.baserunner = 'Second'
                THEN e.base_state < 4
            ELSE TRUE
        END AS is_lead_runner,
        CASE WHEN b.baserunner = 'Second'
                THEN e.base_state & 1 = 1
            WHEN b.baserunner = 'Third'
                THEN e.base_state = 7
            ELSE TRUE
        END AS is_force_on_runner,
        CASE WHEN e.plate_appearance_result = 'Single'
                THEN b.baserunner IN ('First', 'Second')
            WHEN e.plate_appearance_result = 'Double'
                THEN b.baserunner = 'First'
            ELSE FALSE
        END AS is_extra_base_chance,

    FROM main_models.stg_event_baserunners AS b
    LEFT JOIN main_models.stg_events AS e USING (event_key)
    LEFT JOIN main_seeds.seed_plate_appearance_result_types AS part
        USING (plate_appearance_result)
    LEFT JOIN main_seeds.seed_baserunner_info AS baserunner_meta
        ON b.baserunner = baserunner_meta.baserunner
    LEFT JOIN main_seeds.seed_bases_info AS bases_meta
        ON b.attempted_advance_to_base = bases_meta.base
    WHERE NOT e.no_play_flag
),

final AS (
    SELECT
        event_key,
        game_id,
        batting_team_id,
        fielding_team_id,
        runner_id,
        current_pitcher_id,
        baserunner,
        runner_lineup_position,
        reached_on_event_key,
        charge_event_key,
        explicit_charged_pitcher_id,
        (is_successful AND number_base_to = 4)::UTINYINT AS runs,
        is_out::UTINYINT AS outs_on_basepaths,
        -- Note that this is different from OBP - it includes fielders choices, errors, etc.
        (is_successful AND baserunner = 'Batter')::UTINYINT AS times_reached_base,
        (is_lead_runner)::UTINYINT AS times_lead_runner,
        (is_force_on_runner)::UTINYINT AS times_force_on_runner,
        (is_next_base_empty)::UTINYINT AS times_next_base_empty,
        (baserunning_play_type = 'StolenBase')::UTINYINT AS stolen_bases,
        (stolen_bases > 0 AND baserunner = 'First')::UTINYINT AS stolen_bases_second,
        (stolen_bases > 0 AND baserunner = 'Second')::UTINYINT AS stolen_bases_third,
        (stolen_bases > 0 AND baserunner = 'Third')::UTINYINT AS stolen_bases_home,
        (baserunning_play_type LIKE '%CaughtStealing')::UTINYINT AS caught_stealing,
        (caught_stealing > 0 AND baserunner = 'First')::UTINYINT AS caught_stealing_second,
        (caught_stealing > 0 AND baserunner = 'Second')::UTINYINT AS caught_stealing_third,
        (caught_stealing > 0 AND baserunner = 'Third')::UTINYINT AS caught_stealing_home,
        (
            stolen_bases + caught_stealing > 0
            OR is_next_base_empty AND is_on_base
        )::UTINYINT AS stolen_base_opportunities,
        (stolen_base_opportunities > 0 AND baserunner = 'First')::UTINYINT AS stolen_base_opportunities_second,
        (stolen_base_opportunities > 0 AND baserunner = 'Second')::UTINYINT AS stolen_base_opportunities_third,
        (stolen_base_opportunities > 0 AND baserunner = 'Third')::UTINYINT AS stolen_base_opportunities_home,
        (baserunning_play_type LIKE 'PickedOff%' AND is_out)::UTINYINT AS picked_off,
        (picked_off > 0 AND baserunner = 'First')::UTINYINT AS picked_off_first,
        (picked_off > 0 AND baserunner = 'Second')::UTINYINT AS picked_off_second,
        (picked_off > 0 AND baserunner = 'Third')::UTINYINT AS picked_off_third,
        (baserunning_play_type = 'PickedOffCaughtStealing')::UTINYINT AS picked_off_caught_stealing,

        (baserunning_play_type = 'WildPitch' AND is_successful)::UTINYINT AS advances_on_wild_pitches,
        (baserunning_play_type = 'PassedBall' AND is_successful)::UTINYINT AS advances_on_passed_balls,
        (baserunning_play_type = 'Balk' AND is_successful)::UTINYINT AS advances_on_balks,
        (
            baserunning_play_type = 'OtherAdvance' AND is_successful
        )::UTINYINT AS advances_on_unspecified_plays,
        (
            baserunning_play_type = 'DefensiveIndifference' AND is_successful
        )::UTINYINT AS advances_on_defensive_indifference,
        (
            (baserunning_play_type = 'AdvancedOnError' OR advanced_on_error_flag) AND is_successful
        )::UTINYINT AS advances_on_errors,

        (is_plate_appearance AND is_on_base)::UTINYINT AS plate_appearances_while_on_base,
        (is_in_play)::UTINYINT AS balls_in_play_while_running,
        (is_in_play AND is_on_base)::UTINYINT AS balls_in_play_while_on_base,
        batter_total_bases AS batter_total_bases_while_running,
        CASE WHEN is_on_base
                THEN batter_total_bases
            ELSE 0
        END::UTINYINT AS batter_total_bases_while_on_base,
        CASE WHEN is_hit AND number_base_to - number_base_from > batter_total_bases
                THEN 1
            ELSE 0
        END::UTINYINT AS extra_base_advance_attempts,
        CASE WHEN is_successful
                THEN number_base_to - number_base_from
            ELSE 0
        END::INT1 AS bases_advanced,
        CASE WHEN is_successful AND is_in_play AND NOT advanced_on_error_flag
                THEN number_base_to - number_base_from
            ELSE 0
        END::INT1 AS bases_advanced_on_balls_in_play,
        CASE WHEN is_successful AND is_in_play AND NOT advanced_on_error_flag
                THEN number_base_to
                    - number_base_from
                    - LEAST(4 - number_base_from, batter_total_bases)
            ELSE 0
        END::INT1 AS surplus_bases_advanced_on_balls_in_play,
        (
            is_out AND explicit_out_flag AND number_base_to - number_base_from > 1
        )::UTINYINT AS outs_on_extra_base_advance_attempts,
        (explicit_out_flag AND NOT is_out)::UTINYINT AS outs_avoided_on_errors,
        -- Tags do not count as an unforced out if they occur when a force was in play.
        -- This will cause us to miss some cases when a runner on base is tagged out
        -- after advancing, straying off the bag, and then failing to return.
        (
            is_out AND (
                outs_on_extra_base_advance_attempts = 1
                OR NOT is_force_on_runner
                -- Force outs can't happen on the same plays as hits,
                -- so runners marked out on hits are always unforced
                OR is_hit
            )
        )::UTINYINT AS unforced_outs_on_basepaths,

        (is_extra_base_chance)::UTINYINT AS extra_base_chances,
        (is_extra_base_chance AND extra_base_advance_attempts > 0 AND is_successful)::UTINYINT AS extra_bases_taken,

    FROM joined
)

SELECT * FROM final
