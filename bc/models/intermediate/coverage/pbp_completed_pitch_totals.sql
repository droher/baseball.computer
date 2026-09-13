MODEL (
  name main_models.pbp_completed_pitch_totals,
  kind FULL,
  grain (game_id, pitcher_id, batter_id),
  audits (estimated_contract_complete, min_row_count(threshold := 1), unique_grain(columns := (game_id, pitcher_id, batter_id))),
  description 'Completed pitch counters by game, pitcher, and batter. Each counter keeps observed and estimated contributions separate using its declared completion method; rates use terminal completed plate appearances.'
);

WITH event_rows AS (
    SELECT
        completed.game_id,
        event.pitcher_id,
        event.batter_id,
        completed.appearance_start_event_id,
        completed.plate_appearance_result,
        completed.artifact_id,
        completed.source_snapshot_id,
        completed.confidence_status,
        completed.weak_identification_flag,
        completed.completed_pitches::BIGINT AS completed_pitches,
        completed.pitches_method,
        completed.completed_swings::BIGINT AS completed_swings,
        completed.swings_method,
        completed.completed_swings_with_contact::BIGINT AS completed_swings_with_contact,
        completed.swings_with_contact_method,
        completed.completed_strikes::BIGINT AS completed_strikes,
        completed.strikes_method,
        completed.completed_strikes_called::BIGINT AS completed_strikes_called,
        completed.strikes_called_method,
        completed.completed_strikes_swinging::BIGINT AS completed_strikes_swinging,
        completed.strikes_swinging_method,
        completed.completed_strikes_foul::BIGINT AS completed_strikes_foul,
        completed.strikes_foul_method,
        completed.completed_strikes_foul_tip::BIGINT AS completed_strikes_foul_tip,
        completed.strikes_foul_tip_method,
        completed.completed_strikes_in_play::BIGINT AS completed_strikes_in_play,
        completed.strikes_in_play_method,
        completed.completed_strikes_unknown::BIGINT AS completed_strikes_unknown,
        completed.strikes_unknown_method,
        completed.completed_balls::BIGINT AS completed_balls,
        completed.balls_method,
        completed.completed_balls_called::BIGINT AS completed_balls_called,
        completed.balls_called_method,
        completed.completed_balls_intentional::BIGINT AS completed_balls_intentional,
        completed.balls_intentional_method,
        completed.completed_balls_automatic::BIGINT AS completed_balls_automatic,
        completed.balls_automatic_method,
        completed.completed_unknown_pitches::BIGINT AS completed_unknown_pitches,
        completed.unknown_pitches_method,
        completed.completed_pitchouts::BIGINT AS completed_pitchouts,
        completed.pitchouts_method,
        completed.completed_pitcher_pickoff_attempts::BIGINT AS completed_pitcher_pickoff_attempts,
        completed.pitcher_pickoff_attempts_method,
        completed.completed_catcher_pickoff_attempts::BIGINT AS completed_catcher_pickoff_attempts,
        completed.catcher_pickoff_attempts_method,
        completed.completed_pitches_blocked_by_catcher::BIGINT AS completed_pitches_blocked_by_catcher,
        completed.pitches_blocked_by_catcher_method,
        completed.completed_pitches_with_runners_going::BIGINT AS completed_pitches_with_runners_going,
        completed.pitches_with_runners_going_method,
        completed.completed_passed_balls::BIGINT AS completed_passed_balls,
        completed.passed_balls_method,
        completed.completed_wild_pitches::BIGINT AS completed_wild_pitches,
        completed.wild_pitches_method,
        completed.completed_balks::BIGINT AS completed_balks,
        completed.balks_method
    FROM main_models.pbp_completed_pitches AS completed
    INNER JOIN main_models.stg_events AS event USING (event_key)
),
rolled AS (
    SELECT
        game_id,
        pitcher_id,
        batter_id,
        COUNT(DISTINCT appearance_start_event_id)::BIGINT AS pitching_appearances,
        COUNT(DISTINCT appearance_start_event_id)
            FILTER (WHERE plate_appearance_result IS NOT NULL)::BIGINT
            AS completed_plate_appearances,
        (
            COUNT(DISTINCT appearance_start_event_id)
            - COUNT(DISTINCT appearance_start_event_id)
                FILTER (WHERE plate_appearance_result IS NOT NULL)
        )::BIGINT AS interrupted_appearances,
        SUM(completed_pitches)::BIGINT AS completed_pitches,
        SUM(completed_swings)::BIGINT AS completed_swings,
        SUM(completed_swings_with_contact)::BIGINT AS completed_swings_with_contact,
        SUM(completed_strikes)::BIGINT AS completed_strikes,
        SUM(completed_strikes_called)::BIGINT AS completed_strikes_called,
        SUM(completed_strikes_swinging)::BIGINT AS completed_strikes_swinging,
        SUM(completed_strikes_foul)::BIGINT AS completed_strikes_foul,
        SUM(completed_strikes_foul_tip)::BIGINT AS completed_strikes_foul_tip,
        SUM(completed_strikes_in_play)::BIGINT AS completed_strikes_in_play,
        SUM(completed_strikes_unknown)::BIGINT AS completed_strikes_unknown,
        SUM(completed_balls)::BIGINT AS completed_balls,
        SUM(completed_balls_called)::BIGINT AS completed_balls_called,
        SUM(completed_balls_intentional)::BIGINT AS completed_balls_intentional,
        SUM(completed_balls_automatic)::BIGINT AS completed_balls_automatic,
        SUM(completed_unknown_pitches)::BIGINT AS completed_unknown_pitches,
        SUM(completed_pitchouts)::BIGINT AS completed_pitchouts,
        SUM(completed_pitcher_pickoff_attempts)::BIGINT AS completed_pitcher_pickoff_attempts,
        SUM(completed_catcher_pickoff_attempts)::BIGINT AS completed_catcher_pickoff_attempts,
        SUM(completed_pitches_blocked_by_catcher)::BIGINT AS completed_pitches_blocked_by_catcher,
        SUM(completed_pitches_with_runners_going)::BIGINT AS completed_pitches_with_runners_going,
        SUM(completed_passed_balls)::BIGINT AS completed_passed_balls,
        SUM(completed_wild_pitches)::BIGINT AS completed_wild_pitches,
        SUM(completed_balks)::BIGINT AS completed_balks,
        SUM(CASE WHEN (pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches ELSE 0 END)::BIGINT AS observed_pitches,
        SUM(CASE WHEN (swings_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_swings ELSE 0 END)::BIGINT AS observed_swings,
        SUM(CASE WHEN (swings_with_contact_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_swings_with_contact ELSE 0 END)::BIGINT AS observed_swings_with_contact,
        SUM(CASE WHEN (strikes_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes ELSE 0 END)::BIGINT AS observed_strikes,
        SUM(CASE WHEN (strikes_called_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_called ELSE 0 END)::BIGINT AS observed_strikes_called,
        SUM(CASE WHEN (strikes_swinging_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_swinging ELSE 0 END)::BIGINT AS observed_strikes_swinging,
        SUM(CASE WHEN (strikes_foul_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_foul ELSE 0 END)::BIGINT AS observed_strikes_foul,
        SUM(CASE WHEN (strikes_foul_tip_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_foul_tip ELSE 0 END)::BIGINT AS observed_strikes_foul_tip,
        SUM(CASE WHEN (strikes_in_play_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_in_play ELSE 0 END)::BIGINT AS observed_strikes_in_play,
        SUM(CASE WHEN (strikes_unknown_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_unknown ELSE 0 END)::BIGINT AS observed_strikes_unknown,
        SUM(CASE WHEN (balls_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls ELSE 0 END)::BIGINT AS observed_balls,
        SUM(CASE WHEN (balls_called_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_called ELSE 0 END)::BIGINT AS observed_balls_called,
        SUM(CASE WHEN (balls_intentional_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_intentional ELSE 0 END)::BIGINT AS observed_balls_intentional,
        SUM(CASE WHEN (balls_automatic_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_automatic ELSE 0 END)::BIGINT AS observed_balls_automatic,
        SUM(CASE WHEN (unknown_pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_unknown_pitches ELSE 0 END)::BIGINT AS observed_unknown_pitches,
        SUM(CASE WHEN (pitchouts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitchouts ELSE 0 END)::BIGINT AS observed_pitchouts,
        SUM(CASE WHEN (pitcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitcher_pickoff_attempts ELSE 0 END)::BIGINT AS observed_pitcher_pickoff_attempts,
        SUM(CASE WHEN (catcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_catcher_pickoff_attempts ELSE 0 END)::BIGINT AS observed_catcher_pickoff_attempts,
        SUM(CASE WHEN (pitches_blocked_by_catcher_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches_blocked_by_catcher ELSE 0 END)::BIGINT AS observed_pitches_blocked_by_catcher,
        SUM(CASE WHEN (pitches_with_runners_going_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches_with_runners_going ELSE 0 END)::BIGINT AS observed_pitches_with_runners_going,
        SUM(CASE WHEN (passed_balls_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_passed_balls ELSE 0 END)::BIGINT AS observed_passed_balls,
        SUM(CASE WHEN (wild_pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_wild_pitches ELSE 0 END)::BIGINT AS observed_wild_pitches,
        SUM(CASE WHEN (balks_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balks ELSE 0 END)::BIGINT AS observed_balks,
        SUM(CASE WHEN NOT (pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches ELSE 0 END)::BIGINT AS estimated_pitches,
        SUM(CASE WHEN NOT (swings_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_swings ELSE 0 END)::BIGINT AS estimated_swings,
        SUM(CASE WHEN NOT (swings_with_contact_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_swings_with_contact ELSE 0 END)::BIGINT AS estimated_swings_with_contact,
        SUM(CASE WHEN NOT (strikes_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes ELSE 0 END)::BIGINT AS estimated_strikes,
        SUM(CASE WHEN NOT (strikes_called_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_called ELSE 0 END)::BIGINT AS estimated_strikes_called,
        SUM(CASE WHEN NOT (strikes_swinging_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_swinging ELSE 0 END)::BIGINT AS estimated_strikes_swinging,
        SUM(CASE WHEN NOT (strikes_foul_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_foul ELSE 0 END)::BIGINT AS estimated_strikes_foul,
        SUM(CASE WHEN NOT (strikes_foul_tip_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_foul_tip ELSE 0 END)::BIGINT AS estimated_strikes_foul_tip,
        SUM(CASE WHEN NOT (strikes_in_play_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_in_play ELSE 0 END)::BIGINT AS estimated_strikes_in_play,
        SUM(CASE WHEN NOT (strikes_unknown_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_strikes_unknown ELSE 0 END)::BIGINT AS estimated_strikes_unknown,
        SUM(CASE WHEN NOT (balls_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls ELSE 0 END)::BIGINT AS estimated_balls,
        SUM(CASE WHEN NOT (balls_called_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_called ELSE 0 END)::BIGINT AS estimated_balls_called,
        SUM(CASE WHEN NOT (balls_intentional_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_intentional ELSE 0 END)::BIGINT AS estimated_balls_intentional,
        SUM(CASE WHEN NOT (balls_automatic_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balls_automatic ELSE 0 END)::BIGINT AS estimated_balls_automatic,
        SUM(CASE WHEN NOT (unknown_pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_unknown_pitches ELSE 0 END)::BIGINT AS estimated_unknown_pitches,
        SUM(CASE WHEN NOT (pitchouts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitchouts ELSE 0 END)::BIGINT AS estimated_pitchouts,
        SUM(CASE WHEN NOT (pitcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitcher_pickoff_attempts ELSE 0 END)::BIGINT AS estimated_pitcher_pickoff_attempts,
        SUM(CASE WHEN NOT (catcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_catcher_pickoff_attempts ELSE 0 END)::BIGINT AS estimated_catcher_pickoff_attempts,
        SUM(CASE WHEN NOT (pitches_blocked_by_catcher_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches_blocked_by_catcher ELSE 0 END)::BIGINT AS estimated_pitches_blocked_by_catcher,
        SUM(CASE WHEN NOT (pitches_with_runners_going_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_pitches_with_runners_going ELSE 0 END)::BIGINT AS estimated_pitches_with_runners_going,
        SUM(CASE WHEN NOT (passed_balls_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_passed_balls ELSE 0 END)::BIGINT AS estimated_passed_balls,
        SUM(CASE WHEN NOT (wild_pitches_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_wild_pitches ELSE 0 END)::BIGINT AS estimated_wild_pitches,
        SUM(CASE WHEN NOT (balks_method IN ('observed_incremental', 'observed_baserunning_event')) THEN completed_balks ELSE 0 END)::BIGINT AS estimated_balks,
        BOOL_AND(
            (pitches_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (swings_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (swings_with_contact_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_called_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_swinging_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_foul_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_foul_tip_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_in_play_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (strikes_unknown_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (balls_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (balls_called_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (balls_intentional_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (balls_automatic_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (unknown_pitches_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (pitchouts_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (pitcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (catcher_pickoff_attempts_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (pitches_blocked_by_catcher_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (pitches_with_runners_going_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (passed_balls_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (wild_pitches_method IN ('observed_incremental', 'observed_baserunning_event'))
            AND (balks_method IN ('observed_incremental', 'observed_baserunning_event'))
        ) AS all_counters_observed,
        MIN(artifact_id) AS artifact_id,
        MIN(source_snapshot_id) AS source_snapshot_id,
        MIN(confidence_status) AS confidence_status,
        BOOL_OR(weak_identification_flag) AS weak_identification_flag
    FROM event_rows
    GROUP BY ALL
)
SELECT
    rolled.*,
    completed_pitches::DOUBLE / NULLIF(completed_plate_appearances, 0)
        AS pitches_per_completed_plate_appearance,
    completed_strikes::DOUBLE / NULLIF(completed_pitches, 0) AS strike_rate,
    completed_swings::DOUBLE / NULLIF(completed_pitches, 0) AS swing_rate,
    completed_swings_with_contact::DOUBLE / NULLIF(completed_swings, 0) AS contact_rate,
    'pbp_completed_pitch_totals' AS model_name,
    '1' AS model_version,
    'completed_counter_rollup' AS method,
    CASE WHEN all_counters_observed THEN 'observed' ELSE 'mixed' END AS observed_status
FROM rolled
