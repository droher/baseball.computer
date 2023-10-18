WITH source AS (
    SELECT * FROM {{ source('event', 'events') }}
),

renamed AS (
    SELECT
        game_id,
        event_id,
        event_key,
        batting_side,
        inning,
        frame,
        batter_lineup_position,
        batter_id,
        pitcher_id,
        batting_team_id,
        fielding_team_id,
        outs,
        count_balls,
        count_strikes,
        base_state,
        LEFT(specified_batter_hand, 1)::HAND AS specified_batter_hand,
        LEFT(specified_pitcher_hand, 1)::HAND AS specified_pitcher_hand,
        strikeout_responsible_batter_id,
        walk_responsible_pitcher_id,
        plate_appearance_result,
        batted_contact_type,
        batted_to_fielder,
        batted_location_general,
        batted_location_depth,
        batted_location_angle,
        batted_location_strength,
        outs_on_play,
        runs_on_play,
        runs_batted_in,
        team_unearned_runs,
        STRPTIME(SUBSTRING(game_id, 4, 8), '%Y%m%d')::DATE AS date,
        SUBSTRING(game_id, 4, 4)::INT2 AS season,
        BIT_COUNT(base_state) AS runners_count,
    from source
)

SELECT * from renamed
