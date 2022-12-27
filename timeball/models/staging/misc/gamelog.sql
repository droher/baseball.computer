WITH source AS (
    SELECT * FROM {{ source('misc', 'gamelog') }}
),

renamed AS (
    SELECT
        date,
        double_header,
        day_of_week,
        visiting_team,
        visiting_team_league,
        visiting_team_game_number,
        home_team,
        home_team_league,
        home_team_game_number,
        visitor_runs_scored,
        home_runs_score,
        length_in_outs,
        day_night,
        completion_info,
        forfeit_info,
        protest_info,
        park_id,
        attendance,
        duration,
        vistor_line_score,
        home_line_score,
        visitor_ab,
        visitor_h,
        visitor_d,
        visitor_t,
        visitor_hr,
        visitor_rbi,
        visitor_sh,
        visitor_sf,
        visitor_hbp,
        visitor_bb,
        visitor_ibb,
        visitor_k,
        visitor_sb,
        visitor_cs,
        visitor_gdp,
        visitor_ci,
        visitor_lob,
        visitor_pitchers,
        visitor_er,
        visitor_ter,
        visitor_wp,
        visitor_balks,
        visitor_po,
        visitor_a,
        visitor_e,
        visitor_passed,
        visitor_db,
        visitor_tp,
        home_ab,
        home_h,
        home_d,
        home_t,
        home_hr,
        home_rbi,
        home_sh,
        home_sf,
        home_hbp,
        home_bb,
        home_ibb,
        home_k,
        home_sb,
        home_cs,
        home_gdp,
        home_ci,
        home_lob,
        home_pitchers,
        home_er,
        home_ter,
        home_wp,
        home_balks,
        home_po,
        home_a,
        home_e,
        home_passed,
        home_db,
        home_tp,
        umpire_h_id,
        umpire_h_name,
        umpire_1b_id,
        umpire_1b_name,
        umpire_2b_id,
        umpire_2b_name,
        umpire_3b_id,
        umpire_3b_name,
        umpire_lf_id,
        umpire_lf_name,
        umpire_rf_id,
        umpire_rf_name,
        visitor_manager_id,
        visitor_manager_name,
        home_manager_id,
        home_manager_name,
        winning_pitcher_id,
        winning_pitcher_name,
        losing_pitcher_id,
        losing_pitcher_name,
        saving_pitcher_id,
        saving_pitcher_name,
        game_winning_rbi_id,
        game_winning_rbi_name,
        visitor_starting_pitcher_id,
        visitor_starting_pitcher_name,
        home_starting_pitcher_id,
        home_starting_pitcher_name,
        visitor_batting_1_player_id,
        visitor_batting_1_name,
        visitor_batting_1_position,
        visitor_batting_2_player_id,
        visitor_batting_2_name,
        visitor_batting_2_position,
        visitor_batting_3_player_id,
        visitor_batting_3_name,
        visitor_batting_3_position,
        visitor_batting_4_player_id,
        visitor_batting_4_name,
        visitor_batting_4_position,
        visitor_batting_5_player_id,
        visitor_batting_5_name,
        visitor_batting_5_position,
        visitor_batting_6_player_id,
        visitor_batting_6_name,
        visitor_batting_6_position,
        visitor_batting_7_player_id,
        visitor_batting_7_name,
        visitor_batting_7_position,
        visitor_batting_8_player_id,
        visitor_batting_8_name,
        visitor_batting_8_position,
        visitor_batting_9_player_id,
        visitor_batting_9_name,
        visitor_batting_9_position,
        home_batting_1_player_id,
        home_batting_1_name,
        home_batting_1_position,
        home_batting_2_player_id,
        home_batting_2_name,
        home_batting_2_position,
        home_batting_3_player_id,
        home_batting_3_name,
        home_batting_3_position,
        home_batting_4_player_id,
        home_batting_4_name,
        home_batting_4_position,
        home_batting_5_player_id,
        home_batting_5_name,
        home_batting_5_position,
        home_batting_6_player_id,
        home_batting_6_name,
        home_batting_6_position,
        home_batting_7_player_id,
        home_batting_7_name,
        home_batting_7_position,
        home_batting_8_player_id,
        home_batting_8_name,
        home_batting_8_position,
        home_batting_9_player_id,
        home_batting_9_name,
        home_batting_9_position,
        additional_info,
        acquisition_info

    FROM source
)

SELECT * FROM renamed
