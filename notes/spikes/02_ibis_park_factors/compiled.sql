WITH "t8" AS (
  SELECT
    "t7"."park_id",
    "t7"."season",
    "t7"."league"
  FROM (
    SELECT
      "t5"."park_id",
      "t5"."season",
      "t5"."home_league" AS "league",
      COUNT(*) AS "games"
    FROM (
      SELECT
        *
      FROM "main_models"."game_start_info" AS "t2"
      WHERE
        "t2"."game_type" = 'RegularSeason'
    ) AS "t5"
    GROUP BY
      1,
      2,
      3
  ) AS "t7"
  WHERE
    "t7"."games" > 25
), "t14" AS (
  SELECT
    "t13"."park_id",
    "t13"."season",
    "t13"."league",
    "t13"."batter_id",
    "t13"."pitcher_id",
    CAST(SUM("t13"."plate_appearances") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "plate_appearances",
    CAST(SUM("t13"."singles") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "singles",
    CAST(SUM("t13"."doubles") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "doubles",
    CAST(SUM("t13"."triples") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "triples",
    CAST(SUM("t13"."home_runs") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "home_runs",
    CAST(SUM("t13"."strikeouts") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "strikeouts",
    CAST(SUM("t13"."walks") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "walks",
    CAST(SUM("t13"."batting_outs") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batting_outs",
    CAST(SUM("t13"."runs") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "runs",
    CAST(SUM("t13"."balls_in_play") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "balls_in_play",
    CAST(SUM("t13"."trajectory_fly_ball") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "trajectory_fly_ball",
    CAST(SUM("t13"."trajectory_ground_ball") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "trajectory_ground_ball",
    CAST(SUM("t13"."trajectory_line_drive") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "trajectory_line_drive",
    CAST(SUM("t13"."trajectory_pop_up") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "trajectory_pop_up",
    CAST(SUM("t13"."trajectory_unknown") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "trajectory_unknown",
    CAST(SUM("t13"."batted_distance_infield") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_distance_infield",
    CAST(SUM("t13"."batted_distance_outfield") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_distance_outfield",
    CAST(SUM("t13"."batted_distance_unknown") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_distance_unknown",
    CAST(SUM("t13"."batted_angle_left") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_angle_left",
    CAST(SUM("t13"."batted_angle_right") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_angle_right",
    CAST(SUM("t13"."batted_angle_middle") OVER (
      PARTITION BY "t13"."park_id", "t13"."batter_id", "t13"."pitcher_id", "t13"."league"
      ORDER BY "t13"."season" ASC
      ROWS BETWEEN 2 preceding AND CURRENT ROW
    ) AS INT) AS "batted_angle_middle"
  FROM (
    SELECT
      "t12"."park_id",
      "t12"."season",
      "t12"."league",
      "t12"."batter_id",
      "t12"."pitcher_id",
      CAST(SUM("t12"."plate_appearances") AS INT) AS "plate_appearances",
      CAST(SUM("t12"."singles") AS INT) AS "singles",
      CAST(SUM("t12"."doubles") AS INT) AS "doubles",
      CAST(SUM("t12"."triples") AS INT) AS "triples",
      CAST(SUM("t12"."home_runs") AS INT) AS "home_runs",
      CAST(SUM("t12"."strikeouts") AS INT) AS "strikeouts",
      CAST(SUM("t12"."walks") AS INT) AS "walks",
      CAST(SUM("t12"."batting_outs") AS INT) AS "batting_outs",
      CAST(SUM("t12"."runs") AS INT) AS "runs",
      CAST(SUM("t12"."balls_in_play") AS INT) AS "balls_in_play",
      CAST(SUM("t12"."trajectory_fly_ball") AS INT) AS "trajectory_fly_ball",
      CAST(SUM("t12"."trajectory_ground_ball") AS INT) AS "trajectory_ground_ball",
      CAST(SUM("t12"."trajectory_line_drive") AS INT) AS "trajectory_line_drive",
      CAST(SUM("t12"."trajectory_pop_up") AS INT) AS "trajectory_pop_up",
      CAST(SUM("t12"."trajectory_unknown") AS INT) AS "trajectory_unknown",
      CAST(SUM("t12"."batted_distance_infield") AS INT) AS "batted_distance_infield",
      CAST(SUM("t12"."batted_distance_outfield") AS INT) AS "batted_distance_outfield",
      CAST(SUM("t12"."batted_distance_unknown") AS INT) AS "batted_distance_unknown",
      CAST(SUM("t12"."batted_angle_left") AS INT) AS "batted_angle_left",
      CAST(SUM("t12"."batted_angle_right") AS INT) AS "batted_angle_right",
      CAST(SUM("t12"."batted_angle_middle") AS INT) AS "batted_angle_middle"
    FROM (
      SELECT
        "t6"."game_id",
        "t6"."event_id",
        "t6"."event_key",
        "t6"."season",
        "t6"."league",
        "t6"."is_interleague",
        "t6"."game_type",
        "t6"."date",
        "t6"."park_id",
        "t6"."bat_first_side",
        "t6"."time_of_day",
        "t6"."pitching_team_starting_pitcher_id",
        "t6"."inning_start",
        "t6"."frame_start",
        "t6"."outs_start",
        "t6"."inning_in_outs_start",
        "t6"."is_gidp_eligible",
        "t6"."batting_side",
        "t6"."fielding_side",
        "t6"."score_home_start",
        "t6"."score_away_start",
        "t6"."home_margin_start",
        "t6"."batting_team_margin_start",
        "t6"."batter_lineup_position",
        "t6"."batter_fielding_position",
        "t6"."batter_hand",
        "t6"."pitcher_hand",
        "t6"."away_team_id",
        "t6"."home_team_id",
        "t6"."batting_team_id",
        "t6"."fielding_team_id",
        "t6"."batter_id",
        "t6"."pitcher_id",
        "t6"."base_state_start",
        "t6"."runners_count_start",
        "t6"."frame_start_flag",
        "t6"."runner_first_id_start",
        "t6"."runner_second_id_start",
        "t6"."runner_third_id_start",
        "t6"."count_balls",
        "t6"."count_strikes",
        "t6"."inning_end",
        "t6"."frame_end",
        "t6"."outs_on_play",
        "t6"."outs_end",
        "t6"."base_state_end",
        "t6"."runs_on_play",
        "t6"."score_home_end",
        "t6"."score_away_end",
        "t6"."home_margin_end",
        "t6"."batting_team_margin_end",
        "t6"."frame_end_flag",
        "t6"."truncated_frame_flag",
        "t6"."game_end_flag",
        "t6"."league_group",
        "t6"."season_group",
        "t6"."inning_group_start",
        "t6"."inning_group_end",
        "t6"."truncated_home_margin_start",
        "t6"."truncated_home_margin_end",
        "t6"."run_expectancy_start_key",
        "t6"."run_expectancy_end_key",
        "t6"."win_expectancy_start_key",
        "t6"."win_expectancy_end_key",
        "t3"."game_id" AS "game_id_right",
        "t3"."team_id",
        "t3"."player_id",
        "t3"."baserunner",
        "t3"."plate_appearances",
        "t3"."at_bats",
        "t3"."hits",
        "t3"."singles",
        "t3"."doubles",
        "t3"."triples",
        "t3"."home_runs",
        "t3"."total_bases",
        "t3"."strikeouts",
        "t3"."walks",
        "t3"."intentional_walks",
        "t3"."hit_by_pitches",
        "t3"."sacrifice_hits",
        "t3"."sacrifice_flies",
        "t3"."reached_on_errors",
        "t3"."reached_on_interferences",
        "t3"."inside_the_park_home_runs",
        "t3"."ground_rule_doubles",
        "t3"."infield_hits",
        "t3"."on_base_opportunities",
        "t3"."on_base_successes",
        "t3"."runs_batted_in",
        "t3"."grounded_into_double_plays",
        "t3"."double_plays",
        "t3"."triple_plays",
        "t3"."batting_outs",
        "t3"."balls_in_play",
        "t3"."balls_batted",
        "t3"."trajectory_fly_ball",
        "t3"."trajectory_ground_ball",
        "t3"."trajectory_line_drive",
        "t3"."trajectory_pop_up",
        "t3"."trajectory_unknown",
        "t3"."trajectory_known",
        "t3"."trajectory_broad_air_ball",
        "t3"."trajectory_broad_ground_ball",
        "t3"."trajectory_broad_unknown",
        "t3"."trajectory_broad_known",
        "t3"."bunts",
        "t3"."batted_distance_plate",
        "t3"."batted_distance_infield",
        "t3"."batted_distance_outfield",
        "t3"."batted_distance_unknown",
        "t3"."batted_distance_known",
        "t3"."fielded_by_battery",
        "t3"."fielded_by_infielder",
        "t3"."fielded_by_outfielder",
        "t3"."fielded_by_known",
        "t3"."fielded_by_unknown",
        "t3"."batted_angle_left",
        "t3"."batted_angle_right",
        "t3"."batted_angle_middle",
        "t3"."batted_angle_unknown",
        "t3"."batted_angle_known",
        "t3"."batted_location_plate",
        "t3"."batted_location_right_infield",
        "t3"."batted_location_middle_infield",
        "t3"."batted_location_left_infield",
        "t3"."batted_location_left_field",
        "t3"."batted_location_center_field",
        "t3"."batted_location_right_field",
        "t3"."batted_location_unknown",
        "t3"."batted_location_known",
        "t3"."batted_balls_pulled",
        "t3"."batted_balls_opposite_field",
        "t3"."runs",
        "t3"."times_reached_base",
        "t3"."stolen_bases",
        "t3"."caught_stealing",
        "t3"."picked_off",
        "t3"."picked_off_caught_stealing",
        "t3"."outs_on_basepaths",
        "t3"."unforced_outs_on_basepaths",
        "t3"."outs_avoided_on_errors",
        "t3"."advances_on_wild_pitches",
        "t3"."advances_on_passed_balls",
        "t3"."advances_on_balks",
        "t3"."advances_on_unspecified_plays",
        "t3"."advances_on_defensive_indifference",
        "t3"."advances_on_errors",
        "t3"."plate_appearances_while_on_base",
        "t3"."balls_in_play_while_running",
        "t3"."balls_in_play_while_on_base",
        "t3"."batter_total_bases_while_running",
        "t3"."batter_total_bases_while_on_base",
        "t3"."extra_base_advance_attempts",
        "t3"."bases_advanced",
        "t3"."bases_advanced_on_balls_in_play",
        "t3"."surplus_bases_advanced_on_balls_in_play",
        "t3"."outs_on_extra_base_advance_attempts",
        "t3"."pitches",
        "t3"."swings",
        "t3"."swings_with_contact",
        "t3"."strikes",
        "t3"."strikes_called",
        "t3"."strikes_swinging",
        "t3"."strikes_foul",
        "t3"."strikes_foul_tip",
        "t3"."strikes_in_play",
        "t3"."strikes_unknown",
        "t3"."balls",
        "t3"."balls_called",
        "t3"."balls_intentional",
        "t3"."balls_automatic",
        "t3"."unknown_pitches",
        "t3"."pitchouts",
        "t3"."pitcher_pickoff_attempts",
        "t3"."catcher_pickoff_attempts",
        "t3"."pitches_blocked_by_catcher",
        "t3"."pitches_with_runners_going",
        "t3"."passed_balls",
        "t3"."wild_pitches",
        "t3"."balks",
        "t3"."left_on_base",
        "t3"."left_on_base_with_two_outs",
        "t3"."stolen_bases_second",
        "t3"."stolen_bases_third",
        "t3"."stolen_bases_home",
        "t3"."caught_stealing_second",
        "t3"."caught_stealing_third",
        "t3"."caught_stealing_home",
        "t3"."stolen_base_opportunities",
        "t3"."stolen_base_opportunities_second",
        "t3"."stolen_base_opportunities_third",
        "t3"."stolen_base_opportunities_home",
        "t3"."picked_off_first",
        "t3"."picked_off_second",
        "t3"."picked_off_third",
        "t3"."times_force_on_runner",
        "t3"."times_lead_runner",
        "t3"."times_next_base_empty",
        "t3"."extra_base_chances",
        "t3"."extra_bases_taken"
      FROM (
        SELECT
          *
        FROM "main_models"."event_states_full" AS "t0"
        WHERE
          "t0"."game_type" = 'RegularSeason' AND NOT (
            "t0"."is_interleague"
          )
      ) AS "t6"
      INNER JOIN "main_models"."event_offense_stats" AS "t3"
        ON "t6"."event_key" = "t3"."event_key"
      INNER JOIN "t8" AS "t11"
        ON "t6"."park_id" = "t11"."park_id"
        AND "t6"."season" = "t11"."season"
        AND "t6"."league" = "t11"."league"
    ) AS "t12"
    GROUP BY
      1,
      2,
      3,
      4,
      5
  ) AS "t13"
), "t19" AS (
  SELECT
    *
  FROM "t14" AS "t15"
  UNION ALL
  SELECT
    *
  FROM (
    SELECT
      "t10"."park_id",
      "t17"."season",
      "t17"."league",
      'MARK' AS "batter_id",
      'PRIOR' AS "pitcher_id",
      CAST(1000 AS INT) AS "plate_appearances",
      CAST("t17"."avg_singles_per_pa" * 1000 AS INT) AS "singles",
      CAST("t17"."avg_doubles_per_pa" * 1000 AS INT) AS "doubles",
      CAST("t17"."avg_triples_per_pa" * 1000 AS INT) AS "triples",
      CAST("t17"."avg_home_runs_per_pa" * 1000 AS INT) AS "home_runs",
      CAST("t17"."avg_strikeouts_per_pa" * 1000 AS INT) AS "strikeouts",
      CAST("t17"."avg_walks_per_pa" * 1000 AS INT) AS "walks",
      CAST("t17"."avg_batting_outs_per_pa" * 1000 AS INT) AS "batting_outs",
      CAST("t17"."avg_runs_per_pa" * 1000 AS INT) AS "runs",
      CAST("t17"."avg_balls_in_play_per_pa" * 1000 AS INT) AS "balls_in_play",
      CAST("t17"."avg_trajectory_fly_ball_per_pa" * 1000 AS INT) AS "trajectory_fly_ball",
      CAST("t17"."avg_trajectory_ground_ball_per_pa" * 1000 AS INT) AS "trajectory_ground_ball",
      CAST("t17"."avg_trajectory_line_drive_per_pa" * 1000 AS INT) AS "trajectory_line_drive",
      CAST("t17"."avg_trajectory_pop_up_per_pa" * 1000 AS INT) AS "trajectory_pop_up",
      CAST("t17"."avg_trajectory_unknown_per_pa" * 1000 AS INT) AS "trajectory_unknown",
      CAST("t17"."avg_batted_distance_infield_per_pa" * 1000 AS INT) AS "batted_distance_infield",
      CAST("t17"."avg_batted_distance_outfield_per_pa" * 1000 AS INT) AS "batted_distance_outfield",
      CAST("t17"."avg_batted_distance_unknown_per_pa" * 1000 AS INT) AS "batted_distance_unknown",
      CAST("t17"."avg_batted_angle_left_per_pa" * 1000 AS INT) AS "batted_angle_left",
      CAST("t17"."avg_batted_angle_right_per_pa" * 1000 AS INT) AS "batted_angle_right",
      CAST("t17"."avg_batted_angle_middle_per_pa" * 1000 AS INT) AS "batted_angle_middle"
    FROM (
      SELECT
        "t15"."season",
        "t15"."league",
        SUM("t15"."singles") / SUM("t15"."plate_appearances") AS "avg_singles_per_pa",
        SUM("t15"."doubles") / SUM("t15"."plate_appearances") AS "avg_doubles_per_pa",
        SUM("t15"."triples") / SUM("t15"."plate_appearances") AS "avg_triples_per_pa",
        SUM("t15"."home_runs") / SUM("t15"."plate_appearances") AS "avg_home_runs_per_pa",
        SUM("t15"."strikeouts") / SUM("t15"."plate_appearances") AS "avg_strikeouts_per_pa",
        SUM("t15"."walks") / SUM("t15"."plate_appearances") AS "avg_walks_per_pa",
        SUM("t15"."batting_outs") / SUM("t15"."plate_appearances") AS "avg_batting_outs_per_pa",
        SUM("t15"."runs") / SUM("t15"."plate_appearances") AS "avg_runs_per_pa",
        SUM("t15"."balls_in_play") / SUM("t15"."plate_appearances") AS "avg_balls_in_play_per_pa",
        SUM("t15"."trajectory_fly_ball") / SUM("t15"."plate_appearances") AS "avg_trajectory_fly_ball_per_pa",
        SUM("t15"."trajectory_ground_ball") / SUM("t15"."plate_appearances") AS "avg_trajectory_ground_ball_per_pa",
        SUM("t15"."trajectory_line_drive") / SUM("t15"."plate_appearances") AS "avg_trajectory_line_drive_per_pa",
        SUM("t15"."trajectory_pop_up") / SUM("t15"."plate_appearances") AS "avg_trajectory_pop_up_per_pa",
        SUM("t15"."trajectory_unknown") / SUM("t15"."plate_appearances") AS "avg_trajectory_unknown_per_pa",
        SUM("t15"."batted_distance_infield") / SUM("t15"."plate_appearances") AS "avg_batted_distance_infield_per_pa",
        SUM("t15"."batted_distance_outfield") / SUM("t15"."plate_appearances") AS "avg_batted_distance_outfield_per_pa",
        SUM("t15"."batted_distance_unknown") / SUM("t15"."plate_appearances") AS "avg_batted_distance_unknown_per_pa",
        SUM("t15"."batted_angle_left") / SUM("t15"."plate_appearances") AS "avg_batted_angle_left_per_pa",
        SUM("t15"."batted_angle_right") / SUM("t15"."plate_appearances") AS "avg_batted_angle_right_per_pa",
        SUM("t15"."batted_angle_middle") / SUM("t15"."plate_appearances") AS "avg_batted_angle_middle_per_pa"
      FROM "t14" AS "t15"
      GROUP BY
        1,
        2
    ) AS "t17"
    INNER JOIN "t8" AS "t10"
      ON "t17"."season" = "t10"."season" AND "t17"."league" = "t10"."league"
  ) AS "t18"
)
SELECT
  "t30"."park_id",
  "t30"."season",
  "t30"."league",
  CAST(ROUND("t30"."sqrt_sample_size", 0) AS BIGINT) AS "sqrt_sample_size",
  CAST(ROUND(
    (
      "t30"."avg_this_singles_per_pa" / (
        1 - "t30"."avg_this_singles_per_pa"
      )
    ) / (
      "t30"."avg_other_singles_per_pa" / (
        1 - "t30"."avg_other_singles_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "singles_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_doubles_per_pa" / (
        1 - "t30"."avg_this_doubles_per_pa"
      )
    ) / (
      "t30"."avg_other_doubles_per_pa" / (
        1 - "t30"."avg_other_doubles_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "doubles_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_triples_per_pa" / (
        1 - "t30"."avg_this_triples_per_pa"
      )
    ) / (
      "t30"."avg_other_triples_per_pa" / (
        1 - "t30"."avg_other_triples_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "triples_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_home_runs_per_pa" / (
        1 - "t30"."avg_this_home_runs_per_pa"
      )
    ) / (
      "t30"."avg_other_home_runs_per_pa" / (
        1 - "t30"."avg_other_home_runs_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "home_runs_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_strikeouts_per_pa" / (
        1 - "t30"."avg_this_strikeouts_per_pa"
      )
    ) / (
      "t30"."avg_other_strikeouts_per_pa" / (
        1 - "t30"."avg_other_strikeouts_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "strikeouts_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_walks_per_pa" / (
        1 - "t30"."avg_this_walks_per_pa"
      )
    ) / (
      "t30"."avg_other_walks_per_pa" / (
        1 - "t30"."avg_other_walks_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "walks_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batting_outs_per_pa" / (
        1 - "t30"."avg_this_batting_outs_per_pa"
      )
    ) / (
      "t30"."avg_other_batting_outs_per_pa" / (
        1 - "t30"."avg_other_batting_outs_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batting_outs_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_runs_per_pa" / (
        1 - "t30"."avg_this_runs_per_pa"
      )
    ) / (
      "t30"."avg_other_runs_per_pa" / (
        1 - "t30"."avg_other_runs_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "runs_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_balls_in_play_per_pa" / (
        1 - "t30"."avg_this_balls_in_play_per_pa"
      )
    ) / (
      "t30"."avg_other_balls_in_play_per_pa" / (
        1 - "t30"."avg_other_balls_in_play_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "balls_in_play_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_trajectory_fly_ball_per_pa" / (
        1 - "t30"."avg_this_trajectory_fly_ball_per_pa"
      )
    ) / (
      "t30"."avg_other_trajectory_fly_ball_per_pa" / (
        1 - "t30"."avg_other_trajectory_fly_ball_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "trajectory_fly_ball_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_trajectory_ground_ball_per_pa" / (
        1 - "t30"."avg_this_trajectory_ground_ball_per_pa"
      )
    ) / (
      "t30"."avg_other_trajectory_ground_ball_per_pa" / (
        1 - "t30"."avg_other_trajectory_ground_ball_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "trajectory_ground_ball_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_trajectory_line_drive_per_pa" / (
        1 - "t30"."avg_this_trajectory_line_drive_per_pa"
      )
    ) / (
      "t30"."avg_other_trajectory_line_drive_per_pa" / (
        1 - "t30"."avg_other_trajectory_line_drive_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "trajectory_line_drive_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_trajectory_pop_up_per_pa" / (
        1 - "t30"."avg_this_trajectory_pop_up_per_pa"
      )
    ) / (
      "t30"."avg_other_trajectory_pop_up_per_pa" / (
        1 - "t30"."avg_other_trajectory_pop_up_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "trajectory_pop_up_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_trajectory_unknown_per_pa" / (
        1 - "t30"."avg_this_trajectory_unknown_per_pa"
      )
    ) / (
      "t30"."avg_other_trajectory_unknown_per_pa" / (
        1 - "t30"."avg_other_trajectory_unknown_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "trajectory_unknown_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_distance_infield_per_pa" / (
        1 - "t30"."avg_this_batted_distance_infield_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_distance_infield_per_pa" / (
        1 - "t30"."avg_other_batted_distance_infield_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_distance_infield_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_distance_outfield_per_pa" / (
        1 - "t30"."avg_this_batted_distance_outfield_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_distance_outfield_per_pa" / (
        1 - "t30"."avg_other_batted_distance_outfield_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_distance_outfield_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_distance_unknown_per_pa" / (
        1 - "t30"."avg_this_batted_distance_unknown_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_distance_unknown_per_pa" / (
        1 - "t30"."avg_other_batted_distance_unknown_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_distance_unknown_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_angle_left_per_pa" / (
        1 - "t30"."avg_this_batted_angle_left_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_angle_left_per_pa" / (
        1 - "t30"."avg_other_batted_angle_left_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_angle_left_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_angle_right_per_pa" / (
        1 - "t30"."avg_this_batted_angle_right_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_angle_right_per_pa" / (
        1 - "t30"."avg_other_batted_angle_right_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_angle_right_park_factor",
  CAST(ROUND(
    (
      "t30"."avg_this_batted_angle_middle_per_pa" / (
        1 - "t30"."avg_this_batted_angle_middle_per_pa"
      )
    ) / (
      "t30"."avg_other_batted_angle_middle_per_pa" / (
        1 - "t30"."avg_other_batted_angle_middle_per_pa"
      )
    ),
    2
  ) AS DOUBLE) AS "batted_angle_middle_park_factor"
FROM (
  SELECT
    "t29"."this_park_id" AS "park_id",
    "t29"."season",
    "t29"."league",
    SUM("t29"."sample_size") AS "sqrt_sample_size",
    SUM("t29"."this_singles_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_singles_per_pa",
    SUM("t29"."other_singles_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_singles_per_pa",
    SUM("t29"."this_doubles_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_doubles_per_pa",
    SUM("t29"."other_doubles_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_doubles_per_pa",
    SUM("t29"."this_triples_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_triples_per_pa",
    SUM("t29"."other_triples_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_triples_per_pa",
    SUM("t29"."this_home_runs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_home_runs_per_pa",
    SUM("t29"."other_home_runs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_home_runs_per_pa",
    SUM("t29"."this_strikeouts_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_strikeouts_per_pa",
    SUM("t29"."other_strikeouts_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_strikeouts_per_pa",
    SUM("t29"."this_walks_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_walks_per_pa",
    SUM("t29"."other_walks_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_walks_per_pa",
    SUM("t29"."this_batting_outs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batting_outs_per_pa",
    SUM("t29"."other_batting_outs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batting_outs_per_pa",
    SUM("t29"."this_runs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_runs_per_pa",
    SUM("t29"."other_runs_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_runs_per_pa",
    SUM("t29"."this_balls_in_play_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_balls_in_play_per_pa",
    SUM("t29"."other_balls_in_play_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_balls_in_play_per_pa",
    SUM("t29"."this_trajectory_fly_ball_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_trajectory_fly_ball_per_pa",
    SUM("t29"."other_trajectory_fly_ball_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_trajectory_fly_ball_per_pa",
    SUM("t29"."this_trajectory_ground_ball_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_trajectory_ground_ball_per_pa",
    SUM("t29"."other_trajectory_ground_ball_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_trajectory_ground_ball_per_pa",
    SUM("t29"."this_trajectory_line_drive_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_trajectory_line_drive_per_pa",
    SUM("t29"."other_trajectory_line_drive_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_trajectory_line_drive_per_pa",
    SUM("t29"."this_trajectory_pop_up_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_trajectory_pop_up_per_pa",
    SUM("t29"."other_trajectory_pop_up_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_trajectory_pop_up_per_pa",
    SUM("t29"."this_trajectory_unknown_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_trajectory_unknown_per_pa",
    SUM("t29"."other_trajectory_unknown_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_trajectory_unknown_per_pa",
    SUM("t29"."this_batted_distance_infield_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_distance_infield_per_pa",
    SUM("t29"."other_batted_distance_infield_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_distance_infield_per_pa",
    SUM("t29"."this_batted_distance_outfield_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_distance_outfield_per_pa",
    SUM("t29"."other_batted_distance_outfield_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_distance_outfield_per_pa",
    SUM("t29"."this_batted_distance_unknown_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_distance_unknown_per_pa",
    SUM("t29"."other_batted_distance_unknown_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_distance_unknown_per_pa",
    SUM("t29"."this_batted_angle_left_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_angle_left_per_pa",
    SUM("t29"."other_batted_angle_left_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_angle_left_per_pa",
    SUM("t29"."this_batted_angle_right_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_angle_right_per_pa",
    SUM("t29"."other_batted_angle_right_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_angle_right_per_pa",
    SUM("t29"."this_batted_angle_middle_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_this_batted_angle_middle_per_pa",
    SUM("t29"."other_batted_angle_middle_per_pa" * "t29"."sample_weight") / SUM("t29"."sample_weight") AS "avg_other_batted_angle_middle_per_pa"
  FROM (
    SELECT
      "t28"."this_park_id",
      "t28"."other_park_id",
      "t28"."season",
      "t28"."league",
      "t28"."batter_id",
      "t28"."pitcher_id",
      "t28"."this_plate_appearances",
      "t28"."other_plate_appearances",
      "t28"."this_singles",
      "t28"."other_singles",
      "t28"."this_doubles",
      "t28"."other_doubles",
      "t28"."this_triples",
      "t28"."other_triples",
      "t28"."this_home_runs",
      "t28"."other_home_runs",
      "t28"."this_strikeouts",
      "t28"."other_strikeouts",
      "t28"."this_walks",
      "t28"."other_walks",
      "t28"."this_batting_outs",
      "t28"."other_batting_outs",
      "t28"."this_runs",
      "t28"."other_runs",
      "t28"."this_balls_in_play",
      "t28"."other_balls_in_play",
      "t28"."this_trajectory_fly_ball",
      "t28"."other_trajectory_fly_ball",
      "t28"."this_trajectory_ground_ball",
      "t28"."other_trajectory_ground_ball",
      "t28"."this_trajectory_line_drive",
      "t28"."other_trajectory_line_drive",
      "t28"."this_trajectory_pop_up",
      "t28"."other_trajectory_pop_up",
      "t28"."this_trajectory_unknown",
      "t28"."other_trajectory_unknown",
      "t28"."this_batted_distance_infield",
      "t28"."other_batted_distance_infield",
      "t28"."this_batted_distance_outfield",
      "t28"."other_batted_distance_outfield",
      "t28"."this_batted_distance_unknown",
      "t28"."other_batted_distance_unknown",
      "t28"."this_batted_angle_left",
      "t28"."other_batted_angle_left",
      "t28"."this_batted_angle_right",
      "t28"."other_batted_angle_right",
      "t28"."this_batted_angle_middle",
      "t28"."other_batted_angle_middle",
      "t28"."sample_size",
      "t28"."sum_sample_size",
      "t28"."this_singles_per_pa",
      "t28"."other_singles_per_pa",
      "t28"."this_doubles_per_pa",
      "t28"."other_doubles_per_pa",
      "t28"."this_triples_per_pa",
      "t28"."other_triples_per_pa",
      "t28"."this_home_runs_per_pa",
      "t28"."other_home_runs_per_pa",
      "t28"."this_strikeouts_per_pa",
      "t28"."other_strikeouts_per_pa",
      "t28"."this_walks_per_pa",
      "t28"."other_walks_per_pa",
      "t28"."this_batting_outs_per_pa",
      "t28"."other_batting_outs_per_pa",
      "t28"."this_runs_per_pa",
      "t28"."other_runs_per_pa",
      "t28"."this_balls_in_play_per_pa",
      "t28"."other_balls_in_play_per_pa",
      "t28"."this_trajectory_fly_ball_per_pa",
      "t28"."other_trajectory_fly_ball_per_pa",
      "t28"."this_trajectory_ground_ball_per_pa",
      "t28"."other_trajectory_ground_ball_per_pa",
      "t28"."this_trajectory_line_drive_per_pa",
      "t28"."other_trajectory_line_drive_per_pa",
      "t28"."this_trajectory_pop_up_per_pa",
      "t28"."other_trajectory_pop_up_per_pa",
      "t28"."this_trajectory_unknown_per_pa",
      "t28"."other_trajectory_unknown_per_pa",
      "t28"."this_batted_distance_infield_per_pa",
      "t28"."other_batted_distance_infield_per_pa",
      "t28"."this_batted_distance_outfield_per_pa",
      "t28"."other_batted_distance_outfield_per_pa",
      "t28"."this_batted_distance_unknown_per_pa",
      "t28"."other_batted_distance_unknown_per_pa",
      "t28"."this_batted_angle_left_per_pa",
      "t28"."other_batted_angle_left_per_pa",
      "t28"."this_batted_angle_right_per_pa",
      "t28"."other_batted_angle_right_per_pa",
      "t28"."this_batted_angle_middle_per_pa",
      "t28"."other_batted_angle_middle_per_pa",
      "t28"."scaling_factor",
      "t28"."sample_size" * (
        "t28"."scaling_factor" / "t28"."sum_sample_size"
      ) AS "sample_weight"
    FROM (
      SELECT
        "t27"."this_park_id",
        "t27"."other_park_id",
        "t27"."season",
        "t27"."league",
        "t27"."batter_id",
        "t27"."pitcher_id",
        "t27"."this_plate_appearances",
        "t27"."other_plate_appearances",
        "t27"."this_singles",
        "t27"."other_singles",
        "t27"."this_doubles",
        "t27"."other_doubles",
        "t27"."this_triples",
        "t27"."other_triples",
        "t27"."this_home_runs",
        "t27"."other_home_runs",
        "t27"."this_strikeouts",
        "t27"."other_strikeouts",
        "t27"."this_walks",
        "t27"."other_walks",
        "t27"."this_batting_outs",
        "t27"."other_batting_outs",
        "t27"."this_runs",
        "t27"."other_runs",
        "t27"."this_balls_in_play",
        "t27"."other_balls_in_play",
        "t27"."this_trajectory_fly_ball",
        "t27"."other_trajectory_fly_ball",
        "t27"."this_trajectory_ground_ball",
        "t27"."other_trajectory_ground_ball",
        "t27"."this_trajectory_line_drive",
        "t27"."other_trajectory_line_drive",
        "t27"."this_trajectory_pop_up",
        "t27"."other_trajectory_pop_up",
        "t27"."this_trajectory_unknown",
        "t27"."other_trajectory_unknown",
        "t27"."this_batted_distance_infield",
        "t27"."other_batted_distance_infield",
        "t27"."this_batted_distance_outfield",
        "t27"."other_batted_distance_outfield",
        "t27"."this_batted_distance_unknown",
        "t27"."other_batted_distance_unknown",
        "t27"."this_batted_angle_left",
        "t27"."other_batted_angle_left",
        "t27"."this_batted_angle_right",
        "t27"."other_batted_angle_right",
        "t27"."this_batted_angle_middle",
        "t27"."other_batted_angle_middle",
        "t27"."sample_size",
        "t27"."sum_sample_size",
        "t27"."this_singles_per_pa",
        "t27"."other_singles_per_pa",
        "t27"."this_doubles_per_pa",
        "t27"."other_doubles_per_pa",
        "t27"."this_triples_per_pa",
        "t27"."other_triples_per_pa",
        "t27"."this_home_runs_per_pa",
        "t27"."other_home_runs_per_pa",
        "t27"."this_strikeouts_per_pa",
        "t27"."other_strikeouts_per_pa",
        "t27"."this_walks_per_pa",
        "t27"."other_walks_per_pa",
        "t27"."this_batting_outs_per_pa",
        "t27"."other_batting_outs_per_pa",
        "t27"."this_runs_per_pa",
        "t27"."other_runs_per_pa",
        "t27"."this_balls_in_play_per_pa",
        "t27"."other_balls_in_play_per_pa",
        "t27"."this_trajectory_fly_ball_per_pa",
        "t27"."other_trajectory_fly_ball_per_pa",
        "t27"."this_trajectory_ground_ball_per_pa",
        "t27"."other_trajectory_ground_ball_per_pa",
        "t27"."this_trajectory_line_drive_per_pa",
        "t27"."other_trajectory_line_drive_per_pa",
        "t27"."this_trajectory_pop_up_per_pa",
        "t27"."other_trajectory_pop_up_per_pa",
        "t27"."this_trajectory_unknown_per_pa",
        "t27"."other_trajectory_unknown_per_pa",
        "t27"."this_batted_distance_infield_per_pa",
        "t27"."other_batted_distance_infield_per_pa",
        "t27"."this_batted_distance_outfield_per_pa",
        "t27"."other_batted_distance_outfield_per_pa",
        "t27"."this_batted_distance_unknown_per_pa",
        "t27"."other_batted_distance_unknown_per_pa",
        "t27"."this_batted_angle_left_per_pa",
        "t27"."other_batted_angle_left_per_pa",
        "t27"."this_batted_angle_right_per_pa",
        "t27"."other_batted_angle_right_per_pa",
        "t27"."this_batted_angle_middle_per_pa",
        "t27"."other_batted_angle_middle_per_pa",
        MAX("t27"."sum_sample_size") OVER (
          PARTITION BY "t27"."this_park_id", "t27"."season", "t27"."league"
          ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS "scaling_factor"
      FROM (
        SELECT
          "t26"."this_park_id",
          "t26"."other_park_id",
          "t26"."season",
          "t26"."league",
          "t26"."batter_id",
          "t26"."pitcher_id",
          "t26"."this_plate_appearances",
          "t26"."other_plate_appearances",
          "t26"."this_singles",
          "t26"."other_singles",
          "t26"."this_doubles",
          "t26"."other_doubles",
          "t26"."this_triples",
          "t26"."other_triples",
          "t26"."this_home_runs",
          "t26"."other_home_runs",
          "t26"."this_strikeouts",
          "t26"."other_strikeouts",
          "t26"."this_walks",
          "t26"."other_walks",
          "t26"."this_batting_outs",
          "t26"."other_batting_outs",
          "t26"."this_runs",
          "t26"."other_runs",
          "t26"."this_balls_in_play",
          "t26"."other_balls_in_play",
          "t26"."this_trajectory_fly_ball",
          "t26"."other_trajectory_fly_ball",
          "t26"."this_trajectory_ground_ball",
          "t26"."other_trajectory_ground_ball",
          "t26"."this_trajectory_line_drive",
          "t26"."other_trajectory_line_drive",
          "t26"."this_trajectory_pop_up",
          "t26"."other_trajectory_pop_up",
          "t26"."this_trajectory_unknown",
          "t26"."other_trajectory_unknown",
          "t26"."this_batted_distance_infield",
          "t26"."other_batted_distance_infield",
          "t26"."this_batted_distance_outfield",
          "t26"."other_batted_distance_outfield",
          "t26"."this_batted_distance_unknown",
          "t26"."other_batted_distance_unknown",
          "t26"."this_batted_angle_left",
          "t26"."other_batted_angle_left",
          "t26"."this_batted_angle_right",
          "t26"."other_batted_angle_right",
          "t26"."this_batted_angle_middle",
          "t26"."other_batted_angle_middle",
          "t26"."sample_size",
          "t26"."sum_sample_size",
          "t26"."this_singles" / "t26"."this_plate_appearances" AS "this_singles_per_pa",
          "t26"."other_singles" / "t26"."other_plate_appearances" AS "other_singles_per_pa",
          "t26"."this_doubles" / "t26"."this_plate_appearances" AS "this_doubles_per_pa",
          "t26"."other_doubles" / "t26"."other_plate_appearances" AS "other_doubles_per_pa",
          "t26"."this_triples" / "t26"."this_plate_appearances" AS "this_triples_per_pa",
          "t26"."other_triples" / "t26"."other_plate_appearances" AS "other_triples_per_pa",
          "t26"."this_home_runs" / "t26"."this_plate_appearances" AS "this_home_runs_per_pa",
          "t26"."other_home_runs" / "t26"."other_plate_appearances" AS "other_home_runs_per_pa",
          "t26"."this_strikeouts" / "t26"."this_plate_appearances" AS "this_strikeouts_per_pa",
          "t26"."other_strikeouts" / "t26"."other_plate_appearances" AS "other_strikeouts_per_pa",
          "t26"."this_walks" / "t26"."this_plate_appearances" AS "this_walks_per_pa",
          "t26"."other_walks" / "t26"."other_plate_appearances" AS "other_walks_per_pa",
          "t26"."this_batting_outs" / "t26"."this_plate_appearances" AS "this_batting_outs_per_pa",
          "t26"."other_batting_outs" / "t26"."other_plate_appearances" AS "other_batting_outs_per_pa",
          "t26"."this_runs" / "t26"."this_plate_appearances" AS "this_runs_per_pa",
          "t26"."other_runs" / "t26"."other_plate_appearances" AS "other_runs_per_pa",
          "t26"."this_balls_in_play" / "t26"."this_plate_appearances" AS "this_balls_in_play_per_pa",
          "t26"."other_balls_in_play" / "t26"."other_plate_appearances" AS "other_balls_in_play_per_pa",
          "t26"."this_trajectory_fly_ball" / "t26"."this_plate_appearances" AS "this_trajectory_fly_ball_per_pa",
          "t26"."other_trajectory_fly_ball" / "t26"."other_plate_appearances" AS "other_trajectory_fly_ball_per_pa",
          "t26"."this_trajectory_ground_ball" / "t26"."this_plate_appearances" AS "this_trajectory_ground_ball_per_pa",
          "t26"."other_trajectory_ground_ball" / "t26"."other_plate_appearances" AS "other_trajectory_ground_ball_per_pa",
          "t26"."this_trajectory_line_drive" / "t26"."this_plate_appearances" AS "this_trajectory_line_drive_per_pa",
          "t26"."other_trajectory_line_drive" / "t26"."other_plate_appearances" AS "other_trajectory_line_drive_per_pa",
          "t26"."this_trajectory_pop_up" / "t26"."this_plate_appearances" AS "this_trajectory_pop_up_per_pa",
          "t26"."other_trajectory_pop_up" / "t26"."other_plate_appearances" AS "other_trajectory_pop_up_per_pa",
          "t26"."this_trajectory_unknown" / "t26"."this_plate_appearances" AS "this_trajectory_unknown_per_pa",
          "t26"."other_trajectory_unknown" / "t26"."other_plate_appearances" AS "other_trajectory_unknown_per_pa",
          "t26"."this_batted_distance_infield" / "t26"."this_plate_appearances" AS "this_batted_distance_infield_per_pa",
          "t26"."other_batted_distance_infield" / "t26"."other_plate_appearances" AS "other_batted_distance_infield_per_pa",
          "t26"."this_batted_distance_outfield" / "t26"."this_plate_appearances" AS "this_batted_distance_outfield_per_pa",
          "t26"."other_batted_distance_outfield" / "t26"."other_plate_appearances" AS "other_batted_distance_outfield_per_pa",
          "t26"."this_batted_distance_unknown" / "t26"."this_plate_appearances" AS "this_batted_distance_unknown_per_pa",
          "t26"."other_batted_distance_unknown" / "t26"."other_plate_appearances" AS "other_batted_distance_unknown_per_pa",
          "t26"."this_batted_angle_left" / "t26"."this_plate_appearances" AS "this_batted_angle_left_per_pa",
          "t26"."other_batted_angle_left" / "t26"."other_plate_appearances" AS "other_batted_angle_left_per_pa",
          "t26"."this_batted_angle_right" / "t26"."this_plate_appearances" AS "this_batted_angle_right_per_pa",
          "t26"."other_batted_angle_right" / "t26"."other_plate_appearances" AS "other_batted_angle_right_per_pa",
          "t26"."this_batted_angle_middle" / "t26"."this_plate_appearances" AS "this_batted_angle_middle_per_pa",
          "t26"."other_batted_angle_middle" / "t26"."other_plate_appearances" AS "other_batted_angle_middle_per_pa"
        FROM (
          SELECT
            "t25"."this_park_id",
            "t25"."other_park_id",
            "t25"."season",
            "t25"."league",
            "t25"."batter_id",
            "t25"."pitcher_id",
            "t25"."this_plate_appearances",
            "t25"."other_plate_appearances",
            "t25"."this_singles",
            "t25"."other_singles",
            "t25"."this_doubles",
            "t25"."other_doubles",
            "t25"."this_triples",
            "t25"."other_triples",
            "t25"."this_home_runs",
            "t25"."other_home_runs",
            "t25"."this_strikeouts",
            "t25"."other_strikeouts",
            "t25"."this_walks",
            "t25"."other_walks",
            "t25"."this_batting_outs",
            "t25"."other_batting_outs",
            "t25"."this_runs",
            "t25"."other_runs",
            "t25"."this_balls_in_play",
            "t25"."other_balls_in_play",
            "t25"."this_trajectory_fly_ball",
            "t25"."other_trajectory_fly_ball",
            "t25"."this_trajectory_ground_ball",
            "t25"."other_trajectory_ground_ball",
            "t25"."this_trajectory_line_drive",
            "t25"."other_trajectory_line_drive",
            "t25"."this_trajectory_pop_up",
            "t25"."other_trajectory_pop_up",
            "t25"."this_trajectory_unknown",
            "t25"."other_trajectory_unknown",
            "t25"."this_batted_distance_infield",
            "t25"."other_batted_distance_infield",
            "t25"."this_batted_distance_outfield",
            "t25"."other_batted_distance_outfield",
            "t25"."this_batted_distance_unknown",
            "t25"."other_batted_distance_unknown",
            "t25"."this_batted_angle_left",
            "t25"."other_batted_angle_left",
            "t25"."this_batted_angle_right",
            "t25"."other_batted_angle_right",
            "t25"."this_batted_angle_middle",
            "t25"."other_batted_angle_middle",
            "t25"."sample_size",
            SUM("t25"."sample_size") OVER (
              PARTITION BY "t25"."this_park_id", "t25"."other_park_id", "t25"."season", "t25"."league"
              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS "sum_sample_size"
          FROM (
            SELECT
              "t24"."this_park_id",
              "t24"."other_park_id",
              "t24"."season",
              "t24"."league",
              "t24"."batter_id",
              "t24"."pitcher_id",
              "t24"."this_plate_appearances",
              "t24"."other_plate_appearances",
              "t24"."this_singles",
              "t24"."other_singles",
              "t24"."this_doubles",
              "t24"."other_doubles",
              "t24"."this_triples",
              "t24"."other_triples",
              "t24"."this_home_runs",
              "t24"."other_home_runs",
              "t24"."this_strikeouts",
              "t24"."other_strikeouts",
              "t24"."this_walks",
              "t24"."other_walks",
              "t24"."this_batting_outs",
              "t24"."other_batting_outs",
              "t24"."this_runs",
              "t24"."other_runs",
              "t24"."this_balls_in_play",
              "t24"."other_balls_in_play",
              "t24"."this_trajectory_fly_ball",
              "t24"."other_trajectory_fly_ball",
              "t24"."this_trajectory_ground_ball",
              "t24"."other_trajectory_ground_ball",
              "t24"."this_trajectory_line_drive",
              "t24"."other_trajectory_line_drive",
              "t24"."this_trajectory_pop_up",
              "t24"."other_trajectory_pop_up",
              "t24"."this_trajectory_unknown",
              "t24"."other_trajectory_unknown",
              "t24"."this_batted_distance_infield",
              "t24"."other_batted_distance_infield",
              "t24"."this_batted_distance_outfield",
              "t24"."other_batted_distance_outfield",
              "t24"."this_batted_distance_unknown",
              "t24"."other_batted_distance_unknown",
              "t24"."this_batted_angle_left",
              "t24"."other_batted_angle_left",
              "t24"."this_batted_angle_right",
              "t24"."other_batted_angle_right",
              "t24"."this_batted_angle_middle",
              "t24"."other_batted_angle_middle",
              SQRT(LEAST("t24"."this_plate_appearances", "t24"."other_plate_appearances")) AS "sample_size"
            FROM (
              SELECT
                "t23"."park_id" AS "this_park_id",
                "t22"."park_id" AS "other_park_id",
                "t23"."season",
                "t23"."league",
                "t23"."batter_id",
                "t23"."pitcher_id",
                "t23"."plate_appearances" AS "this_plate_appearances",
                "t22"."plate_appearances" AS "other_plate_appearances",
                "t23"."singles" AS "this_singles",
                "t22"."singles" AS "other_singles",
                "t23"."doubles" AS "this_doubles",
                "t22"."doubles" AS "other_doubles",
                "t23"."triples" AS "this_triples",
                "t22"."triples" AS "other_triples",
                "t23"."home_runs" AS "this_home_runs",
                "t22"."home_runs" AS "other_home_runs",
                "t23"."strikeouts" AS "this_strikeouts",
                "t22"."strikeouts" AS "other_strikeouts",
                "t23"."walks" AS "this_walks",
                "t22"."walks" AS "other_walks",
                "t23"."batting_outs" AS "this_batting_outs",
                "t22"."batting_outs" AS "other_batting_outs",
                "t23"."runs" AS "this_runs",
                "t22"."runs" AS "other_runs",
                "t23"."balls_in_play" AS "this_balls_in_play",
                "t22"."balls_in_play" AS "other_balls_in_play",
                "t23"."trajectory_fly_ball" AS "this_trajectory_fly_ball",
                "t22"."trajectory_fly_ball" AS "other_trajectory_fly_ball",
                "t23"."trajectory_ground_ball" AS "this_trajectory_ground_ball",
                "t22"."trajectory_ground_ball" AS "other_trajectory_ground_ball",
                "t23"."trajectory_line_drive" AS "this_trajectory_line_drive",
                "t22"."trajectory_line_drive" AS "other_trajectory_line_drive",
                "t23"."trajectory_pop_up" AS "this_trajectory_pop_up",
                "t22"."trajectory_pop_up" AS "other_trajectory_pop_up",
                "t23"."trajectory_unknown" AS "this_trajectory_unknown",
                "t22"."trajectory_unknown" AS "other_trajectory_unknown",
                "t23"."batted_distance_infield" AS "this_batted_distance_infield",
                "t22"."batted_distance_infield" AS "other_batted_distance_infield",
                "t23"."batted_distance_outfield" AS "this_batted_distance_outfield",
                "t22"."batted_distance_outfield" AS "other_batted_distance_outfield",
                "t23"."batted_distance_unknown" AS "this_batted_distance_unknown",
                "t22"."batted_distance_unknown" AS "other_batted_distance_unknown",
                "t23"."batted_angle_left" AS "this_batted_angle_left",
                "t22"."batted_angle_left" AS "other_batted_angle_left",
                "t23"."batted_angle_right" AS "this_batted_angle_right",
                "t22"."batted_angle_right" AS "other_batted_angle_right",
                "t23"."batted_angle_middle" AS "this_batted_angle_middle",
                "t22"."batted_angle_middle" AS "other_batted_angle_middle"
              FROM "t19" AS "t23"
              INNER JOIN "t19" AS "t22"
                ON "t23"."park_id" <> "t22"."park_id"
                AND "t23"."season" = "t22"."season"
                AND "t23"."batter_id" = "t22"."batter_id"
                AND "t23"."pitcher_id" = "t22"."pitcher_id"
            ) AS "t24"
          ) AS "t25"
        ) AS "t26"
      ) AS "t27"
    ) AS "t28"
  ) AS "t29"
  GROUP BY
    1,
    2,
    3
) AS "t30"