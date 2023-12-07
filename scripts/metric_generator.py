def offense_stats(is_event_level: bool) -> dict[str, str]:
    babip_group_level = """
        SUM(hits - home_runs) / (
            SUM(at_bats) - SUM(home_runs) - SUM(strikeouts) + SUM(COALESCE(sacrifice_flies, 0))
        )
    """
    babip_event_level = "SUM(hits * balls_in_play) / SUM(at_bats * balls_in_play)"

    event_level_only = {
        "air_ball_rate_outs": "SUM(trajectory_broad_air_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_ball_rate_outs": "SUM(trajectory_broad_ground_ball * (at_bats - hits)) / SUM(trajectory_broad_known * (at_bats - hits))",
        "ground_air_out_ratio": "air_ball_out_rate / ground_ball_out_rate",
        "air_ball_hit_rate": "SUM(trajectory_broad_air_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_ball_hit_rate": "SUM(trajectory_broad_ground_ball * hits) / SUM(trajectory_broad_known * hits)",
        "ground_air_hit_ratio": "air_ball_hit_rate / ground_ball_hit_rate",
        "fly_ball_rate": "SUM(trajectory_fly_ball) / SUM(trajectory_known)",
        "line_drive_rate": "SUM(trajectory_line_drive) / SUM(trajectory_known)",
        "pop_fly_rate": "SUM(trajectory_pop_fly) / SUM(trajectory_known)",
        "ground_ball_rate": "SUM(trajectory_ground_ball) / SUM(trajectory_known)",
        "ground_ball_batting_average": "SUM(trajectory_ground_ball * hits) / SUM(trajectory_ground_ball * at_bats)",
    }

    return {
        "games": "COUNT(DISTINCT game_id)" if is_event_level else "SUM(games)",
        "at_bats": "SUM(at_bats)",
        "plate_appearances": "SUM(plate_appearances)",
        "hits": "SUM(hits)",
        "doubles": "SUM(doubles)",
        "triples": "SUM(triples)",
        "home_runs": "SUM(home_runs)",
        "runs_batted_in": "SUM(runs_batted_in)",
        "runs": "SUM(runs)",
        "walks": "SUM(walks)",
        "intentional_walks": "SUM(intentional_walks)",
        "strikeouts": "SUM(strikeouts)",
        "hit_by_pitches": "SUM(hit_by_pitches)",
        "sacrifice_flies": "SUM(sacrifice_flies)",
        "sacrifice_hits": "SUM(sacrifice_hits)",
        "grounded_into_double_plays": "SUM(grounded_into_double_plays)",
        "stolen_bases": "SUM(stolen_bases)",
        "caught_stealing": "SUM(caught_stealing)",
        "total_bases": "SUM(total_bases)",
        # Rate stats
        "batting_average": "SUM(hits) / SUM(at_bats)",
        "on_base_percentage": "SUM(on_base_successes) / SUM(on_base_opportunities)",
        "slugging_percentage": "SUM(total_bases) / SUM(at_bats)",
        "on_base_plus_slugging": "SUM(on_base_successes) / SUM(on_base_opportunities) + SUM(total_bases) / SUM(at_bats)",
        "isolated_power": "SUM(total_bases) / SUM(at_bats) - SUM(hits) / SUM(at_bats)",
        "secondary_average": "SUM(total_bases - hits + walks + stolen_bases - caught_stealing) / SUM(at_bats)",
        "batting_average_on_balls_in_play": babip_event_level if is_event_level else babip_group_level,
        "home_run_rate": "SUM(home_runs) / SUM(plate_appearances)",
        "walk_rate": "SUM(walks) / SUM(plate_appearances)",
        "strikeout_rate": "SUM(strikeouts) / SUM(plate_appearances)",
        "stolen_base_percentage": "SUM(stolen_bases) / SUM(stolen_bases + caught_stealing)",
    }
