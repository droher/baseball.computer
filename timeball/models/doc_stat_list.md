<!-- {% macro event_level_offense_stats() %}
    {{ return([
        "plate_appearances",
        "at_bats",
        "hits",
        "singles",
        "doubles",
        "triples",
        "home_runs",
        "total_bases",
        "strikeouts",
        "walks",
        "intentional_walks",
        "hit_by_pitches",
        "sacrifice_hits",
        "sacrifice_flies",
        "reached_on_errors",
        "reached_on_interferences",
        "inside_the_park_home_runs",
        "ground_rule_doubles",
        "infield_hits",
        "on_base_opportunities",
        "on_base_successes",
        "runs_batted_in",
        "grounded_into_double_plays",
        "double_plays",
        "triple_plays",
        "batting_outs",
        "balls_in_play",
        "balls_batted",
        "trajectory_fly_ball",
        "trajectory_ground_ball",
        "trajectory_line_drive",
        "trajectory_pop_fly",
        "trajectory_unknown",
        "trajectory_known",
        "trajectory_broad_type_air_ball",
        "trajectory_broad_type_ground_ball",
        "trajectory_broad_type_unknown",
        "trajectory_broad_type_known",
        "bunts",
        "batted_distance_plate",
        "batted_distance_infield",
        "batted_distance_outfield",
        "batted_distance_unknown",
        "batted_distance_known",
        "fielded_by_battery",
        "fielded_by_infielder",
        "fielded_by_outfielder",
        "fielded_by_known",
        "fielded_by_unknown",
        "batted_angle_left",
        "batted_angle_right",
        "batted_angle_middle",
        "batted_angle_unknown",
        "batted_angle_known",
        "batted_location_plate",
        "batted_location_right_infield",
        "batted_location_middle_infield",
        "batted_location_left_infield",
        "batted_location_left_field",
        "batted_location_center_field",
        "batted_location_right_field",
        "batted_location_unknown",
        "batted_location_known",
        "batted_balls_pulled",
        "batted_balls_opposite_field",
        "runs",
        "times_reached_base",
        "stolen_bases",
        "caught_stealing",
        "picked_off",
        "picked_off_caught_stealing",
        "outs_on_basepaths",
        "unforced_outs_on_basepaths",
        "outs_avoided_on_errors",
        "advances_on_wild_pitches",
        "advances_on_passed_balls",
        "advances_on_balks",
        "advances_on_unspecified_plays",
        "advances_on_defensive_indifference",
        "advances_on_errors",
        "plate_appearances_while_on_base",
        "balls_in_play_while_running",
        "balls_in_play_while_on_base",
        "batter_total_bases_while_running",
        "batter_total_bases_while_on_base",
        "extra_base_advance_attempts",
        "bases_advanced",
        "bases_advanced_on_balls_in_play",
        "surplus_bases_advanced_on_balls_in_play",
        "outs_on_extra_base_advance_attempts",
        "pitches",
        "swings",
        "swings_with_contact",
        "strikes",
        "strikes_called",
        "strikes_swinging",
        "strikes_foul",
        "strikes_foul_tip",
        "strikes_in_play",
        "strikes_unknown",
        "balls",
        "balls_called",
        "balls_intentional",
        "balls_automatic",
        "unknown_pitches",
        "pitchouts",
        "pitcher_pickoff_attempts",
        "catcher_pickoff_attempts",
        "pitches_blocked_by_catcher",
        "pitches_with_runners_going",
        "passed_balls",
        "wild_pitches",
        "balks",
        "left_on_base",
        "left_on_base_with_two_outs"
    ]) }}
{% endmacro %}

{% macro event_level_pitching_stats() %}
    {% set non_pitching_stats = [
        "plate_appearances",
        "runs_batted_in",
        "plate_appearances_while_on_base",
        "balls_in_play_while_running",
        "balls_in_play_while_on_base",
        "batter_total_bases_while_running",
        "batter_total_bases_while_on_base",
    ] %}
    {% set extra_pitching_stats = [
        "batters_faced",
        "outs_recorded",
        "inherited_runners_scored",
        "bequeathed_runners_scored",
        "team_unearned_runs"
    ] %}

    {{ return(extra_pitching_stats + remove_items(event_level_offense_stats(), non_pitching_stats)) }}
{% endmacro %}


{% macro game_level_pitching_stats() %}
    {{ return([
        "games_started",
        "innings_pitched",
        "inherited_runners",
        "bequeathed_runners",
        "games_relieved",
        "games_finished",
        "save_situations_entered",
        "holds",
        "blown_saves",
        "saves_by_rule",
        "save_opportunities",
        "wins",
        "losses",
        "saves",
        "earned_runs",
        "complete_games",
        "shutouts",
        "quality_starts",
        "cheap_wins",
        "tough_losses",
        "no_decisions",
        "no_hitters",
        "perfect_games",
    ]) }}
{% endmacro %}
 -->
The descriptions for the official stats here are in part adapted from MLB's official glossary: https://www.mlb.com/glossary

{% docs plate_appearances %}
    (PA) Number of times a batter came to the plate, including walks, hit by pitches, sacrifices, and at-bats.
{% enddocs %}

{% docs at_bats %}
    (AB) Number of plate appearances that ended in either a hit or a non-sacrifice out.
{% enddocs %}

{% docs hits %}
    (H) Number of times a batter reached base safely without an error or fielder's choice.
{% enddocs %}

{% docs singles %}
    (1B) Number of hits in which a batter reached first base.
{% enddocs %}

{% docs doubles %}
    (2B) Number of hits in which a batter reached second base without the help of an error or an attempted
    play on another runner.
{% enddocs %}

{% docs triples %}
    (3B) Number of hits in which a batter reached third base without the help of an error or an attempted
    play on another runner.
{% enddocs %}

{% docs home_runs %}
    (HR) Number of hits in which a batter reached home plate without the help of an error or an attempted
    play on another runner (usually by hitting the ball out of the park).
{% enddocs %}

{% docs total_bases %}
    (TB) Number of bases a batter reached safely without the help of an error or an attempted
    play on another runner. HR * 4 + 3B * 3 + 2B * 2 + (H - HR - 3B - 2B)
{% enddocs %}

{% docs strikeouts %}
    (K, SO) Number of times a batter struck out. This includes plays in which a batter reached base on
    a dropped third strike.
{% enddocs %}

{% docs walks %}
    (BB, occasionally W) Number of times a batter reaches base on called balls out of the strike zone
    (Four balls for all of MLB history after the 1880s).
{% enddocs %}

{% docs intentional_walks %}
    (IBB, occasionally IW) Number of times a batter was intentionally walked. This number may be missing or undercounted
    in earlier years, as intentional walks were not officially tracked until 1955.
{% enddocs %}

{% docs hit_by_pitches %}
    (HBP) Number of times a batter was awarded first base after being hit by a pitch.
{% enddocs %}

{% docs sacrifice_hits %}
    (SH) Number of times a batter performed a sacrifice bunt to advance another runner.
    A bunt may count as a sacrifice even if the batter reaches base safely.
    Unsuccessful attempts are not counted, nor are non-sacrifice bunts.
{% enddocs %}

{% docs sacrifice_flies %}
    (SF) Number of times a batter hit a fly ball that resulted in an out (or error),
    but allowed a runner to score on the throw.
{% enddocs %}

{% docs reached_on_errors %}
    (ROE) Number of times a batter reached base safely due to an error.
{% enddocs %}

{% docs reached_on_interferences %}
    Number of times a batter was awarded first base for being illegally hindered
    by a fielder, usually the catcher.
{% enddocs %}

{% docs inside_the_park_home_runs %}
    Number of times a batter hit a home run without the ball leaving the field of play.
{% enddocs %}

{% docs ground_rule_doubles %}
    Number of times a batter was awarded a double on a ball that went out of play after
    bouncing in fair territory.
{% enddocs %}

{% docs infield_hits %}
    Number of times a batter reached base safely on a hit that did not reach the outfield.
{% enddocs %}

{% docs on_base_opportunities %}
    Number of plate appearances in which a batter either did or did not reach base,
    as defined by the formula for on-base percentage. Note that this is very similar to but different
    from plate_appearances, as it does not include sacrifice hits or interference.
{% enddocs %}

{% docs on_base_successes %}
    Number of hits, walks, and hit by pitches that serves as the numerator in the formula
    for on-base percentage.
{% enddocs %}

{% docs runs_batted_in %}
    (RBI) Number of runs that scored as a result of a batter's plate appearance. This is usually
    the number of runs that scored on the play, but errors and other similar cases may cause
    some runs not to be credited to the batter. Scorerkeeper discretion occasionally causes differences
    between the number in the database and the official MLB total.
{% enddocs %}

{% docs grounded_into_double_plays %}
    (GIDP) Number of times a batter grounded into a double play. This is the conventional way
    to record a double play and as such is an important statistic on its own. Games without
    play-by-play accounts don't have data on the trajectory of the double play, so
    this number is not populated for those games.
{% enddocs %}

{% docs double_plays %}
    (DP) Number of times a play ended in two outs being recorded.
{% enddocs %}

{% docs triple_plays %}
    (TP) Number of times a play ended in three outs being recorded.
{% enddocs %}

{% docs batting_outs %}
    Number of outs that "should" have been recorded as a result of a batter's plate apperances.
    A batting out is still counted if no actual out is recorded on an error or a failed fielder's choice.
    Outs on baserunners (including the batter trying to stretch a hit) do not count here.
    Grounded-into-double-plays count as two outs, but other types of double plays do not (the 
    idea here is that the baserunner is responsible on other types of double plays).
    Unofficial stat, but designed here to be generally useful in determining rates of official stats.
{% enddocs %}

{% docs balls_in_play %}
    Number of plate appearances that resulted in a live
    ball on the field of play. The difference between this and batted_balls is that
    balls_in_play does not include out-of-the-park home runs. This distinction is important
    for calculating batting average on balls in play, a stat designed to isolate at_bats
    in which the defense was involved.
{% enddocs %}

{% docs balls_batted %}
    Number of plate appearances that ended in a fair ball or a foul flyout. This is equivalent
    to balls_in_play + home_runs, and may be a more useful denominator for "in play" stats depending
    on the context.
{% enddocs %}

{% docs trajectory_fly_ball %}
    (FB) Number of plate appearances that ended in a fly ball. A fly ball is defined here
    as the subset of balls hit in the air that are neither line drives nor pop flies.
{% enddocs %}

{% docs trajectory_ground_ball %}
    (GB) Number of plate appearances that ended in a ground ball, also called a grounder. A ground ball is defined
    here to only include swings, not bunts, although older data may not have a proper distinction. Ground
    balls tend to be easy to distinguish between other types of contact, but Statcast-era data defines it
    as balls with a launch angle under 10 degrees.
{% enddocs %}

{% docs trajectory_line_drive %}
    (LD) Number of plate appearances that ended in a line drive, also called a liner. Line drives are distiguished
    from fly balls by some combination of angle and exit velocity. In the Statcast era, line drives
    are defined purely in terms of launch angle (10-25 degrees), but just about any colloquial definition
    involves hard-hitness as well. The line-drive-fly-ball distinction is by far the most arbitrary
    and subjective trajectory categorization. Many scorekeepers never included line drives, while others counted
    any successful in-play air-hit as a line drive. Nevertheless, the distinction is a crucial one,
    as line drives are by far the most likely type of batted ball to result in a hit (even when they are not hit very hard).
{% enddocs %}

{% docs trajectory_pop_fly %}
    (sometimes PU) Number of times a batter hit a pop fly, also called a pop-up.
    Pop flies are distinguished from fly balls in that they are hit at a higher angle, tend to be
    hit with less exit velocity, and (as a result) end up in the infield or shallow outfield. Before
    Statcast-era standardization, the distinction between a fly ball and a pop-up was a matter of
    scorerkeeper judgement, and many unofficial scorekeepers did not distinguish between the two.
    Statcast determines pop flies exclusively by angle (> 50 degrees).
{% enddocs %}

{% docs trajectory_unknown %}
    Number of plate appearances ending in a batted ball whose trajectory was not recorded and cannot
    be reliably inferred from context. This number includes balls that we know were hit in the air,
    but do not know which kind of air ball (FB/PU/LD) they were (see trajectory_broad_classification_unknown
    for a number that does not include those balls). The strong majority of batted balls prior to 1988
    fall into this category, especially hits.
{% enddocs %}

{% docs trajectory_known %}
    Number of times a batter hit a ball in play whose trajectory was recorded or was reliably inferred
    from the context. An example of reliable inference is an at-bat with the fielding play 6-3, which
    almost always is a ground ball fielded by the shortstop and thrown to the first baseman. See
    `calc_batted_ball_type` for the inference logic.
{% enddocs %}

{% docs trajectory_broad_type_air_ball %}
    Number of times a batter hit a ball in play whose trajectory was recorded and was an air ball.
{% enddocs %}

{% docs trajectory_broad_type_ground_ball %}
    Number of times a batter hit a ball in play whose trajectory was recorded and was a ground ball.
{% enddocs %}

{% docs trajectory_broad_type_unknown %}
    Number of times a batter hit a ball in play whose trajectory was recorded and was unknown.
{% enddocs %}

{% docs trajectory_broad_type_known %}
    Number of times a batter hit a ball in play whose trajectory was recorded and was known.
{% enddocs %}

{% docs bunts %}
    Number of times a batter attempted a bunt.
{% enddocs %}
