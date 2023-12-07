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

{% docs trajectory_pop_up %}
    (sometimes PU)  Number of plate appearances that ended in a pop-up, also called a pop fly.
    Pop-ups are distinguished from fly balls in that they are hit at a higher angle, tend to be
    hit with less exit velocity, and (as a result) end up in the infield or shallow outfield. Before
    Statcast-era standardization, the distinction between a fly ball and a pop-up was a matter of
    scorerkeeper judgement, and many unofficial scorekeepers did not distinguish between the two.
    Statcast determines pop flies exclusively by angle (> 50 degrees).
{% enddocs %}

{% docs trajectory_unknown %}
    Number of plate appearances ending in a batted ball whose trajectory was not recorded and cannot
    be reliably deduced from context. This number includes balls that we know were hit in the air,
    but do not know which kind of air ball (FB/PU/LD) they were (see trajectory_broad_classification_unknown
    for a number that does not include those balls). The strong majority of batted balls prior to 1988
    fall into this category, especially hits.
{% enddocs %}

{% docs trajectory_known %}
    Number of plate appearances ending in a batted ball whose trajectory was recorded or was reliably deduced
    from the context. An example of reliable deduction is an at-bat with the fielding play 6-3, which
    almost always is a ground ball fielded by the shortstop and thrown to the first baseman. See
    `calc_batted_ball_type` for the deduction logic.
{% enddocs %}

{% docs trajectory_broad_air_ball %}
    Number of plate appearances that ended in an air ball (a fly ball, line drive, or pop-up).
    Because it is much easier to deduce that a ball was hit in the air than it is to deduce the exact
    trajectory, this number field is more reliably populated than any of its three consituent parts.
{% enddocs %}

{% docs trajectory_broad_ground_ball %}
    Same as `trajectory_ground_ball`.
{% enddocs %}

{% docs trajectory_broad_unknown %}
    Number of plate appearances ending in a batted ball whose trajectory was not recorded and cannot
    be reliably deduced from context, even to the extent of knowing whether it was a ground ball or an air ball.
    This will include a disproportionate number of hits, which are more likely to be missing trajectory data
    and harder to make deductions about.
{% enddocs %}

{% docs trajectory_broad_known %}
    Number of plate appearances ending in a batted ball whose ground/air status was recorded or
    reliably deduced from the context. This is the sum of `trajectory_broad_air_ball` and
    `trajectory_broad_ground_ball`. Outs in play have excellent coverage historically here,
    even for older games.
{% enddocs %}

{% docs bunts %}
    Number of plate appearances ending in an in-play bunt.
    This does not include strikeouts on foul bunts with two strikes.
{% enddocs %}

{% docs batted_distance_plate %}
    Number of plate appearances in which the ball was batted to catcher's area around home plate.
{% enddocs %}

{% docs batted_distance_infield %}
    Number of plate appearances in which the ball was batted to the infield (not including the catcher).
    All ground balls are included here, regardless of whether they made it through to the outfield.
{% enddocs %}

{% docs batted_distance_outfield %}
    Number of plate appearances in which the ball was hit on the fly to the outfield.
{% enddocs %}

{% docs batted_distance_unknown %}
    Number of plate appearances in which the ball was hit, but the distance was not recorded
    and cannot be reliably deduced from context.
{% enddocs %}

{% docs batted_distance_known %}
    Number of plate appearances in which the ball was hit, and the distance was either
    recorded or reliably deduced from context.
{% enddocs %}

{% docs fielded_by_battery %}
    Number of plate appearances in which the ball was fielded by the pitcher or catcher.
{% enddocs %}

{% docs fielded_by_infielder %}
    Number of plate appearances in which the ball was fielded by an infielder.
{% enddocs %}

{% docs fielded_by_outfielder %}
    Number of plate appearances in which the ball was fielded by an outfielder.
{% enddocs %}

{% docs fielded_by_known %}
    Number of plate appearances in which the ball was fielded by a player, and the player was recorded.
{% enddocs %}

{% docs fielded_by_unknown %}
    Number of plate appearances in which the ball was fielded by a player, but the player was not recorded.
{% enddocs %}

{% docs batted_angle_left %}
    Number of plate appearances in which the ball was batted to the left side of the field.
    This includes balls where the location was not recorded, but the fielder is on the left side
    (3B, LF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_angle_right %}
    Number of plate appearances in which the ball was batted to the right side of the field.
    This includes balls where the location was not recorded, but the fielder is on the right side
    (1B, RF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_angle_middle %}
    Number of plate appearances in which the ball was batted to the middle of the field.
    This includes balls where the location was not recorded, but the fielder is up the middle
    (P, 2B, SS, CF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_angle_unknown %}
    Number of plate appearances in which the ball was batted, but we don't have enough location
    to determine the spray angle.
{% enddocs %}

{% docs batted_angle_known %}
    Number of plate appearances in which the ball was hit and we have enough location to determine
    the spray angle.
{% enddocs %}

{% docs batted_location_plate %}
    Number of plate appearances in which the ball was batted to the catcher's area around home plate.
{% enddocs %}

{% docs batted_location_right_infield %}
    Number of plate appearances in which the ball was batted to the right side of the infield.
    This includes balls where the location was not recorded, but the fielder is on the right side
    (1B). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_middle_infield %}
    Number of plate appearances in which the ball was batted to the middle of the infield.
    This includes balls where the location was not recorded, but the fielder is up the middle
    (P, SS, 2B). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_left_infield %}
    Number of plate appearances in which the ball was batted to the left side of the infield.
    This includes balls where the location was not recorded, but the fielder is on the left side
    (3B). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_left_field %}
    Number of plate appearances in which the ball was batted to the left side of the outfield.
    This includes balls where the location was not recorded, but the fielder is on the left side
    (LF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_center_field %}
    Number of plate appearances in which the ball was batted to the center of the outfield.
    This includes balls where the location was not recorded, but the fielder is up the middle
    (CF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_right_field %}
    Number of plate appearances in which the ball was batted to the right side of the outfield.
    This includes balls where the location was not recorded, but the fielder is on the right side
    (RF, CF). See `seed_hit_location_categories` and `seed_hit_to_fielder_categories` for more details.
{% enddocs %}

{% docs batted_location_unknown %}
    Number of plate appearances in which the ball was batted, but we don't have enough location
    to determine the specific location.
{% enddocs %}

{% docs batted_location_known %}
    Number of plate appearances in which the ball was batted and we have enough location to determine
    the specific location.
{% enddocs %}

{% docs batted_balls_pulled %}
    Number of plate appearances in which the ball was pulled (the left side for right-handed batters, the right side for left-handed batters).
{% enddocs %}

{% docs batted_balls_opposite_field %}
    Number of plate appearances in which the ball was hit to the opposite field (the right side for right-handed batters, the left side for left-handed batters).
{% enddocs %}

{% docs runs %}
   (R) Number of runs scored.
{% enddocs %}

{% docs times_reached_base %}
    Number of times a batter ended a plate appearance on base, even if it was through a fielder's choice, error, etc.
{% enddocs %}

{% docs times_lead_runner %}
    Number of events that a runner was the lead runner on a play
    (the runner who is furthest along the basepaths).
    Batter is never counted as the lead runner. No-play events excluded,
    but events without plate appearances are included.
{% enddocs %}

{% docs times_force_on_runner %}
    Number of events that a force existed on the runner's next base.
    The batter is counted as having a force on them (at first).
    No-play events excluded, but events without plate appearances are included.
{% enddocs %}

{% docs times_next_base_empty %}
    Number of events that the runner's next base was empty.
    The batter is counted on events where first base is empty.
    No-play events excluded, but events without plate appearances are included.
{% enddocs %}

{% docs stolen_base_opportunities %}
    Number of events in which a runner had an opportunity to steal a base as the
    lead basestealer OR the runner recorded a SB/CS in any situation.
    "Opportunity" is defined as a situation in which the next base
    was empty at the start of the event (not including the batter).
    No-play events excluded, but events without plate appearances are included.
{% enddocs %}

{% docs stolen_base_opportunities_second %}
    Number of opportunities to steal second base (see `stolen_base_opportunities` for detailed criteria).
{% enddocs %}

{% docs stolen_base_opportunities_third %}
    Number of opportunities to steal third base (see `stolen_base_opportunities` for detailed criteria).
{% enddocs %}

{% docs stolen_base_opportunities_home %}
    Number of opportunities to steal home (see `stolen_base_opportunities` for detailed criteria).
{% enddocs %}

{% docs stolen_bases %}
    (SB) Number of successful stolen bases.
{% enddocs %}

{% docs stolen_bases_second %}
    Number of successful steals of second base.
{% enddocs %}

{% docs stolen_bases_third %}
    Number of successful steals of third base.
{% enddocs %}

{% docs stolen_bases_home %}
    Number of successful steals of home.
{% enddocs %}

{% docs caught_stealing %}
    (CS) Number of times a runner was caught stealing.
{% enddocs %}

{% docs caught_stealing_second %}
    Number of times a runner was caught stealing second base.
{% enddocs %}

{% docs caught_stealing_third %}
    Number of times a runner was caught stealing third base.
{% enddocs %}

{% docs caught_stealing_home %}
    Number of times a runner was caught stealing home.
{% enddocs %}

{% docs picked_off %}
    (PO, at risk of confusion with putouts) Number of times a runner was picked off.
{% enddocs %}

{% docs picked_off_first %}
    Number of times a runner was picked off first base.
{% enddocs %}

{% docs picked_off_second %}
    Number of times a runner was picked off second base.
{% enddocs %}

{% docs picked_off_third %}
    Number of times a runner was picked off third base.
{% enddocs %}

{% docs picked_off_caught_stealing %}
    (POCS) Number of times a runner was picked off, but instead of going back to the bag,
    tried to run to the next base and was put out.
{% enddocs %}

{% docs outs_on_basepaths %}
    Number of outs recorded by a baserunner (this is not mutually exclusive with outs recorded by the batter
    in cases like failed advances or dropped third-strike putouts).
{% enddocs %}

{% docs unforced_outs_on_basepaths %}
    Number of outs recorded by a baserunner that was not the result of a force on the runner.
    "Unforced" is meant to be in both the literal sense of a force not being in play, but also
    the figurative sense of the runner being responsible for the out. The latter may or may not
    be the best interpretation of any given play, but it is useful to assign responsibility to the
    runner by default in those contexts.
{% enddocs %}

{% docs outs_avoided_on_errors %}
    Number of times that a baserunner would have been out, but an error allowed them to remain
    on the basepaths (either staying put or advancing).
{% enddocs %}

{% docs advances_on_wild_pitches %}
    (WP) Number of times a baserunner advanced on a wild pitch.
{% enddocs %}

{% docs advances_on_passed_balls %}
    (PB) Number of times a baserunner advanced on a passed ball.
{% enddocs %}

{% docs advances_on_balks %}
    (sometimes BK) Number of times a baserunner advanced on a balk.
{% enddocs %}

{% docs advances_on_unspecified_plays %}
    Number of times a baserunner advanced for an unspecified reason.
{% enddocs %}

{% docs advances_on_defensive_indifference %}
   (DI) Number of times a baserunner advanced on defensive indifference.
   Defensive indifference is a judgement call by the official scorer that the defense
   did not try to stop the runner from stealing a base. This usually happens
   when the defense has a lead late in the game.
{% enddocs %}

{% docs advances_on_errors %}
    Number of times a baserunner advanced on an error.
{% enddocs %}

{% docs plate_appearances_while_on_base %}
    Number of plate appearances in which the baserunner started on 1st, 2nd, or 3rd base.
{% enddocs %}

{% docs balls_in_play_while_running %}
    Number of balls in play while either batting or on base.
{% enddocs %}

{% docs balls_in_play_while_on_base %}
    Number of balls in play in which the baserunner started on 1st, 2nd, or 3rd base.
{% enddocs %}

{% docs batter_total_bases_while_running %}
    Number of total bases accumulated by the batter while the baserunner was running, including the batter.
{% enddocs %}

{% docs batter_total_bases_while_on_base %}
    Number of total bases accumulated by the batter while the baserunner was on base, excluding the batter.
{% enddocs %}

{% docs extra_base_advance_attempts %}
    Number of times a baserunner tried to advance by a greater number of bases than the batter.
{% enddocs %}

{% docs bases_advanced %}
    Number of bases advanced by a baserunner, including the batter.
{% enddocs %}

{% docs bases_advanced_on_balls_in_play %}
    Number of bases advanced by a baserunner on a ball in play, including the batter.
{% enddocs %}

{% docs surplus_bases_advanced_on_balls_in_play %}
    Number of bases advanced by a baserunner on a ball in play minus the number of total bases
    accumulated by the batter on the same play. For example, if a runner goes from first to third
    on a single, this number is 1 (3 - 2). If a runner only goes from second to third on a double,
    this number is -1 (1 - 2).
{% enddocs %}

{% docs outs_on_extra_base_advance_attempts %}
    Number of times a baserunner was out attempting to advance
    by a greater number of bases than the batter. This includes
    batters who were put out trying to stretch a hit to the next base.
{% enddocs %}

{% docs pitches %}
    Number of pitches thrown.
{% enddocs %}

{% docs swings %}
    Number of pitches that were swung at.
{% enddocs %}

{% docs swings_with_contact %}
    Number of pitches that were swung at and made contact.
{% enddocs %}

{% docs strikes %}
    Number of pitches that were called or swinging strikes.
{% enddocs %}

{% docs strikes_called %}
    Number of pitches that were called strikes.
{% enddocs %}

{% docs strikes_swinging %}
    Number of pitches that were swung on and missed (mutually exclusive
    with `swings_with_contact`, which also count as strikes).
{% enddocs %}

{% docs strikes_foul %}
    Number of pitches that were fouled off.
{% enddocs %}

{% docs strikes_foul_tip %}
    Number of pitches that were fouled off and caught by the catcher for strike three.
{% enddocs %}

{% docs strikes_in_play %}
    Number of pitches that were swung on and batted into play.
{% enddocs %}

{% docs strikes_unknown %}
    Number of pitches that we know were strikes, but don't know what kind.
{% enddocs %}

{% docs balls %}
    Number of pitches that were called balls.
{% enddocs %}

{% docs balls_called %}
    Number of pitches that were called balls (as opposed to automatic balls that were
    not actually thrown).
{% enddocs %}

{% docs balls_intentional %}
    Number of pitches that were called balls as part of an intentional walk.
{% enddocs %}

{% docs balls_automatic %}
    Number of pitches that were called balls on an automatic walk or a delay penalty.
{% enddocs %}

{% docs unknown_pitches %}
    Number of pitches that were thrown without any other information recorded.
{% enddocs %}

{% docs pitchouts %}
    Number of pitches that were thrown as pitchouts. A pitchout is a pitch that is thrown
    intentionally very far outside in order to make it easier for the catcher to throw out
    a baserunner who is likely to steal.
{% enddocs %}

{% docs pitcher_pickoff_attempts %}
    Number of times the pitcher attempted to pick off a baserunner.
{% enddocs %}

{% docs catcher_pickoff_attempts %}
    Number of times the catcher attempted to pick off a baserunner.
{% enddocs %}

{% docs pitches_blocked_by_catcher %}
    Number of pitches that were blocked by the catcher.
{% enddocs %}

{% docs pitches_with_runners_going %}
    Number of pitches that were thrown while a baserunner was on the move
    (as part of a steal or hit-and-run).
{% enddocs %}

{% docs passed_balls %}
    (PB) Number of passed balls.
{% enddocs %}

{% docs wild_pitches %}
    (WP) Number of wild pitches.
{% enddocs %}

{% docs balks %}
    (BK) Number of balks.
{% enddocs %}

{% docs left_on_base %}
    (LOB) At an individual level, the number of baserunners that a batter failed to advance during a plate appearance.
    At a team level, the number of baserunners remaining on base at the end of an inning. In order to count, baserunners
    must not have scored or been put out.
{% enddocs %}

{% docs left_on_base_with_two_outs %}
    (LOB) At an individual level, the number of baserunners that remain on base (unscored and not out) after a plate appearance that ends with the third out recorded. At a team level, this is interchangable with `left_on_base`.
{% enddocs %}

{% docs games_started %}
    (GS) Number of games started by a player.
{% enddocs %}

{% docs innings_pitched %}
    (IP) Number of innings pitched by a pitcher. Fractional innings are given by .33 and .67 here,
    but they are often formatted as .1 and .2 elsewhere.
{% enddocs %}

{% docs inherited_runners %}
    (IR) Number of runners on base when a pitcher entered the game.
{% enddocs %}

{% docs bequeathed_runners %}
    Number of runners on base when a pitcher left the game.
{% enddocs %}

{% docs games_relieved %}
    Number of games in which a pitcher entered the game in relief.
{% enddocs %}

{% docs games_finished %}
    (GF) Number of games in which a pitcher entered the game in relief and finished the game.
{% enddocs %}

{% docs save_situations_entered %}
    Number of games in which a pitcher entered the game in a save situation. A save situation is defined
    as when the pitcher enters the game ineligible to get the win with one of the following conditions:

    - A lead of three runs or less and ineligible to get the win
    - Any lead with the tying run on base, at bat, or on deck

    If a pitcher pitches the final three innings of a game with any lead, they are credited with a save
    but are not considered to have entered a save situation unless one of the above conditions is met.
    Note the difference between this field and `save_opportunities`, which does not count games in which
    the pitcher exited without completing or blowing the save.
{% enddocs %}

{% docs holds %}
    (HLD, H) Number of holds recorded by a pitcher. A hold is defined as a relief appearance in which
    the pitcher enters the game in a save situation, records at least one out, and leaves the game
    with the lead intact.
{% enddocs %}

{% docs blown_saves %}
    (BS) Number of games in which a pitcher entered the game in a save situation and gave up the lead.
    The status of inherited runners is irrelevant in determining who blew the save - it is entirely
    a function of who was on the mount when the lead as lost.
{% enddocs %}

{% docs saves_by_rule %}
    Number of games in which a pitcher earned a save according to the default rules.
    This may be different than the actual number of saves, as the official scorer may award
    the player a win instead of a save at their discretion in rare cases.
{% enddocs %}

{% docs save_opportunities %}
    (SVO) The sum of saves and blown saves.
{% enddocs %}

{% docs wins %}
    (W) Number of games in which a pitcher was credited with the win.
{% enddocs %}

{% docs losses %}
    (L) Number of games in which a pitcher was debited with the loss.
{% enddocs %}

{% docs saves %}
    (SV) Number of games in which a pitcher was credited with the save.
{% enddocs %}

{% docs earned_runs %}
    (ER) Number of runs that are charged to a pitcher. This may include runs that scored after the pitcher
    left the game (in the case of inherited runners), as well as runs that the scorekeeper deemed to be
    the pitcher's responsibility in the absence of official inherited runner rules in the olden days.
    Earned runs are only an official statistic at a game level: the earnedness of any given run is never
    officially specified, only the total. It would be possible to make a very good guess algorithmically,
    but that would be a huge pain for a stat that is not very useful in the first place.
{% enddocs %}

{% docs complete_games %}
    (CG) Number of games in which a pitcher pitched the entire game, regardless of how long the game lasted.
{% enddocs %}

{% docs shutouts %}
    (SHO) Number of games in which a pitcher recorded every out for his team and did not allow any runs,
    earned or unearned, regardless of how long the game lasted. Note the subtle difference between the
    definition of a shutout and a complete game: it is possible to pitch a shutout without a complete game
    if the pitcher enters in relief when no outs have been recorded. AFAIK, the only reason this distinction
    exists is for us to remember the time that Babe Ruth started a game, walked a batter, yelled at the umpire,
    got ejected, punched the umpire, and then Ernie Shore came in and retired every batter he faced,
    which is good enough for me.
{% enddocs %}

{% docs quality_starts %}
    (QS) Number of games in which a starting pitcher pitched at least six innings and allowed three or fewer earned runs.
{% enddocs %}

{% docs cheap_wins %}
    Number of wins in which a starting pitcher did not record a quality start.
{% enddocs %}

{% docs tough_losses %}
    Number of losses in which a starting pitcher record a quality start.
{% enddocs %}

{% docs no_decisions %}
   (ND) Number of games in which a pitcher pitched but did not recieve a win or a loss.
{% enddocs %}

{% docs no_hitters %}
    At an individual level, the number of games in which a pitcher was the only pitcher for their team and pitched a full-length game without allowing any hits. At a team level, the "only pitcher" requirement is dropped.
{% enddocs %}

{% docs perfect_games %}
    Number of games in which a pitcher was the only pitcher for their team and pitched the entire game
    without allowing any baserunners. At a team level, the "only pitcher" requirement is dropped.
{% enddocs %}

{% docs batters_faced %}
    Number of batters faced by a pitcher, generally equivalent to plate appearances (in old box-score-only games, this may not match up perfectly).
{% enddocs %}

{% docs outs_recorded %}
    Number of outs recorded by a pitcher.
{% enddocs %}

{% docs inherited_runners_scored %}
    (IRS) Number of inherited runners that scored while a pitcher was on the mound.
{% enddocs %}

{% docs bequeathed_runners_scored %}
    Number of bequeathed runners that scored after a pitcher left the game.
{% enddocs %}

{% docs team_unearned_runs %}
    Number of runs that are charged as earned to the pitcher, but unearned to the team.
    This happens when a pitcher enters an inning which should have been over already because
    of an error and then the pitcher allows a run to score without the help of any additional
    errors. This is a rare occurrence and it might seem unnecessary to track,
    but it is absolutely essential that we do so in order to preserve the space-time continuum.
{% enddocs %}

---- Fielding -----

{% docs putouts %}
    (PO) Outs credited to fielder based on who physically made the out, either by:
        - Catching a ball on the fly
        - Tagging a runner out
        - Forcing a runner out
        - Catching a called third strike
        - Being the nearest fielder to a baserunner who is called out for interference

    Putouts have been an important baseball statistic longer than MLB has been around,
    so it has excellent historical coverage. Unassisted
    putouts are very important in determining the location of batted balls.
    
    In modern baseball rules, each out in the game is credited as a putout, and at the play/event
    level this will always be the case. However, in older games for which we only have box score or season-level
    data, rare plays like baserunner interference may not be assigned to a specific fielder.
    
    Every play-by-play out has a putout, but the fielder who gets the putout may be unknown.
{% enddocs %}

{% docs assists %}
    (A) Number of outs in which a fielder touched the ball before anothber fielder recorded a putout.
    This is almost always a throw to another fielder, but it can also be a deflection.

    Assists have excellent historical data coverage, and they are particularly useful in determining
    the location of ground balls. The data is present in almost all season-level and game-level accounts.
    However, individual plays with unknown-fielder putouts are likely to be missing assists entirely.
{% enddocs %}

{% docs errors %}
    (E) Number of errors recorded by a fielder. This is recorded when a fielder either fails to make an out
    that an average fielder should have made or makes a bad play that allows a baserunner to advance.
    Errors are awarded at the scorekeeper's discretion, making them more subjective than the other two main
    fielding stats.
    
    Errors used to be a much larger component of evaluating fielder quality.
    When the stat was invented, routine plays were completed much less frequently than they are today.
    As baseball evolved, routine plays became truly routine, and a fielder's ability to get to the ball
    became more relevant than their ability
    to reliably convert the out once they got there. While plenty of people as early as the 19th century were
    aware of this, statistics mostly failed to reflect it until the 1980s, when Bill James and others
    developed Range Factor and other stats that attempted to measure a fielder's ability to create chances.
{% enddocs %}

{% docs fielders_choices %}
    (FC) Number of times a fielder who fielded the ball attempted to put out a baserunner other than the batter.
    A fielder's choice can be assigned when the attempt fails, even if there was no error on the play.

    Fielder's choices are only available on play-by-play data and tend to be recorded when the attempt was
    a tag and not a force. Force plays are explicitly noted and have little overlap with fielder's choices.
{% enddocs %}

{% docs plays_started %}
    Number of times that a fielder made the first play on a prospective batted-ball-out and did not immediately record an error.
    Hits without any putouts are not included, but assists made on plays that had errors or failed fielder's choices do count.
{% enddocs %}

{% docs assisted_putouts %}
    Number of putouts by the fielder that had at least one recorded, associated assist. For older games, this may be a subset
    of the true total assisted putouts, as unknown-fielder putouts almost never have recorded assists.
{% enddocs %}

{% docs first_errors %}
    Number of events in which a fielder made an error that was the first attempted out on the play.
{% enddocs %}

{% docs unknown_putouts %}
    Number of putouts that were recorded, but without a specified fielder.
{% enddocs %}

{% docs incomplete_events %}
    Number of events in which at least one known fielding play was made by an unknown fielder.
{% enddocs %}

{% docs fielding_plays %}
    The total number of fielding plays of any kind, as determined by the raw count from `stg_event_fielding_plays`.
{% enddocs %}