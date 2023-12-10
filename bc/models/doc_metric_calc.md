{% docs batting_average %}
    (AVG, BA) Hits divided by at bats. Historically speaking, the single most well-known hitting statistic.
    It retains much of its popularity and cultural significance today, if not its importance: the
    "batting title" goes to the player with the highest batting average.
    While batting average is rightly maligned for the limited picture it captures of a player's offensive
    contribution, it's still a nice object of study when learning about statistical inference.
{% enddocs %}

{% docs on_base_percentage %}
    (OBP) Measures a player's ability to get on base. It is calculated as (H + BB + HBP)/(AB + BB + HBP + SF),
    which is confusingly a bit different from on-base events per plate appearance. We have `on_base_successes`
    and `on_base_opportunities` to make the OBP calculation simpler. The modern analogue to batting average,
    both in what it tries to measure and in its cultural significance.
{% enddocs %}

{% docs slugging_percentage %}
    (SLG) Total bases per at bat. More of an average than a percentage, but the name has stuck (though
    you'll also hear "slugging average"). The simplest and most well-known measure of the "advancement factor"
    of a player's offensive ability.
{% enddocs %}

{% docs on_base_plus_slugging %}
    (OPS) On-base percentage plus slugging percentage. Popularized in the 1980s, OPS is a simple way to
    combine the two aspects of a player's hitting ability. In terms of its overall precision, it is easily surpassed
    by other metrics, but it remains hard to beat for its economy of calculation and expression.
{% enddocs %}

{% docs isolated_power %}
    (ISO) Slugging percentage minus batting average, or the average number of extra bases per at bat.
    An intuitive expression of a player's raw power or ability to push runners around the bases.
    Less indicative of overall ability than slugging percentage, but more precise in what it tries to measure. 
{% enddocs %}

{% docs secondary_average %}
    (SecA) A statistic invented by Bill James that measures a player's ability to gain bases by means independent
    of batting average. Its formula is (TB - H + BB + SB - CS)/(AB). A good way to answer the question of which players
    would be most underrated by only looking at their batting averages. Also a good way to show how well you can understand
    a player's offensive ability without taking their batting average into account.
{% enddocs %}

{% docs batting_average_on_balls_in_play %}
    (BABIP) A measure of the rate at which fieldable balls go for hits. It is calculated as (H - HR)/(AB - K - HR + SF).
    Pitchers are generally (but controversially) thought to have little control over balls in play, so a high BABIP
    is a good sign that that they have been unlucky or have played in front of a poor defense. Most pitchers do have some
    "true" BABIP ability that is different from the league average, but the difference is usually much smaller than the year-to-year
    variance in their actual BABIP. In rare cases, pitchers can have such a strong influence on balls in play that their
    contribution is measurable in smaller sample sizes. For pitchers in the dead-ball era and earlier, it makes less sense
    to treat BABIP as luck because a much higher percentage of at-bats ended in balls in play, and pitchers were more focused
    on getting outs by inducing weak contact without having to worry about home runs.

    All of the above is also true for hitters, but to a much lesser extent, as hitters face different defenses, control
    their ability to beat out grounders for infield hits, and generally have more control over the quality of contact they
    make. BABIP can also be misleading for hitters because it excludes home runs, so Barry Bonds ends up having a lower BABIP
    even though there were occasional eyewitness reports of his making solid contact.

    BABIP was invented by Voros McCracken around the turn of the millenium, and it remains the most prominent
    example of a statistic that tracks player luck as opposed to skill or performance.
{% enddocs %}

{% docs home_run_rate %}
    (HR/PA) Home runs per plate appearance.
{% enddocs %}

{% docs walk_rate %}
    (BB/PA) Walks per plate appearance.   
{% enddocs %}

{% docs strikeout_rate %}
    (K/PA) Strikeouts per plate appearance.
{% enddocs %}

{% docs stolen_base_percentage %}
    (SB%) The rate of stolen base attempts that are successful. Calculated as SB/(SB + CS).
{% enddocs %}

{% docs earned_run_average %}
    (ERA) Earned runs per nine innings, the analogue of batting average for pitchers in both its
    fame and its limitations. `earned_runs` are a subset of the total runs allowed by a pitcher:
    those unaffected by fielding errors. ERA is a strange compromise between the desire to isolate
    a pitcher's true defense-independent ability and to describe actually happened when they were on the mound.
    Because fielding errors are a small component of overall luck and defense, it doesn't really remove much
    noise and might even add more. In general, the idea is right but the execution is wrong.
{% enddocs %}

{% docs run_average %}
    (RA) Runs allowed by the pitcher per nine innings. Because of the historical importance of ERA,
    this is often thought of as earned run average plus unearned run average, but this obscures how much simpler
    it is to calculate than ERA. While it is subject to many factors beyond the pitcher's control, it does arguably
    the best possible job of describing what actually happened when the pitcher was on the mound. This makes it a popular
    starting point for some implementations of WAR (such as Baseball Reference's).
{% enddocs %}

{% docs walks_per_9_innings %}
    (BB/9) Walks allowed by the pitcher per nine innings. Probably the statistic most 
    frequently used to describe a pitcher's control. One of the three main "peripheral"
    stats that describe a pitcher's performance on outcomes over which they have the most control.
{% enddocs %}

{% docs strikeouts_per_9_innings %}
    (K/9) Strikeouts recorded by the pitcher per nine innings. One of the three main "peripheral"
    stats that describe a pitcher's performance on outcomes over which they have the most control.
{% enddocs %}

{% docs home_runs_per_9_innings %}
    (HR/9) Home runs allowed by the pitcher per nine innings. One of the three main "peripheral"
    stats that describe a pitcher's performance on outcomes over which they have the most control,
    although the lower frequency of home runs gives it a much higher relative variance than the other two.
{% enddocs %}

{% docs hits_per_9_innings %}
    (H/9) Hits allowed by the pitcher per nine innings.
{% enddocs %}

{% docs walks_and_hits_per_innings_pitched %}
    (WHIP) Describes the rate at which pitchers allow baserunners. It is calculated as (BB + H)/IP.
{% enddocs %}

{% docs strikeout_to_walk_ratio %}
    (K/BB) Ratio of strikeouts to walks, almost always appearing as a pitching statistic even though it
    can also be expressed for hitters. A simple and useful way to describe pitching performance
    on non-at-bat outcomes. Also a bit more robust when comparing across time periods than most
    unadjusted strikeout-related statistics. Leaders in this stat tend to be a mix
    of the most dominant pitchers in the league and contact-oriented control freaks,
    e.g. Pedro Martinez and Phil Hughes (respectively or not, who's to say).
{% enddocs %}

{% docs batting_average_against %}
    (BAA, OAV) Same as batting average, but for pitchers. For the most part,
    this database doesn't rephrase hitting statistics for pitchers, but this one is
    so common that it's worth including.
{% enddocs %}

{% docs on_base_percentage_against %}
    Same as on-base percentage, but for pitchers/defense.
{% enddocs %}

{% docs slugging_percentage_against %}
    Same as slugging percentage, but for pitchers/defense.
{% enddocs %}

{% docs on_base_plus_slugging_against %}
    Same as OPS, but for pitchers/defense.
{% enddocs %}

{% docs known_trajectory_rate_outs %}
    Rate of outs in play for which we know the detailed trajectory of the batted ball.
{% enddocs %}

{% docs known_trajectory_rate_hits %}
    Rate of hits for which we know the detailed trajectory of the batted ball.
{% enddocs %}

{% docs known_trajectory_rate %}
    Overall rate of batted balls for which we know the detailed trajectory.
{% enddocs %}

{% docs known_trajectory_broad_rate_outs %}
    Overall rate of outs in play for which we know whether
    the ball was hit in the air or on the ground.
    This is generally very high even for the oldest play-by-play data.
{% enddocs %}

{% docs known_trajectory_broad_rate_hits %}
    Overall rate of hits for which we know whether
    the ball was hit in the air or on the ground.
{% enddocs %}

{% docs known_trajectory_broad_rate %}
    Overall rate of batted balls for which we know whether
    the ball was hit in the air or on the ground.
{% enddocs %}

{% docs known_trajectory_out_hit_ratio %}
    The ratio of `known_trajectory_rate_outs` to `known_trajectory_rate_hits`.
    This ratio is generally very high for years without complete batted ball data,
    which makes it useful for estimating quantities that would be affected by
    the selection bias.
{% enddocs %}

{% docs known_trajectory_broad_out_hit_ratio %}
    The ratio of `known_trajectory_broad_rate_outs` to `known_trajectory_broad_rate_hits`.
    This ratio is generally very high for years without complete batted ball data,
    which makes it useful for estimating quantities that would be affected by
    the selection bias.
{% enddocs %}

{% docs air_ball_rate_outs %}
    The rate of outs in play that were fly balls, pop-ups, or line drives.
{% enddocs %}

{% docs ground_ball_rate_outs %}
    The rate of outs in play that were ground balls.
{% enddocs %}

{% docs ground_air_out_ratio %}
    The ratio of `ground_ball_rate_outs` to `air_ball_rate_outs`.
    This is a useful metric by itself because it is unaffected by
    the higher percentage of missing data on hits, so it is probably
    a more accurate measure of overall ground ball rate than `ground_ball_rate`
    itself for most seasons.
{% enddocs %}

{% docs air_ball_hit_rate %}
    The rate of hits in play that were fly balls, pop-ups, or line drives.
{% enddocs %}

{% docs ground_ball_hit_rate %}
    The rate of hits in play that were ground balls.
{% enddocs %}

{% docs ground_air_hit_ratio %}
    The ratio of `ground_ball_hit_rate` to `air_ball_hit_rate`.
    Difference between this and `ground_air_out_ratio` is potentially interesting,
    but will be noisy for older years.
{% enddocs %}

{% docs fly_ball_rate %}
    Of all batted_balls for which we know the trajectory,
    The rate that were fly balls.
{% enddocs %}

{% docs line_drive_rate %}
    Of all batted_balls for which we know the trajectory,
    The rate that were line drives.
{% enddocs %}

{% docs pop_up_rate %}
    Of all batted_balls for which we know the trajectory,
    The rate that were pop-ups.
{% enddocs %}

{% docs ground_ball_rate %}
    Of all batted_balls for which we know the trajectory,
    The rate that were ground balls.
{% enddocs %}

{% docs coverage_weighted_air_ball_batting_average %}
    The batting average of batted_balls that were fly balls, pop-ups, or line drives,
    weighted according to the `known_trajectory_broad_out_hit_ratio`. This is an attempt
    to measure trajectory-specific BABIP even in years where trajectory data is rarely
    present for hits. While it handles that specific bias, it is still likely to be noisy
    because of the small sample size of known-trajectory balls.
{% enddocs %}

{% docs coverage_weighted_ground_ball_batting_average %}
    The batting average of batted_balls that were ground balls,
    weighted according to the `known_trajectory_broad_out_hit_ratio`. This is an attempt
    to measure trajectory-specific BABIP even in years where trajectory data is rarely
    present for hits. While it handles that specific bias, it is still likely to be noisy
    because of the small sample size of known-trajectory balls.
{% enddocs %}

{% docs coverage_weighted_fly_ball_batting_average %}
    The batting average of batted_balls that were fly balls,
    weighted according to the `known_trajectory_out_hit_ratio`. This is an attempt
    to measure trajectory-specific BABIP even in years where trajectory data is rarely
    present for hits. It is still vulnerable to the variance and arbitrariness with which
    historical scorekeepers differentiated fly balls from line drives and pop-ups.
{% enddocs %}

{% docs coverage_weighted_line_drive_batting_average %}
    The batting average of batted_balls that were line drives,
    weighted according to the `known_trajectory_out_hit_ratio`. This is an attempt
    to measure trajectory-specific BABIP even in years where trajectory data is rarely
    present for hits. It is still vulnerable to the variance and arbitrariness with which
    historical scorekeepers differentiated line drives from other air outs, which was
    extremely high all the way up to the Statcast era.
{% enddocs %}

{% docs coverage_weighted_pop_up_batting_average %}
    The batting average of batted_balls that were pop-ups,
    weighted according to the `known_trajectory_out_hit_ratio`. This is an attempt
    to measure trajectory-specific BABIP even in years where trajectory data is rarely
    present for hits. It is still vulnerable to the variance and arbitrariness with which
    historical scorekeepers differentiated pop-ups from other air outs.
{% enddocs %}

{% docs known_angle_rate_outs %}
    Rate of batted-ball outs for which we know (or have a good proxy for)
    whether the ball was hit to the left, right, or middle of the field.
{% enddocs %}

{% docs known_angle_rate_hits %}
    Rate of batted-ball hits for which we know (or have a good proxy for)
    whether the ball was hit to the left, right, or middle of the field.
{% enddocs %}

{% docs known_angle_rate %}
    Rate of batted balls for which we know (or have a good proxy for)
    whether the ball was hit to the left, right, or middle of the field.
{% enddocs %}

{% docs known_angle_out_hit_ratio %}
    The ratio of `known_angle_rate_outs` to `known_angle_rate_hits`.
    This ratio is generally very high for years without complete batted ball data,
    which makes it useful for estimating quantities that would be affected by
    the selection bias. Angle is generally better known than location itself, because
    when a batter gets a hit, we often know which outfielder fielded the ball even
    though we don't know how it got there.
{% enddocs %}

{% docs angle_left_rate_outs %}
    The rate of batted-ball outs that were hit to the left side of the field.
{% enddocs %}

{% docs angle_left_rate_hits %}
    The rate of hits that were hit to the left side of the field.
{% enddocs %}

{% docs angle_left_rate %}
    The overall rate of batted balls that were hit to the left side of the field.
{% enddocs %}

{% docs coverage_weighted_angle_left_batting_average %}
    The batting average on batted balls that were hit to the left side of the field,
    weighted according to the `known_angle_out_hit_ratio`. This is an attempt
    to measure angle-specific BABIP even in years where angle data is rarely
    present for hits.
{% enddocs %}

{% docs angle_right_rate_outs %}
    The rate of batted-ball outs that were hit to the right side of the field.
{% enddocs %}

{% docs angle_right_rate_hits %}
    The rate of hits that were hit to the right side of the field.
{% enddocs %}

{% docs angle_right_rate %}
    The overall rate of batted balls that were hit to the right side of the field.
{% enddocs %}

{% docs coverage_weighted_angle_right_batting_average %}
    The batting average on batted balls that were hit to the right side of the field,
    weighted according to the `known_angle_out_hit_ratio`. This is an attempt
    to measure angle-specific BABIP even in years where angle data is rarely
    present for hits.
{% enddocs %}

{% docs angle_middle_rate_outs %}
    The rate of batted-ball outs that were hit to the middle of the field.
{% enddocs %}

{% docs angle_middle_rate_hits %}
    The rate of hits that were hit to the middle of the field.
{% enddocs %}

{% docs angle_middle_rate %}
    The overall rate of batted balls that were hit to the middle of the field.
{% enddocs %}

{% docs coverage_weighted_angle_middle_batting_average %}
    The batting average on batted balls that were hit to the middle of the field,
    weighted according to the `known_angle_out_hit_ratio`. This is an attempt
    to measure angle-specific BABIP even in years where angle data is rarely
    present for hits.
{% enddocs %}

{% docs pulled_rate_outs %}
    `angle_right_rate_outs` for lefty batters
    and `angle_left_rate_outs` for righty batters.
{% enddocs %}

{% docs pulled_rate_hits %}
    `angle_right_rate_hits` for lefty batters
    and `angle_left_rate_hits` for righty batters.
{% enddocs %}

{% docs pulled_rate %}
    `angle_right_rate` for lefty batters
    and `angle_left_rate` for righty batters.
{% enddocs %}

{% docs coverage_weighted_pulled_batting_average %}
    `coverage_weighted_angle_right_batting_average` for lefty batters
    and `coverage_weighted_angle_left_batting_average` for righty batters.
{% enddocs %}

{% docs opposite_field_rate_outs %}
    `angle_left_rate_outs` for lefty batters
    and `angle_right_rate_outs` for righty batters.
{% enddocs %}

{% docs opposite_field_rate_hits %}
    `angle_left_rate_hits` for lefty batters
    and `angle_right_rate_hits` for righty batters.
{% enddocs %}

{% docs opposite_field_rate %}
    `angle_left_rate` for lefty batters
    and `angle_right_rate` for righty batters.
{% enddocs %}

{% docs coverage_weighted_opposite_field_batting_average %}
    `coverage_weighted_angle_left_batting_average` for lefty batters
    and `coverage_weighted_angle_right_batting_average` for righty batters.
{% enddocs %}

{% docs stolen_base_attempt_rate_second %}
    The rate of stolen base opportunities taken
    when the runner was on first base (trying to steal second).
    See `stolen_base_opportunities` for the definition.
{% enddocs %}

{% docs stolen_base_attempt_rate_third %}
    The rate of stolen base opportunities taken
    when the runner was on second base (trying to steal third).
    See `stolen_base_opportunities` for the definition.
{% enddocs %}

{% docs stolen_base_attempt_rate_home %}
    The rate of stolen base opportunities taken
    when the runner was on third base (trying to steal home).
    See `stolen_base_opportunities` for the definition.
{% enddocs %}

{% docs unforced_out_rate %}
    The rate of appearances on the basepaths that ended in an unforced out.
    See `unforced_outs_on_basepaths` for the definition.
{% enddocs %}

{% docs pitch_strike_rate %}
    The rate of pitches that were strikes.
{% enddocs %}

{% docs pitch_contact_rate %}
    The rate of pitches where some kind of contact was made.
{% enddocs %}

{% docs pitch_swing_rate %}
    The rate of pitches where the batter swung (and either made
    contact or missed).
{% enddocs %}

{% docs pitch_ball_rate %}
    The rate of pitches that were balls.
{% enddocs %}

{% docs pitch_swing_and_miss_rate %}
    The rate of pitches where the batter swung and missed.
{% enddocs %}

{% docs pitch_foul_rate %}
    The rate of pitches that were fouled off.
{% enddocs %}

{% docs pitched_called_strike_rate %}
    The rate of pitches that were called strikes.
{% enddocs %}

{% docs pitch_data_coverage_rate %}
    The rate of plate appearances for which we have pitch-by-pitch data.
{% enddocs %}
