{% docs game_id %}
    Retrosheet game ID, A 12-character string structured as follows:

    - Chars 1-3: Home team ID
    - Chars 4-11: Date (YYYYMMDD)
    - Char 12: Game number (0 if only one game was played on that day)
    
    Example: MLN195607170
{% enddocs %}

{% docs season %}
    Year, 4-digit integer
{% enddocs %}

{% docs date %}
    Date on which the entity took place.
{% enddocs %}

{% docs event_key %}
    4-byte integer that uniquely identifies an event across the entire
    dataset (contrast with event_id, which is game-specific). The key
    is generated deterministically given the same dataset, but it may
    change when the data is updated. It is guaranteed to follow the 
    order of events within a single game.
{% enddocs %}

{% docs event_id %}
    1-byte integer that uniquely identifies, in order, an event within a single game.
    This value will remain the same across multiple versions as long as events
    are not added or removed from that specific game, and should align exactly
    with the event_id produced by Retrosheet's BEVENT software.
{% enddocs %}

{% docs team_id %}
    Retrosheet team ID, 3-character string which identifies the team
    that a franchise was playing as at a given time.
{% enddocs %}

{% docs away_team_id %}
    Retrosheet team ID, 3-character string which identifies the team
    that a franchise was playing as at a given time.
{% enddocs %}

{% docs home_team_id %}
    Retrosheet team ID, 3-character string which identifies the team
    that a franchise was playing as at a given time.
{% enddocs %}

{% docs person_id %}
    Retrosheet 8-character person ID which consists of
    the first four characters of the last name, the first character
    of the first name, and then three digits to disambiguate between
    players with the same five characters. The first of the three digits
    designates the type of person (in their main role in MLB):
    - 0 and 1: player
    - 7: scorekeeper (not present in file but used in `scorer` field for newer games)
    - 8: coach
    - 9: umpire
{% enddocs %}


{% docs player_id %}
    Retrosheet 8-character person ID for a player, which consists of
    the first four characters of the last name, the first character
    of the first name, and then three digits to disambiguate between
    players with the same five characters.
{% enddocs %}

{% docs batter_id %}
    Retrosheet 8-character person ID for the batter associated with the
    entity.
{% enddocs %}

{% docs pitcher_id %}
    Retrosheet 8-character person ID for the pitcher associated with the
    entity.
{% enddocs %}

{% docs runner_id %}
    Retrosheet 8-character person ID for the baserunner associated with the
    entity.
{% enddocs %}

{% docs databank_player_id %}
    Baseball Databank player ID, which is different from Retrosheet's.
    This ID must be used to join player data from the Baseball Databank
    sources.
{% enddocs %}

{% docs league %}
    Abbreviation for the league associated with this entity. NULL
    indicates that a team is not part of a league (e.g. all-star games
    or Negro League barnstorming teams).
{% enddocs %}

{% docs park_id %}
    Retrosheet park ID, 3-character string which identifies the ballpark
    associated with this entity. See `stg_parks` for full park info.
{% enddocs %}

{% docs game_type %}
    The context in which a game took place, such as the regular season,
    a specific playoff round, or an all-star game.
{% enddocs %}

{% docs side %}
    The side, Home or Away, associated with the entity. A generic way
    of identifying the two teams playing in a game.
{% enddocs %}

{% docs batting_side %}
    The side that was batting when the entity took place.
{% enddocs %}

{% docs bat_first_side %}
    The side that batted in the top of the inning during the game.
    This is almost always the Away side, but there are a good number of
    home-team-bats-first games in the 19th century and 2020.
    See this SABR article for historical context: https://sabr.org/journal/article/the-death-and-rebirth-of-the-home-team-batting-first/
{% enddocs %}

{% docs fielding_position %}
    A 1-byte integer for the traditional 1-9 fielder identification, along with additional
    values for unknown fielders and batting-specific positions. Depending on the
    context, this value may be NULL for batting positions.
    ```
    0 - Unknown
    1 - Pitcher
    2 - Catcher
    3 - First baseman
    4 - Second baseman
    5 - Third baseman
    6 - Shortstop
    7 - Left fielder
    8 - Center fielder
    9 - Right fielder
    10 - Designated Hitter
    11 - Pinch Hitter
    12 - Pinch Runner
    ```
{% enddocs %}

{% docs fielding_position_category %}
    Divided between P, C, IF, OF, and DH.
    This is particularly important for 19th century data, where we don't know
    which specific outfield position a player accumulated his stats at.
{% enddocs %}

{% docs lineup_position %}
    1-byte integer for the traditional 1-9 batting order position, along with
    10 for the pitcher in games with a DH. With very few exceptions, a player will
    only appear in one lineup position per game. The exceptions are from courtesy
    runners and the Ohtani rule.
{% enddocs %}

{% docs inning %}
    The inning during which the entity took place.
{% enddocs %}

{% docs frame %}
    Top or bottom of the inning.
{% enddocs %}

{% docs frame_start %}
    Top or bottom of the inning at the start of the play.
{% enddocs %}

{% docs frame_end %}
    Top or bottom of the inning at the end of the play.
{% enddocs %}

{% docs time_of_day %}
    A day game/night game indicator for the entity. This value
    is close to 100% populated for the pre-night-baseball era and from 1949 on,
    and is still well-populated for the years in between. By far the most reliably
    populated data point for game environmental conditions.
{% enddocs %}

{% docs start_time %}
    The specific date and time when the game started (local time).
    Data here is generally pretty spotty until the 2000s.
{% enddocs %}

{% docs attendance %}
    The number of people in attendance at the game.
    The source for this number is not consistent over time
    and may refer to paid attendance or some other way
    of estimating the number. Attendance data is is very well-populated
    historically, but there is a zero-or-missing ambiguity in Retrosheet
    data. At the moment, we NULL out 0 attendance for all years
    other than 2020, which contains almost all of the true-zero-attendance
    games in history.
{% enddocs %}

{% docs sky %}
    Enum describing the sky conditions.
{% enddocs %}

{% docs precipitation %}
    Enum describing precipitation level.
{% enddocs %}

{% docs field_condition %}
    Enum describing the field conditions,
    particularly with respect to wetness. This
    can be different from precipitation if, for
    example, there was rain before the game.
{% enddocs %}

{% docs wind_direction %}
    Enum describing wind direction in terms
    of one part of the field to another.
{% enddocs %}

{% docs wind_speed_mph %}
    Wind speed in miles per hour.
{% enddocs %}

{% docs temperature_fahrenheit %}
    Temperature in degrees Fahrenheit.    
{% enddocs %}

{% docs source_type %}
    Broad categorization of the source of the data.
    - PlayByPlay: Retrosheet play-by-play files (either from an account or deduced)
    - BoxScore: Retrosheet box score files
    - GameLog: Retrosheet game log files
{% enddocs %}

{% docs filename %}
    Name of the Retrosheet file that this data is derived from.
    Helpful for debugging.
{% enddocs %}

{% docs umpire_home_id %}
    Retrosheet person ID of the umpire at home plate.
{% enddocs %}

{% docs umpire_first_id %}
    Retrosheet person ID of the umpire at first base.
{% enddocs %}

{% docs umpire_second_id %}
    Retrosheet person ID of the umpire at second base.
{% enddocs %}

{% docs umpire_third_id %}
    Retrosheet person ID of the umpire at third base.
{% enddocs %}

{% docs umpire_left_id %}
    Retrosheet person ID of the umpire in left field.
{% enddocs %}

{% docs umpire_right_id %}
    Retrosheet person ID of the umpire in right field.
{% enddocs %}

{% docs sequence_id %}
    The nth item of a given sequence, used to differentiate and order
    multiple items of the same type within a single entity.
{% enddocs %}

{% docs comment %}
    Comment string from a Retrosheet file. Sometimes these are misc.
    details and trivia from the game, but they are also used as adhoc
    data structures for things like ejections.
{% enddocs %}

{% docs baserunner %}
    Enum indicating the specific baserunner associated with the entity.
    Baserunner is designated by the state at the start of an event.
{% enddocs %}

{% docs batted_to_fielder %}
    The fielder who fielded the batted ball as a 1-byte integer, if applicable. This field is
    *always* populated for plate appearances that end with a ball in play,
    regardless of whether we know the fielder. It is filled with 0
    in those cases where no fielder was recorded. On in-play outs,
    this field has excellent historical coverage going back as far as we have data.
    Hits have much spottier coverage, but there is still a large amount of data across
    all years. For most of baseball history, this ends up being our best proxy
    for batted ball location and trajectory.
{% enddocs %}

{% docs outs_on_play %}
    The total number of outs that were recorded during the event.
    Mutually exclusive with all other events.
{% enddocs %}

{% docs runs_on_play %}
    The total number of runs that scored during the event.
    Mutually exclusive with all other events.
{% enddocs %}

{% docs batted_location_general %}
    Timeball's parser interprets Retrosheet's hit location codes as having
    a general location followed by modifiers that add specificity. This field
    contains the general location, which correspond to each fielding position
    as well as the gaps between each position, e.g. Third and ThirdShortstop.

    Note that the for air balls, the location is defined according to where the
    ball was fielded or landed. For ground balls, the location is defined according
    to where the ball was fielded by an infielder, or where the ball left the infield
    if it made it through to the outfield.
{% enddocs %}

{% docs batted_location_depth %}
    Refers to the depth of the batted ball within the area of the general location
    (as opposed to the depth of the general location itself). The data here can
    be difficult to interpret, as 'Default' can either mean the ball was hit to the
    medium depth of the general location, or that the detaial was not recorded.
{% enddocs %}

{% docs batted_location_angle %}
    Refers to the angle of the batted ball within the area of the general location,
    either towards the foul line or the middle of the field. Only a few of the
    general locations have this modifier. The data here can
    be difficult to interpret, as 'Default' can either mean the ball was hit to the
    middle of the general location, or that the detaial was not recorded.
{% enddocs %}

{% docs batted_contact_strength %}
    A modifier indicating that the ball was sharply or softly hit. Prior to the Statcast era,
    this detail is rarely specified and does not have any standard definition.
    In the Statcast era, hard-hit balls are explicitly defined as 95+ miles per hour
    exit velocity, and soft-hit balls are explicitly defined as 59- miles per hour.
{% enddocs %}

{% docs plate_appearance_result %}
    This field is present for all events in which a plate appearance finished,
    and absent for all other types of events.
    It is an enum describing the result of the plate appearance, e.g. Single,
    InPlayOut, etc. See `seed_plate_appearance_results` for more info.
{% enddocs %}


{% docs charge_event_id %}
    The event ID that this baserunner is charged to,
    for the purpose of keeping tracked of inherited/bequeathed runners.
    The pitcher/catcher present at the charge_event_id are on the hook
    for the earned run. This doesn't necessarily remain constant
    throughout a baserunning apperance, as force-outs can change
    the inheritance of a particular runner.
{% enddocs %}

{% docs reached_on_event_id %}
    Event id on which this baserunner originally reached base,
    if applicable.
{% enddocs %}

{% docs explicit_charged_pitcher_id %}
    For some games prior to the establishment of official inherited runner rules,
    a pitcher could be explicitly noted as the one charged with a runner. When
    this is present, it overrides the pitcher from `charge_event_id`.
{% enddocs %}

{% docs attempted_advance_to_base %}
    For some games prior to the establishment of official inherited runner rules,
    a pitcher could be explicitly noted as the one charged with a runner. When
    this is present, it overrides the pitcher from `charge_event_id`.
{% enddocs %}

{% docs charge_event_key %}
    `event_key` corresponding to `charge_event_id`, see that field
    for more detail.
{% enddocs %}

{% docs reached_on_event_key %}
    `event_key` corresponding to `reached_on_event_id`, see that field
    for more detail.
{% enddocs %}

{% docs inning_start %}
    The inning at the start of the event.
{% enddocs %}

{% docs inning_end %}
    The inning at the end of the event.
{% enddocs %}

{% docs inning_in_outs_start %}
    The inning at the start of the event, expressed in outs.
    ((Inning * 3 - 1) + outs in current innning)
    For example, 2 outs in the second inning would be 5.
{% enddocs %}

{% docs outs_start %}
    The number of outs at the start of the event.
{% enddocs %}

{% docs outs_end %}
    The number of outs at the end of the event.
{% enddocs %}

{% docs is_gidp_eligible %}
    Whether or not the event started with a runner on first and less than two outs.
{% enddocs %}

{% docs base_state_start %}
    The base state at the start of the event, a base-10 representation of a binary number. See `base_state` for more info.
{% enddocs %}

{% docs runner_first_id_start %}
    The Retrosheet person ID of the runner on first base at the start of the event.
{% enddocs %}

{% docs runner_second_id_start %}
    The Retrosheet person ID of the runner on second base at the start of the event.
{% enddocs %}

{% docs runner_third_id_start %}
    The Retrosheet person ID of the runner on third base at the start of the event.
{% enddocs %}

{% docs runners_count_start %}
    The number of runners on base at the start of the event.
{% enddocs %}

{% docs base_state_end %}
    The base state at the end of the event, a base-10 representation of a binary number. See `base_state` for more info.
{% enddocs %}

{% docs runner_first_id_end %}
    The Retrosheet person ID of the runner on first base at the end of the event.
{% enddocs %}

{% docs runner_second_id_end %}
    The Retrosheet person ID of the runner on second base at the end of the event.
{% enddocs %}

{% docs runner_third_id_end %}
    The Retrosheet person ID of the runner on third base at the end of the event.
{% enddocs %}

{% docs runners_count_end %}
    The number of runners on base at the end of the event.
{% enddocs %}

{% docs score_home_start %}
    The home team's score at the start of the event.
{% enddocs %}

{% docs score_away_start %}
    The away team's score at the start of the event.
{% enddocs %}

{% docs score_home_end %}
    The home team's score at the end of the event.
{% enddocs %}

{% docs score_away_end %}
    The away team's score at the end of the event.
{% enddocs %}

{% docs frame_start_flag %}
    Whether the event was the first event of the half-inning.
{% enddocs %}

{% docs frame_end_flag %}
    Whether the event was the last event of the half-inning.
{% enddocs %}

{% docs truncated_frame_flag %}
    Whether the event was the last of the half-inning, but there were fewer
    than three outs recorded, e.g. a walk-off home run or a rain-shortened-game.
{% enddocs %}

{% docs game_start_flag %}
    Whether the event was the first event of the game.
{% enddocs %}

{% docs game_end_flag %}
    Whether the event was the last event of the game.
{% enddocs %}

{% docs home_starting_pitcher_id %}
    The Retrosheet person ID of the starting pitcher for the home team in this game.
{% enddocs %}

{% docs away_starting_pitcher_id %}
    The Retrosheet person ID of the starting pitcher for the away team in this game.
{% enddocs %}

{% docs is_regular_season %}
    Indicates whether the game is a regular season game.
{% enddocs %}

{% docs is_postseason %}
    Indicates whether the game is a postseason game.
{% enddocs %}

{% docs away_franchise_id %}
    The Retrosheet franchise ID of the away team in this game.
    Franchise ID connects team_ids that are associated with the same franchise
    over time. See `seed_franchises` for more info.
{% enddocs %}

{% docs home_franchise_id %}
    The Retrosheet franchise ID of the home team in this game.
    Franchise ID connects team_ids that are associated with the same franchise
    over time. See `seed_franchises` for more info.
{% enddocs %}

{% docs away_league %}
    The league of the away team in this game.
{% enddocs %}

{% docs home_league %}
    The league of the home team in this game.
{% enddocs %}

{% docs away_division %}
    The division of the away team in this game. Null if the team is not associated
    with a divsion (e.g. any game prior to 1969).
{% enddocs %}

{% docs home_division %}
    The division of the home team in this game. Null if the team is not associated
    with a divsion (e.g. any game prior to 1969).
{% enddocs %}

{% docs away_team_name %}
    The name of the away team in this game (city and nickname).
{% enddocs %}

{% docs home_team_name %}
    The name of the home team in this game (city and nickname).
{% enddocs %}

{% docs is_interleague %}
    Indicates whether the game is an interleague game. Defined as
    a game played in any context between two teams who are defined at the time
    as being in different leagues (if one of the teams is not in a league, this is false).
{% enddocs %}

{% docs lineup_map_away %}
    A dictionary that maps starting lineup positions (1-10) to player IDs for the away team.
{% enddocs %}

{% docs lineup_map_home %}
    A dictionary that maps starting lineup positions (1-10) to player IDs for the home team.
{% enddocs %}

{% docs fielding_map_away %}
    A dictionary that maps starting fielding positions (1-10) to player IDs for the away team.
{% enddocs %}

{% docs fielding_map_home %}
    A dictionary that maps starting fielding positions (1-10) to player IDs for the home team.
{% enddocs %}
