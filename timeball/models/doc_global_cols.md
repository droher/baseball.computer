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
