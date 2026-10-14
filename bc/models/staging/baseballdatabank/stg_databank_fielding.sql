MODEL (
  name main_models.stg_databank_fielding,
  kind FULL,
  description 'Aggregate fielding statistics by player, season, and stint with a given team. team_id is translated from Databank to Retrosheet through stg_databank_teams. The not_null audit on team_id fails the build if a major-league row is missing from the crosswalk; Negro Leagues rows with no resolved Retrosheet team keep team_id NULL.',
  grain (databank_player_id, season, stint, fielding_position),
  columns (
    databank_player_id VARCHAR,
    season SMALLINT,
    stint SMALLINT,
    team_id VARCHAR,
    league_id VARCHAR,
    is_negro_leagues BOOLEAN,
    games SMALLINT,
    games_started SMALLINT,
    outs_played SMALLINT,
    putouts SMALLINT,
    assists SMALLINT,
    errors SMALLINT,
    double_plays SMALLINT,
    passed_balls SMALLINT,
    wild_pitches SMALLINT,
    stolen_bases SMALLINT,
    caught_stealing SMALLINT,
    fielding_position INTEGER,
    fielding_position_category VARCHAR
  ),
  column_descriptions (
    is_negro_leagues = 'Whether the Lahman league is a Negro Leagues league rather than a major league.',
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    stint = @doc('stint'),
    team_id = @doc('team_id'),
    league_id = @doc('league_id'),
    games = @doc('games'),
    games_started = @doc('games_started'),
    outs_played = @doc('outs_played'),
    putouts = @doc('putouts'),
    assists = @doc('assists'),
    errors = @doc('errors'),
    double_plays = @doc('double_plays'),
    passed_balls = @doc('passed_balls'),
    wild_pitches = @doc('wild_pitches'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    fielding_position = @doc('fielding_position'),
    fielding_position_category = @doc('fielding_position_category')
  ),
  audits (
    not_null(columns := (databank_player_id, season, stint, is_negro_leagues)),
    not_null(columns := (team_id), condition := NOT is_negro_leagues)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_fielding.parquet'
  ),
);







WITH team_id_crosswalk AS (
    SELECT
        season AS year_id,
        databank_team_id,
        team_id,
        is_negro_leagues
    FROM main_models.stg_databank_teams
),

source AS (
    SELECT
        f.* EXCLUDE (team_id),
        t.team_id AS team_id,
        t.is_negro_leagues
    FROM baseballdatabank.fielding AS f
    LEFT JOIN team_id_crosswalk AS t
        ON f.year_id = t.year_id AND f.team_id = t.databank_team_id
),

renamed AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        team_id AS team_id,
        lg_id AS league_id,
        is_negro_leagues,
        g AS games,
        gs AS games_started,
        inn_outs AS outs_played,
        po AS putouts,
        a AS assists,
        e AS errors,
        dp AS double_plays,
        pb AS passed_balls,
        wp AS wild_pitches,
        sb AS stolen_bases,
        cs AS caught_stealing,
        CASE pos
            WHEN 'P' THEN 1
            WHEN 'C' THEN 2
            WHEN '1B' THEN 3
            WHEN '2B' THEN 4
            WHEN '3B' THEN 5
            WHEN 'SS' THEN 6
            ELSE 0 
        END AS fielding_position,
        CASE
            WHEN pos IN ('P', 'C', 'OF') THEN pos
            WHEN pos IN ('1B', '2B', '3B', 'SS') THEN 'IF'
        END AS fielding_position_category
    FROM source
)

SELECT * FROM renamed
