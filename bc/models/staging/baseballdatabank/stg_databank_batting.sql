MODEL (
  name main_models.stg_databank_batting,
  kind FULL,
  description 'Aggregate batting statistics by player, season, and stint with a given team. team_id is translated from Databank to Retrosheet via baseballdatabank.teams.team_id_retro and the not_null audit on team_id fails the build if any row is missing from the crosswalk.',
  grain (databank_player_id, season, stint),
  columns (
    databank_player_id VARCHAR,
    season SMALLINT,
    stint SMALLINT,
    team_id VARCHAR,
    league_id VARCHAR,
    games USMALLINT,
    at_bats USMALLINT,
    runs USMALLINT,
    hits USMALLINT,
    doubles USMALLINT,
    triples USMALLINT,
    home_runs USMALLINT,
    runs_batted_in USMALLINT,
    stolen_bases USMALLINT,
    caught_stealing USMALLINT,
    walks USMALLINT,
    strikeouts USMALLINT,
    intentional_walks USMALLINT,
    hit_by_pitches USMALLINT,
    sacrifice_hits USMALLINT,
    sacrifice_flies USMALLINT,
    grounded_into_double_plays USMALLINT,
    singles USMALLINT,
    total_bases USMALLINT,
    plate_appearances USMALLINT,
    on_base_opportunities USMALLINT,
    on_base_successes USMALLINT
  ),
  column_descriptions (
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    stint = @doc('stint'),
    team_id = @doc('team_id'),
    league_id = @doc('league_id'),
    games = 'Total number of games played',
    at_bats = @doc('at_bats'),
    runs = @doc('runs'),
    hits = @doc('hits'),
    doubles = @doc('doubles'),
    triples = @doc('triples'),
    home_runs = @doc('home_runs'),
    runs_batted_in = @doc('runs_batted_in'),
    stolen_bases = @doc('stolen_bases'),
    caught_stealing = @doc('caught_stealing'),
    walks = @doc('walks'),
    strikeouts = @doc('strikeouts'),
    intentional_walks = @doc('intentional_walks'),
    hit_by_pitches = @doc('hit_by_pitches'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    grounded_into_double_plays = @doc('grounded_into_double_plays'),
    singles = @doc('singles'),
    total_bases = @doc('total_bases'),
    plate_appearances = @doc('plate_appearances'),
    on_base_opportunities = @doc('on_base_opportunities'),
    on_base_successes = @doc('on_base_successes')
  ),
  audits (
    not_null(columns := (databank_player_id, season, stint, team_id))
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_batting.parquet'
  ),
);







WITH team_id_crosswalk AS (
    SELECT
        year_id,
        team_id AS databank_team_id,
        team_id_retro AS team_id
    FROM baseballdatabank.teams
),

source AS (
    SELECT
        b.* EXCLUDE (team_id),
        t.team_id AS team_id
    FROM baseballdatabank.batting AS b
    LEFT JOIN team_id_crosswalk AS t
        ON b.year_id = t.year_id AND b.team_id = t.databank_team_id
),

renamed AS (

    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        team_id AS team_id,
        lg_id AS league_id,
        g AS games,
        ab AS at_bats,
        r AS runs,
        h AS hits,
        _2b AS doubles, -- noqa: RF06
        _3b AS triples, -- noqa: RF06
        hr AS home_runs,
        rbi AS runs_batted_in,
        sb AS stolen_bases,
        cs AS caught_stealing,
        bb AS walks,
        so AS strikeouts,
        ibb AS intentional_walks,
        hbp AS hit_by_pitches,
        sh AS sacrifice_hits,
        sf AS sacrifice_flies,
        gidp AS grounded_into_double_plays,
        hits - home_runs - triples - doubles AS singles,
        (singles + doubles * 2 + triples * 3 + home_runs * 4)::USMALLINT AS total_bases,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0)
        + COALESCE(sacrifice_hits, 0))::USMALLINT
        AS plate_appearances,
        (at_bats + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0) + COALESCE(sacrifice_flies, 0))::USMALLINT
        AS on_base_opportunities,
        (hits + COALESCE(walks, 0) + COALESCE(hit_by_pitches, 0))::USMALLINT AS on_base_successes,
    FROM source

)

SELECT * FROM renamed
