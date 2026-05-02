MODEL (
  name main_models.stg_databank_pitching,
  kind FULL,
  description 'Aggregate pitching statistics by player, season, and stint with a given team.',
  grain (databank_player_id, season, stint),
  columns (
    databank_player_id VARCHAR,
    season SMALLINT,
    stint SMALLINT,
    team_id VARCHAR,
    league_id VARCHAR,
    wins SMALLINT,
    losses SMALLINT,
    games SMALLINT,
    games_started SMALLINT,
    complete_games SMALLINT,
    shutouts SMALLINT,
    saves SMALLINT,
    outs_recorded SMALLINT,
    hits SMALLINT,
    earned_runs SMALLINT,
    home_runs SMALLINT,
    walks SMALLINT,
    strikeouts SMALLINT,
    opponent_batting_average DOUBLE,
    earned_run_average DOUBLE,
    intentional_walks SMALLINT,
    wild_pitches SMALLINT,
    hit_by_pitches SMALLINT,
    balks SMALLINT,
    batters_faced SMALLINT,
    games_finished SMALLINT,
    runs SMALLINT,
    sacrifice_hits SMALLINT,
    sacrifice_flies SMALLINT,
    grounded_into_double_plays SMALLINT
  ),
  column_descriptions (
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    stint = @doc('stint'),
    team_id = @doc('team_id'),
    league_id = @doc('league_id'),
    wins = @doc('wins'),
    losses = @doc('losses'),
    games = @doc('games'),
    games_started = @doc('games_started'),
    complete_games = @doc('complete_games'),
    shutouts = @doc('shutouts'),
    saves = @doc('saves'),
    outs_recorded = @doc('outs_recorded'),
    hits = @doc('hits'),
    earned_runs = @doc('earned_runs'),
    home_runs = @doc('home_runs'),
    walks = @doc('walks'),
    strikeouts = @doc('strikeouts'),
    earned_run_average = @doc('earned_run_average'),
    intentional_walks = @doc('intentional_walks'),
    wild_pitches = @doc('wild_pitches'),
    hit_by_pitches = @doc('hit_by_pitches'),
    balks = @doc('balks'),
    batters_faced = @doc('batters_faced'),
    games_finished = @doc('games_finished'),
    runs = @doc('runs'),
    sacrifice_hits = @doc('sacrifice_hits'),
    sacrifice_flies = @doc('sacrifice_flies'),
    grounded_into_double_plays = @doc('grounded_into_double_plays')
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_pitching.parquet'
  ),
);







WITH source AS (
    SELECT * FROM baseballdatabank.pitching
),

renamed AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        stint,
        team_id AS team_id,
        lg_id AS league_id,
        w AS wins,
        l AS losses,
        g AS games,
        gs AS games_started,
        cg AS complete_games,
        sho AS shutouts,
        sv AS saves,
        ip_outs AS outs_recorded,
        h AS hits,
        er AS earned_runs,
        hr AS home_runs,
        bb AS walks,
        so AS strikeouts,
        -- OAV could be used in theory to back into at-bats against,
        -- but isn't populated for the years we source from this data
        ba_opp AS opponent_batting_average,
        era AS earned_run_average,
        ibb AS intentional_walks,
        wp AS wild_pitches,
        hbp AS hit_by_pitches,
        bk AS balks,
        bfp AS batters_faced,
        gf AS games_finished,
        r AS runs,
        sh AS sacrifice_hits,
        sf AS sacrifice_flies,
        gidp AS grounded_into_double_plays
    FROM source
)

SELECT * from renamed
