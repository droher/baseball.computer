MODEL (
  name main_models.stg_databank_appearances,
  kind FULL,
  description 'Long-form positional appearances for every (player, season, team). Pivots the per-position game counts (g_p..g_rf) into one row per (player_id, season, team_id, fielding_position) using the project''s 1=P, 2=C, 3=1B, 4=2B, 5=3B, 6=SS, 7=LF, 8=CF, 9=RF convention. Joins stg_people to translate databank player_id to retrosheet player_id; rows without a retrosheet id are dropped. g_dh is intentionally skipped — every gamelog-only game in scope is pre-1973, so no DH lineups need to be emitted.',
  grain (player_id, season, team_id, fielding_position),
  columns (
    player_id VARCHAR,
    databank_player_id VARCHAR,
    season SMALLINT,
    team_id VARCHAR,
    league_id VARCHAR,
    fielding_position UTINYINT,
    games_at_position USMALLINT
  ),
  column_descriptions (
    player_id = @doc('player_id'),
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    team_id = @doc('team_id'),
    league_id = @doc('league_id')
  ),
  audits (
    not_null(columns := (player_id, season, team_id, fielding_position, games_at_position)),
    unique_grain(columns := (player_id, season, team_id, fielding_position)),
    valid_baseball_season(column := season)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_appearances.parquet'
  ),
);









WITH source AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        lg_id AS league_id,
        g_p, g_c, g_1b, g_2b, g_3b, g_ss, g_lf, g_cf, g_rf
    FROM baseballdatabank.appearances
),

with_retrosheet_id AS (
    SELECT
        people.retrosheet_player_id AS player_id,
        source.databank_player_id,
        source.season::SMALLINT AS season,
        source.team_id,
        source.league_id,
        source.g_p::USMALLINT AS g_p,
        source.g_c::USMALLINT AS g_c,
        source.g_1b::USMALLINT AS g_1b,
        source.g_2b::USMALLINT AS g_2b,
        source.g_3b::USMALLINT AS g_3b,
        source.g_ss::USMALLINT AS g_ss,
        source.g_lf::USMALLINT AS g_lf,
        source.g_cf::USMALLINT AS g_cf,
        source.g_rf::USMALLINT AS g_rf
    FROM source
    INNER JOIN main_models.stg_people AS people USING (databank_player_id)
    WHERE people.retrosheet_player_id IS NOT NULL
),

pivoted AS (
    SELECT * FROM with_retrosheet_id
    UNPIVOT (
        games_at_position FOR position_label IN (
            g_p, g_c, g_1b, g_2b, g_3b, g_ss, g_lf, g_cf, g_rf
        )
    )
),

with_position AS (
    SELECT
        player_id,
        databank_player_id,
        season,
        team_id,
        league_id,
        CASE position_label
            WHEN 'g_p' THEN 1
            WHEN 'g_c' THEN 2
            WHEN 'g_1b' THEN 3
            WHEN 'g_2b' THEN 4
            WHEN 'g_3b' THEN 5
            WHEN 'g_ss' THEN 6
            WHEN 'g_lf' THEN 7
            WHEN 'g_cf' THEN 8
            WHEN 'g_rf' THEN 9
        END::UTINYINT AS fielding_position,
        games_at_position::USMALLINT AS games_at_position
    FROM pivoted
    WHERE games_at_position IS NOT NULL AND games_at_position > 0
)

SELECT * FROM with_position
