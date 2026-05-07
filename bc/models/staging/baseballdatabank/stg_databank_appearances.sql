MODEL (
  name main_models.stg_databank_appearances,
  kind FULL,
  description 'Long-form positional appearances for every (player, season, team, stint). Uses Databank fielding for stinted P/C/1B/2B/3B/SS/OF game counts and appearances.g_lf/g_cf/g_rf for the LF/CF/RF breakdown, with the project''s 1=P, 2=C, 3=1B, 4=2B, 5=3B, 6=SS, 7=LF, 8=CF, 9=RF convention. The OF breakdown comes from appearances rather than fielding_of because fielding_of has no team_id and its stint numbering is not consistent with fielding (browp102 1892 is one example), and because fielding_of has gaps where appearances has the full breakdown. The team-year LF/CF/RF total from appearances is allocated across fielding OF stints in proportion to each stint''s fielding.g; this only differs from a simple broadcast for the small handful of multi-stint same-team OF cases. Joins stg_people to translate databank player_id to retrosheet player_id; rows without a retrosheet id are dropped. g_all from appearances is kept as games_total for optimizer targets because appearances has the full team total but no stint column. outs_played is the player-stint sum of fielding inn_outs across all non-pitcher positions; it is denormalized (same value across rows for the same player-stint) and is null-coalesced to 0 when the source data lacks inn_outs. g_dh is intentionally skipped because every gamelog-only game in scope is pre-1973.',
  grain (player_id, season, team_id, stint, fielding_position),
  columns (
    player_id VARCHAR,
    databank_player_id VARCHAR,
    season SMALLINT,
    stint SMALLINT,
    team_id VARCHAR,
    league_id VARCHAR,
    fielding_position UTINYINT,
    games_at_position USMALLINT,
    games_total USMALLINT,
    outs_played UINTEGER
  ),
  column_descriptions (
    player_id = @doc('player_id'),
    databank_player_id = @doc('databank_player_id'),
    season = @doc('season'),
    stint = @doc('stint'),
    team_id = @doc('team_id'),
    league_id = @doc('league_id')
  ),
  audits (
    not_null(columns := (player_id, season, team_id, stint, fielding_position, games_at_position, games_total, outs_played)),
    unique_grain(columns := (player_id, season, team_id, stint, fielding_position)),
    valid_baseball_season(column := season)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/main_models_stg_databank_appearances.parquet'
  ),
);









WITH games_total AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        lg_id AS league_id,
        g_all AS games_total
    FROM baseballdatabank.appearances
),

fielding_source AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        lg_id AS league_id,
        stint,
        CASE pos
            WHEN 'P' THEN 1
            WHEN 'C' THEN 2
            WHEN '1B' THEN 3
            WHEN '2B' THEN 4
            WHEN '3B' THEN 5
            WHEN 'SS' THEN 6
        END::UTINYINT AS fielding_position,
        g AS games_at_position
    FROM baseballdatabank.fielding
    WHERE pos IN ('P', 'C', '1B', '2B', '3B', 'SS')
),

fielding_outfield AS (
    SELECT
        player_id,
        year_id,
        team_id,
        lg_id,
        stint,
        g AS stint_of_g,
        SUM(g) OVER (PARTITION BY player_id, year_id, team_id) AS team_of_g
    FROM baseballdatabank.fielding
    WHERE pos = 'OF'
),

outfield_source AS (
    SELECT
        fo.player_id AS databank_player_id,
        fo.year_id AS season,
        fo.team_id,
        fo.lg_id AS league_id,
        fo.stint,
        ROUND(a.g_lf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_left_field,
        ROUND(a.g_cf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_center_field,
        ROUND(a.g_rf::DOUBLE * fo.stint_of_g / NULLIF(fo.team_of_g, 0))::INTEGER AS games_right_field
    FROM fielding_outfield AS fo
    INNER JOIN baseballdatabank.appearances AS a
        ON fo.player_id = a.player_id
        AND fo.year_id = a.year_id
        AND fo.team_id = a.team_id
),

outfield_unpivoted AS (
    SELECT * FROM outfield_source
    UNPIVOT (
        games_at_position FOR position_label IN (
            games_left_field,
            games_center_field,
            games_right_field
        )
    )
),

outfield_with_position AS (
    SELECT
        databank_player_id,
        season,
        team_id,
        league_id,
        stint,
        CASE position_label
            WHEN 'games_left_field' THEN 7
            WHEN 'games_center_field' THEN 8
            WHEN 'games_right_field' THEN 9
        END::UTINYINT AS fielding_position,
        games_at_position
    FROM outfield_unpivoted
),

position_games AS (
    SELECT * FROM fielding_source
    UNION ALL
    SELECT * FROM outfield_with_position
),

non_pitcher_outs_played AS (
    SELECT
        player_id AS databank_player_id,
        year_id AS season,
        team_id,
        stint,
        SUM(inn_outs)::UINTEGER AS outs_played
    FROM baseballdatabank.fielding
    WHERE pos != 'P'
      AND inn_outs IS NOT NULL
    GROUP BY 1, 2, 3, 4
),

with_retrosheet_id AS (
    SELECT
        people.retrosheet_player_id AS player_id,
        position_games.databank_player_id,
        position_games.season::SMALLINT AS season,
        position_games.stint::SMALLINT AS stint,
        position_games.team_id,
        COALESCE(games_total.league_id, position_games.league_id) AS league_id,
        position_games.fielding_position,
        position_games.games_at_position::USMALLINT AS games_at_position,
        games_total.games_total::USMALLINT AS games_total,
        COALESCE(outs.outs_played, 0)::UINTEGER AS outs_played
    FROM position_games
    INNER JOIN main_models.stg_people AS people USING (databank_player_id)
    LEFT JOIN games_total USING (databank_player_id, season, team_id)
    LEFT JOIN non_pitcher_outs_played AS outs
        USING (databank_player_id, season, team_id, stint)
    WHERE people.retrosheet_player_id IS NOT NULL
)

SELECT * FROM with_retrosheet_id
WHERE games_at_position IS NOT NULL
  AND games_at_position > 0
  AND games_total IS NOT NULL
