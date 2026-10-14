MODEL (
  name main_models.nlb_player_season_pitching_discrepancies,
  kind FULL,
  description 'Lahman and Retrosheet Negro Leagues pitching side by side, one row per season, team, and player either source has. Retrosheet totals count only matched-scope games with a box score (see nlb_game_scopes), the scope Lahman''s lines cover. Lahman rows are matched on the Retrosheet team id from stg_databank_teams and the Retrosheet player id from stg_people; Lahman rows missing either keep it NULL and cannot match. differing_stats lists the paired stats whose values differ.',
  grain (season, team_id, player_id, databank_player_id),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    player_id VARCHAR,
    databank_player_id VARCHAR,
    coverage_category VARCHAR,
    lahman_games INTEGER,
    retrosheet_games INTEGER,
    lahman_games_started INTEGER,
    retrosheet_games_started INTEGER,
    lahman_wins INTEGER,
    retrosheet_wins INTEGER,
    lahman_losses INTEGER,
    retrosheet_losses INTEGER,
    lahman_outs_recorded INTEGER,
    retrosheet_outs_recorded INTEGER,
    lahman_hits INTEGER,
    retrosheet_hits INTEGER,
    lahman_runs INTEGER,
    retrosheet_runs INTEGER,
    lahman_earned_runs INTEGER,
    retrosheet_earned_runs INTEGER,
    lahman_walks INTEGER,
    retrosheet_walks INTEGER,
    lahman_strikeouts INTEGER,
    retrosheet_strikeouts INTEGER,
    lahman_home_runs INTEGER,
    retrosheet_home_runs INTEGER,
    differing_stats VARCHAR[],
    is_equal_outs_with_differing_stats BOOLEAN
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id; NULL for a Lahman row whose team has no resolved Retrosheet id.',
    player_id = 'Retrosheet player id; NULL for a Lahman row whose person has no Retrosheet id.',
    databank_player_id = 'Lahman player id; NULL for Retrosheet-only rows.',
    coverage_category = 'Both, LahmanOnly, or RetrosheetOnly.',
    differing_stats = 'Paired stats whose Lahman and Retrosheet values differ, when the row is in both sources.',
    is_equal_outs_with_differing_stats = 'Both sources have the same nonzero outs recorded but differ on another stat, which points to a disagreement about values rather than coverage.'
  ),
  audits (
    not_null(columns := (season, coverage_category)),
    unique_grain(columns := (season, team_id, player_id, databank_player_id)),
    accepted_values(column := coverage_category, is_in := ('Both', 'LahmanOnly', 'RetrosheetOnly'))
  ),
);

WITH lahman AS (
    SELECT
        pit.season,
        pit.team_id,
        people.retrosheet_player_id AS player_id,
        pit.databank_player_id,
        SUM(pit.games) AS games,
        SUM(pit.games_started) AS games_started,
        SUM(pit.wins) AS wins,
        SUM(pit.losses) AS losses,
        SUM(pit.outs_recorded) AS outs_recorded,
        SUM(pit.hits) AS hits,
        SUM(pit.runs) AS runs,
        SUM(pit.earned_runs) AS earned_runs,
        SUM(pit.walks) AS walks,
        SUM(pit.strikeouts) AS strikeouts,
        SUM(pit.home_runs) AS home_runs
    FROM main_models.stg_databank_pitching AS pit
    LEFT JOIN main_models.stg_people AS people USING (databank_player_id)
    WHERE pit.is_negro_leagues
    GROUP BY 1, 2, 3, 4
),

retrosheet AS (
    SELECT
        scopes.season,
        pit.team_id,
        pit.player_id,
        COUNT(DISTINCT pit.game_id) AS games,
        SUM(pit.p_gs) AS games_started,
        COUNT_IF(pit.is_winning_pitcher) AS wins,
        COUNT_IF(pit.is_losing_pitcher) AS losses,
        SUM(pit.p_ipouts) AS outs_recorded,
        SUM(pit.p_h) AS hits,
        SUM(pit.p_r) AS runs,
        SUM(pit.p_er) AS earned_runs,
        SUM(pit.p_w) AS walks,
        SUM(pit.p_k) AS strikeouts,
        SUM(pit.p_hr) AS home_runs
    FROM main_models.stg_nlb_pitching AS pit
    INNER JOIN main_models.nlb_game_scopes AS scopes USING (game_id)
    WHERE scopes.is_matched_scope AND scopes.has_box_score
    GROUP BY 1, 2, 3
),

joined AS (
    SELECT
        COALESCE(lahman.season, retrosheet.season) AS season,
        COALESCE(lahman.team_id, retrosheet.team_id) AS team_id,
        COALESCE(lahman.player_id, retrosheet.player_id) AS player_id,
        lahman.databank_player_id,
        CASE
            WHEN lahman.season IS NOT NULL AND retrosheet.season IS NOT NULL THEN 'Both'
            WHEN lahman.season IS NOT NULL THEN 'LahmanOnly'
            ELSE 'RetrosheetOnly'
        END AS coverage_category,
        lahman.games::INTEGER AS lahman_games,
        lahman.games_started::INTEGER AS lahman_games_started,
        lahman.wins::INTEGER AS lahman_wins,
        lahman.losses::INTEGER AS lahman_losses,
        lahman.outs_recorded::INTEGER AS lahman_outs_recorded,
        lahman.hits::INTEGER AS lahman_hits,
        lahman.runs::INTEGER AS lahman_runs,
        lahman.earned_runs::INTEGER AS lahman_earned_runs,
        lahman.walks::INTEGER AS lahman_walks,
        lahman.strikeouts::INTEGER AS lahman_strikeouts,
        lahman.home_runs::INTEGER AS lahman_home_runs,
        retrosheet.games::INTEGER AS retrosheet_games,
        retrosheet.games_started::INTEGER AS retrosheet_games_started,
        retrosheet.wins::INTEGER AS retrosheet_wins,
        retrosheet.losses::INTEGER AS retrosheet_losses,
        retrosheet.outs_recorded::INTEGER AS retrosheet_outs_recorded,
        retrosheet.hits::INTEGER AS retrosheet_hits,
        retrosheet.runs::INTEGER AS retrosheet_runs,
        retrosheet.earned_runs::INTEGER AS retrosheet_earned_runs,
        retrosheet.walks::INTEGER AS retrosheet_walks,
        retrosheet.strikeouts::INTEGER AS retrosheet_strikeouts,
        retrosheet.home_runs::INTEGER AS retrosheet_home_runs
    FROM lahman
    FULL OUTER JOIN retrosheet
        ON lahman.season = retrosheet.season
            AND lahman.team_id = retrosheet.team_id
            AND lahman.player_id = retrosheet.player_id
),

final AS (
    SELECT
        season,
        team_id,
        player_id,
        databank_player_id,
        coverage_category,
        lahman_games,
        retrosheet_games,
        lahman_games_started,
        retrosheet_games_started,
        lahman_wins,
        retrosheet_wins,
        lahman_losses,
        retrosheet_losses,
        lahman_outs_recorded,
        retrosheet_outs_recorded,
        lahman_hits,
        retrosheet_hits,
        lahman_runs,
        retrosheet_runs,
        lahman_earned_runs,
        retrosheet_earned_runs,
        lahman_walks,
        retrosheet_walks,
        lahman_strikeouts,
        retrosheet_strikeouts,
        lahman_home_runs,
        retrosheet_home_runs,
        CASE WHEN coverage_category = 'Both' THEN
            LIST_FILTER(
                [
                    CASE WHEN lahman_games IS DISTINCT FROM retrosheet_games THEN 'games' END,
                    CASE WHEN lahman_games_started IS DISTINCT FROM retrosheet_games_started THEN 'games_started' END,
                    CASE WHEN lahman_wins IS DISTINCT FROM retrosheet_wins THEN 'wins' END,
                    CASE WHEN lahman_losses IS DISTINCT FROM retrosheet_losses THEN 'losses' END,
                    CASE WHEN lahman_outs_recorded IS DISTINCT FROM retrosheet_outs_recorded THEN 'outs_recorded' END,
                    CASE WHEN lahman_hits IS DISTINCT FROM retrosheet_hits THEN 'hits' END,
                    CASE WHEN lahman_runs IS DISTINCT FROM retrosheet_runs THEN 'runs' END,
                    CASE WHEN lahman_earned_runs IS DISTINCT FROM retrosheet_earned_runs THEN 'earned_runs' END,
                    CASE WHEN lahman_walks IS DISTINCT FROM retrosheet_walks THEN 'walks' END,
                    CASE WHEN lahman_strikeouts IS DISTINCT FROM retrosheet_strikeouts THEN 'strikeouts' END,
                    CASE WHEN lahman_home_runs IS DISTINCT FROM retrosheet_home_runs THEN 'home_runs' END
                ],
                x -> x IS NOT NULL
            )
        END AS differing_stats,
        COALESCE(
            coverage_category = 'Both'
            AND lahman_outs_recorded > 0
            AND lahman_outs_recorded = retrosheet_outs_recorded
            AND LEN(LIST_FILTER(differing_stats, x -> x NOT IN ('games', 'outs_recorded'))) > 0,
            FALSE
        ) AS is_equal_outs_with_differing_stats
    FROM joined
)

SELECT * FROM final
