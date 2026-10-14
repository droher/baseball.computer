MODEL (
  name main_models.nlb_player_season_batting_discrepancies,
  kind FULL,
  description 'Lahman and Retrosheet Negro Leagues batting side by side, one row per season, team, and player either source has. Retrosheet totals count only matched-scope games with a box score (see nlb_game_scopes), the scope Lahman''s lines cover. Lahman rows are matched on the Retrosheet team id from stg_databank_teams and the Retrosheet player id from stg_people; Lahman rows missing either keep it NULL and cannot match (nlb_lahman_player_resolution and nlb_lahman_team_resolution propose matches). differing_stats lists the paired stats whose values differ.',
  grain (season, team_id, player_id, databank_player_id),
  columns (
    season SMALLINT,
    team_id VARCHAR,
    player_id VARCHAR,
    databank_player_id VARCHAR,
    coverage_category VARCHAR,
    lahman_games INTEGER,
    retrosheet_games INTEGER,
    lahman_at_bats INTEGER,
    retrosheet_at_bats INTEGER,
    lahman_runs INTEGER,
    retrosheet_runs INTEGER,
    lahman_hits INTEGER,
    retrosheet_hits INTEGER,
    lahman_doubles INTEGER,
    retrosheet_doubles INTEGER,
    lahman_triples INTEGER,
    retrosheet_triples INTEGER,
    lahman_home_runs INTEGER,
    retrosheet_home_runs INTEGER,
    lahman_runs_batted_in INTEGER,
    retrosheet_runs_batted_in INTEGER,
    lahman_walks INTEGER,
    retrosheet_walks INTEGER,
    lahman_strikeouts INTEGER,
    retrosheet_strikeouts INTEGER,
    lahman_stolen_bases INTEGER,
    retrosheet_stolen_bases INTEGER,
    differing_stats VARCHAR[],
    is_equal_at_bats_with_differing_stats BOOLEAN
  ),
  column_descriptions (
    season = @doc('season'),
    team_id = 'Retrosheet team id; NULL for a Lahman row whose team has no resolved Retrosheet id.',
    player_id = 'Retrosheet player id; NULL for a Lahman row whose person has no Retrosheet id.',
    databank_player_id = 'Lahman player id; NULL for Retrosheet-only rows.',
    coverage_category = 'Both, LahmanOnly, or RetrosheetOnly.',
    differing_stats = 'Paired stats whose Lahman and Retrosheet values differ, when the row is in both sources.',
    is_equal_at_bats_with_differing_stats = 'Both sources have the same nonzero at-bats but differ on another stat, which points to a disagreement about values rather than coverage.'
  ),
  audits (
    not_null(columns := (season, coverage_category)),
    unique_grain(columns := (season, team_id, player_id, databank_player_id)),
    accepted_values(column := coverage_category, is_in := ('Both', 'LahmanOnly', 'RetrosheetOnly'))
  ),
);

WITH lahman AS (
    SELECT
        bat.season,
        bat.team_id,
        people.retrosheet_player_id AS player_id,
        bat.databank_player_id,
        SUM(bat.games) AS games,
        SUM(bat.at_bats) AS at_bats,
        SUM(bat.runs) AS runs,
        SUM(bat.hits) AS hits,
        SUM(bat.doubles) AS doubles,
        SUM(bat.triples) AS triples,
        SUM(bat.home_runs) AS home_runs,
        SUM(bat.runs_batted_in) AS runs_batted_in,
        SUM(bat.walks) AS walks,
        SUM(bat.strikeouts) AS strikeouts,
        SUM(bat.stolen_bases) AS stolen_bases
    FROM main_models.stg_databank_batting AS bat
    LEFT JOIN main_models.stg_people AS people USING (databank_player_id)
    WHERE bat.is_negro_leagues
    GROUP BY 1, 2, 3, 4
),

retrosheet AS (
    SELECT
        scopes.season,
        bat.team_id,
        bat.player_id,
        COUNT(DISTINCT bat.game_id) AS games,
        SUM(bat.b_ab) AS at_bats,
        SUM(bat.b_r) AS runs,
        SUM(bat.b_h) AS hits,
        SUM(bat.b_d) AS doubles,
        SUM(bat.b_t) AS triples,
        SUM(bat.b_hr) AS home_runs,
        SUM(bat.b_rbi) AS runs_batted_in,
        SUM(bat.b_w) AS walks,
        SUM(bat.b_k) AS strikeouts,
        SUM(bat.b_sb) AS stolen_bases
    FROM main_models.stg_nlb_batting AS bat
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
        lahman.at_bats::INTEGER AS lahman_at_bats,
        lahman.runs::INTEGER AS lahman_runs,
        lahman.hits::INTEGER AS lahman_hits,
        lahman.doubles::INTEGER AS lahman_doubles,
        lahman.triples::INTEGER AS lahman_triples,
        lahman.home_runs::INTEGER AS lahman_home_runs,
        lahman.runs_batted_in::INTEGER AS lahman_runs_batted_in,
        lahman.walks::INTEGER AS lahman_walks,
        lahman.strikeouts::INTEGER AS lahman_strikeouts,
        lahman.stolen_bases::INTEGER AS lahman_stolen_bases,
        retrosheet.games::INTEGER AS retrosheet_games,
        retrosheet.at_bats::INTEGER AS retrosheet_at_bats,
        retrosheet.runs::INTEGER AS retrosheet_runs,
        retrosheet.hits::INTEGER AS retrosheet_hits,
        retrosheet.doubles::INTEGER AS retrosheet_doubles,
        retrosheet.triples::INTEGER AS retrosheet_triples,
        retrosheet.home_runs::INTEGER AS retrosheet_home_runs,
        retrosheet.runs_batted_in::INTEGER AS retrosheet_runs_batted_in,
        retrosheet.walks::INTEGER AS retrosheet_walks,
        retrosheet.strikeouts::INTEGER AS retrosheet_strikeouts,
        retrosheet.stolen_bases::INTEGER AS retrosheet_stolen_bases
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
        lahman_at_bats,
        retrosheet_at_bats,
        lahman_runs,
        retrosheet_runs,
        lahman_hits,
        retrosheet_hits,
        lahman_doubles,
        retrosheet_doubles,
        lahman_triples,
        retrosheet_triples,
        lahman_home_runs,
        retrosheet_home_runs,
        lahman_runs_batted_in,
        retrosheet_runs_batted_in,
        lahman_walks,
        retrosheet_walks,
        lahman_strikeouts,
        retrosheet_strikeouts,
        lahman_stolen_bases,
        retrosheet_stolen_bases,
        CASE WHEN coverage_category = 'Both' THEN
            LIST_FILTER(
                [
                    CASE WHEN lahman_games IS DISTINCT FROM retrosheet_games THEN 'games' END,
                    CASE WHEN lahman_at_bats IS DISTINCT FROM retrosheet_at_bats THEN 'at_bats' END,
                    CASE WHEN lahman_runs IS DISTINCT FROM retrosheet_runs THEN 'runs' END,
                    CASE WHEN lahman_hits IS DISTINCT FROM retrosheet_hits THEN 'hits' END,
                    CASE WHEN lahman_doubles IS DISTINCT FROM retrosheet_doubles THEN 'doubles' END,
                    CASE WHEN lahman_triples IS DISTINCT FROM retrosheet_triples THEN 'triples' END,
                    CASE WHEN lahman_home_runs IS DISTINCT FROM retrosheet_home_runs THEN 'home_runs' END,
                    CASE WHEN lahman_runs_batted_in IS DISTINCT FROM retrosheet_runs_batted_in THEN 'runs_batted_in' END,
                    CASE WHEN lahman_walks IS DISTINCT FROM retrosheet_walks THEN 'walks' END,
                    CASE WHEN lahman_strikeouts IS DISTINCT FROM retrosheet_strikeouts THEN 'strikeouts' END,
                    CASE WHEN lahman_stolen_bases IS DISTINCT FROM retrosheet_stolen_bases THEN 'stolen_bases' END
                ],
                x -> x IS NOT NULL
            )
        END AS differing_stats,
        COALESCE(
            coverage_category = 'Both'
            AND lahman_at_bats > 0
            AND lahman_at_bats = retrosheet_at_bats
            AND LEN(LIST_FILTER(differing_stats, x -> x NOT IN ('games', 'at_bats'))) > 0,
            FALSE
        ) AS is_equal_at_bats_with_differing_stats
    FROM joined
)

SELECT * FROM final
