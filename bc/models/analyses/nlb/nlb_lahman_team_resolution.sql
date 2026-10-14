MODEL (
  name main_models.nlb_lahman_team_resolution,
  kind FULL,
  description 'How each Lahman Negro Leagues team-season maps to a Retrosheet team, one row per Lahman team-season, checked against roster overlap. The roster check takes the Lahman players with a Retrosheet id (from stg_people) and finds the Retrosheet Negro Leagues team in that season (from stg_nlb_players) that shares the most of them. Status: confirmed (the resolved id is the best roster match), override (the same, through a seed_nlb_lahman_team_crosswalk row), crosswalk_disagrees_with_rosters (another team shares more players), unmapped (no resolved Retrosheet id), or no_retrosheet_games (the resolved team has no Retrosheet roster that season, or no linked Lahman player appears for any Retrosheet team that season).',
  grain (season, databank_team_id),
  columns (
    season SMALLINT,
    databank_team_id VARCHAR,
    team_name VARCHAR,
    league_id VARCHAR,
    team_id_retro VARCHAR,
    crosswalk_team_id VARCHAR,
    team_id VARCHAR,
    team_id_in_seed_franchises BOOLEAN,
    lahman_players UINTEGER,
    linked_lahman_players UINTEGER,
    best_roster_team_id VARCHAR,
    best_roster_shared_players UINTEGER,
    best_roster_overlap_share DOUBLE,
    resolved_team_shared_players UINTEGER,
    resolution_status VARCHAR
  ),
  column_descriptions (
    season = @doc('season'),
    databank_team_id = 'Lahman teams.team_id.',
    team_name = 'Lahman team name.',
    league_id = 'Lahman lg_id.',
    team_id_retro = 'Lahman''s Retrosheet team id.',
    crosswalk_team_id = 'Team id from seed_nlb_lahman_team_crosswalk, if any.',
    team_id = 'Retrosheet team id the Lahman staging models use; NULL when unmapped.',
    team_id_in_seed_franchises = 'Whether the crosswalk or team_id_retro id is in seed_franchises.',
    lahman_players = 'Lahman people with a batting or pitching row for the team-season.',
    linked_lahman_players = 'Of those, the ones with a Retrosheet id.',
    best_roster_team_id = 'Retrosheet team that season sharing the most linked players.',
    best_roster_shared_players = 'Linked players shared with best_roster_team_id.',
    best_roster_overlap_share = 'best_roster_shared_players / linked_lahman_players.',
    resolved_team_shared_players = 'Linked players shared with the resolved team_id.',
    resolution_status = 'confirmed, override, crosswalk_disagrees_with_rosters, unmapped, or no_retrosheet_games.'
  ),
  audits (
    not_null(columns := (season, databank_team_id, resolution_status)),
    unique_grain(columns := (season, databank_team_id)),
    accepted_values(column := resolution_status, is_in := ('confirmed', 'override', 'crosswalk_disagrees_with_rosters', 'unmapped', 'no_retrosheet_games'))
  ),
);

WITH franchise_ids AS (
    SELECT DISTINCT team_id FROM main_seeds.seed_franchises
),

lahman_rosters AS (
    SELECT DISTINCT season, databank_team_id, databank_player_id
    FROM main_models.stg_databank_batting
    WHERE is_negro_leagues
    UNION
    SELECT DISTINCT season, databank_team_id, databank_player_id
    FROM main_models.stg_databank_pitching
    WHERE is_negro_leagues
),

linked_rosters AS (
    SELECT
        rosters.season,
        rosters.databank_team_id,
        rosters.databank_player_id,
        people.retrosheet_player_id AS player_id
    FROM lahman_rosters AS rosters
    LEFT JOIN main_models.stg_people AS people USING (databank_player_id)
),

retrosheet_rosters AS (
    SELECT DISTINCT season::SMALLINT AS season, team_id, player_id
    FROM main_models.stg_nlb_players
),

retrosheet_team_seasons AS (
    SELECT DISTINCT season, team_id FROM retrosheet_rosters
),

overlaps AS (
    SELECT
        linked.season,
        linked.databank_team_id,
        retro.team_id,
        COUNT(DISTINCT linked.player_id) AS shared_players
    FROM linked_rosters AS linked
    INNER JOIN retrosheet_rosters AS retro
        ON linked.season = retro.season AND linked.player_id = retro.player_id
    GROUP BY 1, 2, 3
),

best_overlap AS (
    SELECT
        season,
        databank_team_id,
        ARG_MAX(team_id, (shared_players, team_id)) AS best_roster_team_id,
        MAX(shared_players) AS best_roster_shared_players
    FROM overlaps
    GROUP BY 1, 2
),

roster_counts AS (
    SELECT
        season,
        databank_team_id,
        COUNT(DISTINCT databank_player_id) AS lahman_players,
        COUNT(DISTINCT player_id) AS linked_lahman_players
    FROM linked_rosters
    GROUP BY 1, 2
),

final AS (
    SELECT
        teams.season,
        teams.databank_team_id,
        teams.team_name,
        teams.league_id,
        teams.team_id_retro,
        teams.crosswalk_team_id,
        teams.team_id,
        COALESCE(teams.crosswalk_team_id, teams.team_id_retro) IN (SELECT team_id FROM franchise_ids)
            AS team_id_in_seed_franchises,
        COALESCE(counts.lahman_players, 0) AS lahman_players,
        COALESCE(counts.linked_lahman_players, 0) AS linked_lahman_players,
        best.best_roster_team_id,
        COALESCE(best.best_roster_shared_players, 0) AS best_roster_shared_players,
        best.best_roster_shared_players / NULLIF(counts.linked_lahman_players, 0) AS best_roster_overlap_share,
        COALESCE(resolved.shared_players, 0) AS resolved_team_shared_players,
        CASE
            WHEN teams.team_id IS NULL THEN 'unmapped'
            WHEN best.best_roster_team_id IS NULL OR retro_teams.team_id IS NULL THEN 'no_retrosheet_games'
            WHEN COALESCE(resolved.shared_players, 0) < best.best_roster_shared_players
                THEN 'crosswalk_disagrees_with_rosters'
            WHEN teams.crosswalk_team_id IS NOT NULL THEN 'override'
            ELSE 'confirmed'
        END AS resolution_status
    FROM main_models.stg_databank_teams AS teams
    LEFT JOIN roster_counts AS counts USING (season, databank_team_id)
    LEFT JOIN best_overlap AS best USING (season, databank_team_id)
    LEFT JOIN retrosheet_team_seasons AS retro_teams
        ON teams.season = retro_teams.season AND teams.team_id = retro_teams.team_id
    LEFT JOIN overlaps AS resolved
        ON teams.season = resolved.season
            AND teams.databank_team_id = resolved.databank_team_id
            AND teams.team_id = resolved.team_id
    WHERE teams.is_negro_leagues
)

SELECT * FROM final
