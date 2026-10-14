MODEL (
  name main_models.nlb_game_scopes,
  kind FULL,
  description 'Each game in Retrosheet''s negroleagues.zip classified for comparison with Lahman, one row per game. Lahman''s Negro Leagues season lines cover the regular season plus exhibitions between two clubs Lahman lists that season ("major-club exhibitions"), so those games are in matched scope. Only games with a box score carry full batting and pitching lines.',
  grain (game_id),
  columns (
    game_id VARCHAR,
    season SMALLINT,
    home_team_id VARCHAR,
    away_team_id VARCHAR,
    game_scope VARCHAR,
    is_matched_scope BOOLEAN,
    has_box_score BOOLEAN
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    season = @doc('season'),
    home_team_id = 'Retrosheet home team id from the Negro Leagues file.',
    away_team_id = 'Retrosheet away team id from the Negro Leagues file.',
    game_scope = 'RegularSeason, MajorClubExhibition (both clubs in Lahman that season), OtherExhibition, or Postseason (championships, playoffs, and all-star games).',
    is_matched_scope = 'The game is in the scope Lahman''s season lines cover: RegularSeason or MajorClubExhibition.',
    has_box_score = 'Retrosheet has the game''s box score, so its player lines are complete.'
  ),
  audits (
    not_null(columns := (game_id, season, game_scope, is_matched_scope, has_box_score)),
    unique_grain(columns := (game_id)),
    accepted_values(column := game_scope, is_in := ('RegularSeason', 'MajorClubExhibition', 'OtherExhibition', 'Postseason'))
  ),
);

WITH lahman_clubs AS (
    SELECT DISTINCT season, team_id
    FROM main_models.stg_databank_teams
    WHERE is_negro_leagues AND team_id IS NOT NULL
),

final AS (
    SELECT
        games.game_id,
        games.season::SMALLINT AS season,
        games.hometeam AS home_team_id,
        games.visteam AS away_team_id,
        CASE
            WHEN games.gametype = 'regular' THEN 'RegularSeason'
            WHEN games.gametype = 'exhibition' AND home_club.team_id IS NOT NULL AND away_club.team_id IS NOT NULL
                THEN 'MajorClubExhibition'
            WHEN games.gametype = 'exhibition' THEN 'OtherExhibition'
            ELSE 'Postseason'
        END AS game_scope,
        game_scope IN ('RegularSeason', 'MajorClubExhibition') AS is_matched_scope,
        COALESCE(LOWER(games.box) = 'y', FALSE) AS has_box_score
    FROM main_models.stg_nlb_games AS games
    LEFT JOIN lahman_clubs AS home_club
        ON games.season = home_club.season AND games.hometeam = home_club.team_id
    LEFT JOIN lahman_clubs AS away_club
        ON games.season = away_club.season AND games.visteam = away_club.team_id
)

SELECT * FROM final
