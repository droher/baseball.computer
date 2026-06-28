MODEL (
  name main_models.game_exposure_ledger,
  kind FULL,
  description 'Per (game_id, team_id) exposure ledger: how much of the canonical 9-inning denominator each team actually played, and which completion regime governs downstream rate-stat denominators. scheduled_innings is 9 universally in v1 (variant rules — 2020-2021 7-inning doubleheaders, pre-1957 American Association 7-inning twin bills — are documented but not distinguished and are reserved for follow-up). actual_outs_batting / actual_outs_fielding are sourced from team_game_pitching_stats.outs_recorded with the standard inversion (a team batting outs = opposing pitching staff outs_recorded). completion_status is derived from game_results facts (forfeit_flag, suspension_flag, is_shortened_game) plus a walk_off rule: every home-won game with extras is a walk-off (home must walk off to win an extra-inning game); home-won 9-inning games are walk-offs only when the home team batted partial bottom 9 (home batting outs in (25, 26)). 0-out walk-offs (leadoff HR with score tied) are indistinguishable from "home did not bat bottom 9" using outs alone and are conservatively classified as complete in v1. denominator_policy never overrides official suspension / forfeit / walk_off facts: complete + walk_off use full_game; shortened + suspended use observed_outs; forfeit uses official_result_only; unknown (no duration_outs / no outs data and not flagged) uses synthetic_required. exposure_confidence is high for complete (with outs) + walk_off, medium for shortened / suspended / complete-without-outs (gamelog-only games), low for forfeit / unknown. Joins back to game_results for the canonical universe so the FK audit holds.',
  grain (game_id, team_id),
  columns (
    game_id VARCHAR,
    team_id TEAM_ID,
    scheduled_innings UTINYINT,
    actual_outs_batting USMALLINT,
    actual_outs_fielding USMALLINT,
    completion_status VARCHAR,
    denominator_policy VARCHAR,
    exposure_confidence VARCHAR
  ),
  column_descriptions (
    game_id = @doc('game_id'),
    team_id = @doc('team_id'),
    scheduled_innings = 'Scheduled regulation length in innings. v1 hardcodes 9; variant 7-inning rules are not distinguished yet.',
    actual_outs_batting = 'Outs recorded against this team while batting (= opposing pitching staff outs_recorded). NULL when no team_game_pitching_stats row exists (gamelog-only games).',
    actual_outs_fielding = 'Outs recorded by this team while fielding (= this team pitching staff outs_recorded). NULL when no team_game_pitching_stats row exists.',
    completion_status = 'How the team-game finished: complete, walk_off, shortened, suspended, forfeit, unknown.',
    denominator_policy = 'How downstream rate-stat denominators should treat this team-game: full_game, observed_outs, official_result_only, synthetic_required, exclude.',
    exposure_confidence = 'Qualitative confidence in the exposure facts above. high, medium, low.'
  ),
  audits (
    not_null(columns := (game_id, team_id, scheduled_innings, completion_status, denominator_policy, exposure_confidence)),
    unique_grain(columns := (game_id, team_id)),
    accepted_values(column := completion_status, is_in := (
      'complete', 'walk_off', 'shortened', 'suspended', 'forfeit', 'unknown'
    )),
    accepted_values(column := denominator_policy, is_in := (
      'full_game', 'observed_outs', 'official_result_only', 'exclude', 'synthetic_required'
    )),
    accepted_values(column := exposure_confidence, is_in := ('high', 'medium', 'low')),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  )
);

WITH games_in_scope AS (
    SELECT
        game_id,
        season,
        home_team_id,
        away_team_id,
        forfeit_flag,
        suspension_flag,
        is_shortened_game,
        is_nine_inning_game,
        is_extra_inning_game,
        duration_outs,
        winning_side
    FROM main_models.game_results
    WHERE season BETWEEN @start_season AND @end_season
),

team_pitching AS (
    SELECT
        game_id,
        team_id,
        outs_recorded
    FROM main_models.team_game_pitching_stats
),

team_game AS (
    SELECT
        g.game_id,
        g.home_team_id,
        g.away_team_id,
        g.forfeit_flag,
        g.suspension_flag,
        g.is_shortened_game,
        g.is_nine_inning_game,
        g.is_extra_inning_game,
        g.duration_outs,
        g.winning_side,
        h.outs_recorded AS home_fielding_outs,
        a.outs_recorded AS away_fielding_outs,
        a.outs_recorded AS home_batting_outs,
        CASE
            WHEN g.forfeit_flag THEN 'forfeit'
            WHEN g.suspension_flag THEN 'suspended'
            WHEN g.is_shortened_game THEN 'shortened'
            WHEN g.duration_outs IS NULL THEN 'complete'
            WHEN g.winning_side = 'Home' AND g.is_extra_inning_game THEN 'walk_off'
            WHEN g.winning_side = 'Home'
                AND g.is_nine_inning_game
                AND a.outs_recorded IN (25, 26)
                THEN 'walk_off'
            ELSE 'complete'
        END AS completion_status
    FROM games_in_scope AS g
    LEFT JOIN team_pitching AS h
        ON h.game_id = g.game_id AND h.team_id = g.home_team_id
    LEFT JOIN team_pitching AS a
        ON a.game_id = g.game_id AND a.team_id = g.away_team_id
),

home_side AS (
    SELECT
        game_id,
        home_team_id AS team_id,
        away_fielding_outs AS actual_outs_batting,
        home_fielding_outs AS actual_outs_fielding,
        completion_status
    FROM team_game
),

away_side AS (
    SELECT
        game_id,
        away_team_id AS team_id,
        home_fielding_outs AS actual_outs_batting,
        away_fielding_outs AS actual_outs_fielding,
        completion_status
    FROM team_game
),

classified AS (
    SELECT * FROM home_side
    UNION ALL BY NAME
    SELECT * FROM away_side
)

SELECT
    game_id,
    team_id,
    9::UTINYINT AS scheduled_innings,
    actual_outs_batting::USMALLINT AS actual_outs_batting,
    actual_outs_fielding::USMALLINT AS actual_outs_fielding,
    completion_status,
    CASE completion_status
        WHEN 'complete' THEN 'full_game'
        WHEN 'walk_off' THEN 'full_game'
        WHEN 'shortened' THEN 'observed_outs'
        WHEN 'suspended' THEN 'observed_outs'
        WHEN 'forfeit' THEN 'official_result_only'
        ELSE 'synthetic_required'
    END AS denominator_policy,
    CASE
        WHEN completion_status = 'walk_off' THEN 'high'
        WHEN completion_status = 'complete' AND actual_outs_fielding IS NOT NULL THEN 'high'
        WHEN completion_status = 'complete' THEN 'medium'
        WHEN completion_status IN ('shortened', 'suspended') THEN 'medium'
        ELSE 'low'
    END AS exposure_confidence
FROM classified
