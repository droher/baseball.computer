from __future__ import annotations

from python_models.imputation.context import ContextCompletionConfig

OFFICIAL_ROLES = (
    "official_scorer",
    "umpire_home_id",
    "umpire_first_id",
    "umpire_second_id",
    "umpire_third_id",
    "umpire_left_id",
    "umpire_right_id",
)
OUTPUT_SCHEMA = {
    "game_id": "VARCHAR",
    "season": "SMALLINT",
    "role": "VARCHAR",
    "recorded_identity": "VARCHAR",
    "identity_status": "VARCHAR",
    "unresolved_slot": "VARCHAR",
    "candidate_identities": "VARCHAR[]",
    "candidate_probabilities": "DOUBLE[]",
    "candidate_method": "VARCHAR",
    "candidate_observations": "BIGINT",
    "recorded_role_frequency": "DOUBLE",
    "role_presence_status": "VARCHAR",
    "confidence_status": "VARCHAR",
}


def build_officials_completion_sql(config: ContextCompletionConfig) -> str:
    roles = ", ".join(
        f"struct_pack(role := '{role}', identity := {role}::VARCHAR)"
        for role in OFFICIAL_ROLES
    )
    limit = f"LIMIT {config.sample_games}" if config.sample_games else ""
    seed = config.sample_seed.replace("'", "''")
    return f"""
WITH games AS (
    SELECT game_id, season,
        coalesce(home_league::VARCHAR, away_league::VARCHAR, 'Unknown') AS league,
        game_type::VARCHAR AS game_type,
        unnest([{roles}]) AS official
    FROM main_models.game_start_info WHERE source_type = 'PlayByPlay'
), source AS (
    SELECT game_id, season, league, game_type,
        official.role AS role, nullif(official.identity, '') AS identity
    FROM games
), targets AS (
    SELECT DISTINCT game_id FROM source
    WHERE season BETWEEN {config.start_season} AND {config.end_season}
    ORDER BY hash(game_id, '{seed}'), game_id {limit}
), candidate_counts AS (
    SELECT season, league, game_type, role, identity, count(*) AS n
    FROM source WHERE identity IS NOT NULL GROUP BY ALL
), candidate_weights AS (
    SELECT *, n::DOUBLE / sum(n) OVER (PARTITION BY season, league, game_type, role) AS p
    FROM candidate_counts
), candidates AS (
    SELECT season, league, game_type, role,
        list(identity ORDER BY identity) AS identities,
        list(p ORDER BY identity) AS probabilities, sum(n)::BIGINT AS observations
    FROM candidate_weights GROUP BY ALL
), presence AS (
    SELECT season, league, game_type, role,
        count(identity)::DOUBLE / count(*) AS recorded_role_frequency
    FROM source GROUP BY ALL
)
SELECT s.game_id, s.season, s.role, s.identity AS recorded_identity,
    CASE WHEN s.identity IS NOT NULL THEN 'observed' ELSE 'unresolved_participant_slot' END AS identity_status,
    CASE WHEN s.identity IS NULL THEN s.game_id || ':' || s.role END AS unresolved_slot,
    CASE WHEN s.identity IS NOT NULL THEN [s.identity] ELSE coalesce(c.identities, []::VARCHAR[]) END AS candidate_identities,
    CASE WHEN s.identity IS NOT NULL THEN [1.0::DOUBLE] ELSE coalesce(c.probabilities, []::DOUBLE[]) END AS candidate_probabilities,
    CASE WHEN s.identity IS NOT NULL THEN 'source_recorded'
         WHEN c.observations > 0 THEN 'same_season_league_game_type_role_empirical'
         ELSE 'unresolved_identity_no_contemporaneous_candidates' END AS candidate_method,
    coalesce(c.observations, 0)::BIGINT AS candidate_observations,
    p.recorded_role_frequency,
    CASE WHEN s.identity IS NOT NULL THEN 'observed_present'
         WHEN s.role = 'umpire_home_id' THEN 'role_required_identity_unresolved'
         ELSE 'absence_or_unrecorded_presence_unknown' END AS role_presence_status,
    'exploratory' AS confidence_status
FROM source s JOIN targets USING (game_id)
LEFT JOIN candidates c USING (season, league, game_type, role)
LEFT JOIN presence p USING (season, league, game_type, role)
"""
