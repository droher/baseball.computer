MODEL (
  name main_models.nlb_lahman_player_resolution,
  kind FULL,
  description 'Candidate Retrosheet matches for Lahman Negro Leagues people who do not link to a Retrosheet Negro Leagues player, one row per candidate pair. A Lahman person needs resolution when stg_people has no Retrosheet id for them, or the id is in neither the Retrosheet biofile nor the Negro Leagues player file. Candidates are Retrosheet Negro Leagues players with the same accent-folded last name. Evidence: first initial, birth year, team-seasons the two share (Lahman team resolved through stg_databank_teams), and at-bat similarity on those team-seasons. Shared team-seasons dominate the rank and the confidence tier, since names and birth years are too sparse to decide alone. Reviewed matches go in seed_nlb_lahman_player_crosswalk; nothing here is applied automatically.',
  grain (databank_player_id, candidate_player_id),
  columns (
    databank_player_id VARCHAR,
    lahman_first_name VARCHAR,
    lahman_last_name VARCHAR,
    lahman_birth_year INTEGER,
    lahman_retrosheet_player_id VARCHAR,
    candidate_player_id VARCHAR,
    candidate_first_name VARCHAR,
    candidate_last_name VARCHAR,
    candidate_birth_year INTEGER,
    is_same_first_initial BOOLEAN,
    is_birth_year_match BOOLEAN,
    shared_team_seasons UINTEGER,
    at_bats_similarity DOUBLE,
    is_candidate_linked_to_other_lahman_person BOOLEAN,
    candidate_rank UINTEGER,
    confidence_tier VARCHAR
  ),
  column_descriptions (
    databank_player_id = 'Lahman player id.',
    lahman_retrosheet_player_id = 'The Retrosheet id stg_people gives the Lahman person, if any.',
    candidate_player_id = 'Retrosheet player id of the candidate.',
    is_same_first_initial = 'The first names start with the same letter (accent-folded).',
    is_birth_year_match = 'Both birth years are known and equal; FALSE when both are known and differ; NULL when either is unknown.',
    shared_team_seasons = 'Season and team pairs where the Lahman person has a Negro Leagues row and the candidate is on the Retrosheet roster.',
    at_bats_similarity = 'On shared team-seasons, the smaller of the two at-bat totals divided by the larger; NULL when either side has no at-bat total or both are 0.',
    is_candidate_linked_to_other_lahman_person = 'Another Lahman person already has the candidate''s id as their Retrosheet id.',
    candidate_rank = 'Rank of the candidate for the Lahman person: more shared team-seasons first, then first initial, birth year, and at-bat similarity.',
    confidence_tier = 'high (a shared team-season, same first initial, and no birth-year conflict), medium (a shared team-season), or low (name only).'
  ),
  audits (
    not_null(columns := (databank_player_id, candidate_player_id, candidate_rank, confidence_tier)),
    unique_grain(columns := (databank_player_id, candidate_player_id)),
    accepted_values(column := confidence_tier, is_in := ('high', 'medium', 'low'))
  ),
);

WITH lahman_rows AS (
    SELECT season, team_id, databank_player_id, at_bats
    FROM main_models.stg_databank_batting
    WHERE is_negro_leagues
    UNION ALL
    SELECT season, team_id, databank_player_id, NULL AS at_bats
    FROM main_models.stg_databank_pitching
    WHERE is_negro_leagues
),

known_retrosheet_ids AS (
    SELECT player_id FROM main_models.stg_bio
    UNION
    SELECT player_id FROM main_models.stg_nlb_players
),

unlinked_people AS (
    SELECT
        people.databank_player_id,
        people.first_name AS lahman_first_name,
        people.last_name AS lahman_last_name,
        people.birth_year::INTEGER AS lahman_birth_year,
        people.retrosheet_player_id AS lahman_retrosheet_player_id,
        REGEXP_REPLACE(STRIP_ACCENTS(LOWER(people.last_name)), '[^a-z]', '', 'g') AS last_name_key,
        LEFT(STRIP_ACCENTS(LOWER(people.first_name)), 1) AS first_initial
    FROM main_models.stg_people AS people
    WHERE people.databank_player_id IN (SELECT databank_player_id FROM lahman_rows)
        AND (
            people.retrosheet_player_id IS NULL
            OR people.retrosheet_player_id NOT IN (SELECT player_id FROM known_retrosheet_ids)
        )
),

retrosheet_people AS (
    SELECT
        players.player_id,
        ANY_VALUE(players.first) AS candidate_first_name,
        ANY_VALUE(players.last) AS candidate_last_name,
        REGEXP_REPLACE(STRIP_ACCENTS(LOWER(ANY_VALUE(players.last))), '[^a-z]', '', 'g') AS last_name_key,
        LEFT(STRIP_ACCENTS(LOWER(ANY_VALUE(players.first))), 1) AS first_initial
    FROM main_models.stg_nlb_players AS players
    GROUP BY players.player_id
),

retrosheet_birth_years AS (
    SELECT player_id, TRY_CAST(REGEXP_EXTRACT(birth_date::VARCHAR, '(\d{4})', 1) AS INTEGER) AS birth_year
    FROM main_models.stg_bio
),

retrosheet_team_seasons AS (
    SELECT
        players.season::SMALLINT AS season,
        players.team_id,
        players.player_id,
        batting.at_bats
    FROM (SELECT DISTINCT season, team_id, player_id FROM main_models.stg_nlb_players) AS players
    LEFT JOIN (
        SELECT
            scopes.season,
            bat.team_id,
            bat.player_id,
            SUM(bat.b_ab) AS at_bats
        FROM main_models.stg_nlb_batting AS bat
        INNER JOIN main_models.nlb_game_scopes AS scopes USING (game_id)
        GROUP BY 1, 2, 3
    ) AS batting USING (season, team_id, player_id)
),

lahman_team_seasons AS (
    SELECT season, team_id, databank_player_id, SUM(at_bats) AS at_bats
    FROM lahman_rows
    WHERE team_id IS NOT NULL
    GROUP BY 1, 2, 3
),

candidates AS (
    SELECT
        unlinked.*,
        retro.player_id AS candidate_player_id,
        retro.candidate_first_name,
        retro.candidate_last_name,
        retro.first_initial AS candidate_first_initial
    FROM unlinked_people AS unlinked
    INNER JOIN retrosheet_people AS retro USING (last_name_key)
),

shared AS (
    SELECT
        candidates.databank_player_id,
        candidates.candidate_player_id,
        COUNT(*) AS shared_team_seasons,
        CASE WHEN SUM(lahman.at_bats) IS NOT NULL AND SUM(retro.at_bats) IS NOT NULL
            THEN LEAST(SUM(lahman.at_bats), SUM(retro.at_bats))
                / NULLIF(GREATEST(SUM(lahman.at_bats), SUM(retro.at_bats)), 0)
        END AS at_bats_similarity
    FROM candidates
    INNER JOIN lahman_team_seasons AS lahman
        ON candidates.databank_player_id = lahman.databank_player_id
    INNER JOIN retrosheet_team_seasons AS retro
        ON candidates.candidate_player_id = retro.player_id
            AND lahman.season = retro.season
            AND lahman.team_id = retro.team_id
    GROUP BY 1, 2
),

linked_elsewhere AS (
    SELECT DISTINCT retrosheet_player_id AS player_id
    FROM main_models.stg_people
    WHERE retrosheet_player_id IS NOT NULL
),

scored AS (
    SELECT
        candidates.databank_player_id,
        candidates.lahman_first_name,
        candidates.lahman_last_name,
        candidates.lahman_birth_year,
        candidates.lahman_retrosheet_player_id,
        candidates.candidate_player_id,
        candidates.candidate_first_name,
        candidates.candidate_last_name,
        birth_years.birth_year AS candidate_birth_year,
        COALESCE(candidates.first_initial = candidates.candidate_first_initial, FALSE) AS is_same_first_initial,
        candidates.lahman_birth_year = birth_years.birth_year AS is_birth_year_match,
        COALESCE(shared.shared_team_seasons, 0) AS shared_team_seasons,
        shared.at_bats_similarity,
        candidates.candidate_player_id IN (SELECT player_id FROM linked_elsewhere)
            AND candidates.candidate_player_id IS DISTINCT FROM candidates.lahman_retrosheet_player_id
            AS is_candidate_linked_to_other_lahman_person
    FROM candidates
    LEFT JOIN shared
        ON candidates.databank_player_id = shared.databank_player_id
            AND candidates.candidate_player_id = shared.candidate_player_id
    LEFT JOIN retrosheet_birth_years AS birth_years
        ON candidates.candidate_player_id = birth_years.player_id
),

final AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY databank_player_id
            ORDER BY
                shared_team_seasons DESC,
                is_same_first_initial DESC,
                COALESCE(is_birth_year_match, TRUE) DESC,
                at_bats_similarity DESC NULLS LAST,
                candidate_player_id
        ) AS candidate_rank,
        CASE
            WHEN shared_team_seasons > 0 AND is_same_first_initial AND is_birth_year_match IS DISTINCT FROM FALSE
                THEN 'high'
            WHEN shared_team_seasons > 0 THEN 'medium'
            ELSE 'low'
        END AS confidence_tier
    FROM scored
)

SELECT * FROM final
