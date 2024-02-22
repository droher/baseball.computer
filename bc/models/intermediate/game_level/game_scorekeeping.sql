WITH flattened_init AS (
    SELECT
        season,
        game_id,
        away_team_id,
        home_team_id,
        date,
        inputter,
        translator,
        CASE WHEN scorer IS NULL or TRIM(scorer) = '' THEN 'unknown' ELSE scorer END AS filled_scorer,
        -- Delimiters aren't consistent, so we need to split on multiple characters
        UNNEST(STRING_SPLIT_REGEX(filled_scorer, '[/&,]|( -(-?) )')) as split_scorer,
        LOWER(TRIM(split_scorer)) AS scorer
    FROM {{ ref('stg_games') }}
),

flattened AS (
    SELECT
        * REPLACE (
            -- One more split to cover the common cases of "[team1] [team2]" AND "teams"
            UNNEST(
                CASE WHEN scorer NOT IN ('tsn box', 'red sox') AND REGEXP_FULL_MATCH(scorer, '\w{3}\W+\w{3}')
                        THEN STRING_SPLIT_REGEX(scorer, '\W+')
                    WHEN scorer IN ('teams', 'both teams', 'both')
                        THEN [LOWER(away_team_id), LOWER(home_team_id)]
                    ELSE [scorer]
                END
            ) AS scorer
        )
    FROM flattened_init
),

counts AS (
    SELECT DISTINCT ON (f.game_id, cleaned_scorer)
        f.season,
        f.game_id,
        f.date,
        f.away_team_id,
        LOWER(away.nickname) AS away_nickname,
        f.home_team_id,
        LOWER(home.nickname) AS home_nickname,
        f.inputter,
        f.translator,
        CASE WHEN s.normalized_scorer IS NOT NULL
                THEN s.normalized_scorer
            WHEN dis_away.normalized_scorer IS NOT NULL
                THEN dis_away.normalized_scorer
            WHEN dis_home.normalized_scorer IS NOT NULL
                THEN dis_home.normalized_scorer
            WHEN num_range.group_id IS NOT NULL
                THEN num_range.group_id::VARCHAR
            WHEN f.scorer IN (LOWER(away.nickname), LOWER(f.away_team_id))
                THEN LOWER(away.nickname)
            WHEN f.scorer IN (LOWER(home.nickname), LOWER(f.home_team_id))
                THEN LOWER(home.nickname)
            ELSE f.scorer
        END AS cleaned_scorer,
        f.scorer AS raw_scorer,
        COUNT(*) OVER (PARTITION BY f.game_id) AS game_scorer_splits,
        COUNT(*) OVER (PARTITION BY cleaned_scorer) AS scorer_game_count_raw,
        1 / game_scorer_splits AS game_share,
    FROM flattened AS f
    LEFT JOIN {{ ref('seed_scorer_lookup') }} AS s ON f.scorer = LOWER(TRIM(s.scorer))
    LEFT JOIN {{ ref('seed_scorer_numerical_ranges') }} AS num_range
        -- TODO: Test assumption that all possible range candidates are 3 digits
        ON REGEXP_FULL_MATCH(f.scorer, '\d{3}')
            AND f.scorer BETWEEN num_range.start::VARCHAR AND num_range.end::VARCHAR
    LEFT JOIN {{ ref('seed_franchises') }} AS away
        ON f.away_team_id = away.team_id
            AND f.date BETWEEN away.date_start AND COALESCE(away.date_end, '9999-12-31')
    LEFT JOIN {{ ref('seed_franchises') }} AS home
        ON f.home_team_id = home.team_id
            AND f.date BETWEEN home.date_start AND COALESCE(home.date_end, '9999-12-31')
    LEFT JOIN {{ ref('seed_scorer_disambiguation') }} AS dis_away
        ON f.scorer = dis_away.scorer
            AND LOWER(f.away_team_id) = dis_away.team_id
    LEFT JOIN {{ ref('seed_scorer_disambiguation') }} AS dis_home
        ON f.scorer = dis_home.scorer
            AND LOWER(f.home_team_id) = dis_home.team_id
    WHERE TRIM(f.scorer) != ''
),

final AS (
    SELECT
        season,
        game_id,
        date,
        cleaned_scorer,
        raw_scorer,
        away_team_id,
        home_team_id,
        inputter,
        translator,
        game_share,
        scorer_game_count_raw,
        SUM(game_share) OVER scorer AS scorer_game_count_weighted,
        CASE
            WHEN SUM(game_share) OVER scorer_away >= SUM(game_share) OVER scorer_home
                THEN away_team_id
            ELSE home_team_id
        END AS scorer_more_common_team_id
    FROM counts
    WINDOW
        scorer AS (PARTITION BY cleaned_scorer),
        scorer_away AS (PARTITION BY cleaned_scorer, away_team_id),
        scorer_home AS (PARTITION BY cleaned_scorer, home_team_id)
)

SELECT * FROM final
