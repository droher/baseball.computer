WITH flattened AS (
    SELECT DISTINCT
        game_id,
        CASE WHEN scorer IS NULL or TRIM(scorer) = '' THEN 'unknown' ELSE scorer END AS filled_scorer,
        UNNEST(STRING_SPLIT_REGEX(filled_scorer, '[/&,]|( -(-?) )')) as scorer,
    FROM {{ ref('stg_games') }}
),

counts AS (
    SELECT
        game_id,
        COALESCE(s.normalized_scorer, LOWER(TRIM(f.scorer))) AS cleaned_scorer,
        COUNT(*) OVER (PARTITION BY game_id) AS game_scorer_splits,
        COUNT(*) OVER (PARTITION BY cleaned_scorer) AS scorer_game_count_raw,
        1 / game_scorer_splits AS game_share,
    FROM flattened AS f
    LEFT JOIN {{ ref('seed_scorer_lookup') }} AS s ON LOWER(TRIM(f.scorer)) = LOWER(TRIM(s.scorer))
    WHERE TRIM(f.scorer) != ''
),

final AS (
    SELECT
        game_id,
        cleaned_scorer,
        CASE
            WHEN cleaned_scorer LIKE '%701'
                THEN 'Scorekeeper'
            WHEN REGEXP_FULL_MATCH(cleaned_scorer, '\d+')
                THEN 'Anonymous'
            ELSE 'Other'
        END AS scorer_type,
        game_share,
        scorer_game_count_raw,
        SUM(game_share) OVER (PARTITION BY cleaned_scorer) AS scorer_game_count_weighted,
    FROM counts
)

SELECT * FROM final
