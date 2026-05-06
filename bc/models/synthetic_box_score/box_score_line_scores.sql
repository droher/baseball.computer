MODEL (
  name synthetic_box_score.box_score_line_scores,
  kind FULL,
  description 'Inning-by-inning runs parsed from the gamelog away/home line score VARCHARs. Tokens are either a single digit or a parenthesized multi-digit run total (10+ runs in one inning). The X sentinel — winning home team did not bat in the bottom of the last inning — is dropped from the regex match, so it produces no row. Grain matches main_models.stg_box_score_line_scores.',
  columns (
    game_id GAME_ID,
    batting_side SIDE,
    inning UTINYINT,
    runs UTINYINT
  ),
  grain (game_id, batting_side, inning),
  column_descriptions (
    game_id = @doc('game_id'),
    batting_side = @doc('batting_side'),
    inning = @doc('inning'),
    runs = @doc('runs')
  ),
  audits (
    not_null(columns := (game_id, batting_side, inning)),
    unique_grain(columns := (game_id, batting_side, inning)),
    relationships(column := game_id, to_model := main_models.game_results, to_column := game_id)
  ),
  physical_properties (
    download_parquet = 'https://data.baseball.computer/dbt/synthetic_box_score_box_score_line_scores.parquet'
  ),
);









WITH gamelog_only AS (
    SELECT DISTINCT g.game_id
    FROM synthetic_box_score.box_score_games AS g
),

per_side AS (
    SELECT
        gl.game_id,
        'Away'::SIDE AS batting_side,
        gl.away_line_score AS line_score
    FROM main_models.stg_gamelog AS gl
    INNER JOIN gamelog_only USING (game_id)
    WHERE gl.away_line_score IS NOT NULL AND gl.away_line_score != ''
    UNION ALL BY NAME
    SELECT
        gl.game_id,
        'Home'::SIDE AS batting_side,
        gl.home_line_score AS line_score
    FROM main_models.stg_gamelog AS gl
    INNER JOIN gamelog_only USING (game_id)
    WHERE gl.home_line_score IS NOT NULL AND gl.home_line_score != ''
),

tokenized AS (
    SELECT
        per_side.game_id,
        per_side.batting_side,
        t.idx::UTINYINT AS inning,
        TRIM(t.token, '()')::UTINYINT AS runs
    FROM per_side,
        UNNEST(regexp_extract_all(per_side.line_score, '\((\d+)\)|\d'))
        WITH ORDINALITY AS t(token, idx)
)

SELECT
    game_id::GAME_ID AS game_id,
    batting_side,
    inning,
    runs
FROM tokenized
