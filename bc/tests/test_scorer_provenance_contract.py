from pathlib import Path

import duckdb


BC_ROOT = Path(__file__).parents[1]
EXTERNAL_MODELS = BC_ROOT / "external_models.yaml"
STG_GAMES = BC_ROOT / "models/staging/game/stg_games.sql"
GAME_START_INFO = BC_ROOT / "models/intermediate/game_level/game_start_info.sql"
EVENT_CONTEXT = BC_ROOT / "models/intermediate/coverage/event_observation_context.sql"
CONTEXT_LEDGER = (
    BC_ROOT / "models/intermediate/coverage/game_context_observation_ledger.sql"
)


def _model_block(source: str, name: str) -> str:
    start = source.index(f"- name: {name}")
    next_model = source.find("\n- name: ", start + 1)
    return source[start:] if next_model == -1 else source[start:next_model]


def _cte(source: str, name: str, next_name: str) -> str:
    start = source.index(f"{name} AS (")
    end = source.index(f"\n\n{next_name} AS (", start)
    return source[start:end].removesuffix(",")


def test_external_and_staging_models_require_distinct_raw_columns() -> None:
    external = EXTERNAL_MODELS.read_text(encoding="utf-8")
    for model_name in ("box_score.box_score_games", "game.games"):
        block = _model_block(external, model_name)
        assert "    scorer: VARCHAR\n" in block
        assert "    official_scorer: VARCHAR\n" in block
        assert "    source_scorer: VARCHAR\n" in block

    staging = STG_GAMES.read_text(encoding="utf-8")
    assert "    official_scorer VARCHAR," in staging
    assert "    source_scorer VARCHAR," in staging
    assert "        official_scorer," in staging
    assert "        source_scorer," in staging
    assert "COALESCE(official_scorer" not in staging
    assert "COALESCE(source_scorer" not in staging


def test_source_specific_statuses_execute_for_values_sentinels_and_absence() -> None:
    source = CONTEXT_LEDGER.read_text(encoding="utf-8")
    official = _cte(source, "official_scorer", "source_scorer")
    administrative = _cte(source, "source_scorer", "inputter")
    query = f"""
        WITH games_in_scope AS (
            SELECT * FROM (VALUES
                ('g1', 'scogc701', 'Retrosheet research staff'),
                ('g2', '?', ' Unknown '),
                ('g3', NULL, '')
            ) AS t(game_id, official_scorer, source_scorer)
        ),
        stg_source_family AS (
            SELECT game_id, 'retrosheet_pbp' AS source_family
            FROM games_in_scope
        ),
        {official},
        {administrative}
        SELECT game_id, context_dimension, raw_value, observed_status,
               context_confidence
        FROM official_scorer
        UNION ALL BY NAME
        SELECT game_id, context_dimension, raw_value, observed_status,
               context_confidence
        FROM source_scorer
        ORDER BY game_id, context_dimension
    """
    with duckdb.connect() as connection:
        rows = connection.execute(query).fetchall()
    assert rows == [
        ("g1", "official_scorer", "scogc701", "observed", "high"),
        (
            "g1",
            "source_scorer",
            "Retrosheet research staff",
            "observed",
            "high",
        ),
        ("g2", "official_scorer", "?", "unknown_code", "low"),
        ("g2", "source_scorer", " Unknown ", "unknown_code", "low"),
        ("g3", "official_scorer", None, "missing", "low"),
        ("g3", "source_scorer", "", "missing", "low"),
    ]


def test_context_consumers_expose_new_fields_without_changing_legacy_rollup() -> None:
    start_info = GAME_START_INFO.read_text(encoding="utf-8")
    event_context = EVENT_CONTEXT.read_text(encoding="utf-8")
    for field in ("official_scorer", "source_scorer"):
        assert f"        g.{field}," in start_info
        assert f"        {field}," in event_context
        assert f"    gms.{field}," in event_context
    assert "official_scorer_status VARCHAR" in event_context
    assert "source_scorer_status VARCHAR" in event_context
    assert (
        "WHERE context_dimension NOT IN ('official_scorer', 'source_scorer')"
        in event_context
    )
    assert "COALESCE(gms.official_scorer" not in event_context
    assert "COALESCE(gms.source_scorer" not in event_context
