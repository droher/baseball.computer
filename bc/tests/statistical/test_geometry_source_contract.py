from pathlib import Path

import duckdb


BC_ROOT = Path(__file__).parents[2]
LEDGER_SQL = BC_ROOT / "models/intermediate/coverage/event_observation_geometry.sql"
GEOMETRY_SQL = (
    BC_ROOT / "models/intermediate/modeling_datasets/model_input_geometry.sql"
)
OBSERVATION_SQL = (
    BC_ROOT
    / "models/intermediate/modeling_datasets/model_input_observation_batted_ball.sql"
)
AUDIT_SQL = BC_ROOT / "audits/sentinel_status_consistent.sql"


def _cte(source: str, name: str, next_name: str) -> str:
    start = source.index(f"{name} AS (")
    end = source.index(f"\n\n{next_name} AS (", start)
    return source[start:end].removesuffix(",")


def _expression(source: str, anchor: str, alias: str) -> str:
    start = source.index(anchor, source.index("\nSELECT\n"))
    end = source.index(f" AS {alias}", start) + len(f" AS {alias}")
    return source[start:end]


def test_side_and_angle_source_contract_executes() -> None:
    source = LEDGER_SQL.read_text(encoding="utf-8")
    side = _cte(source, "location_side", "location_angle")
    angle = _cte(source, "location_angle", "location_depth")
    query = f"""
        WITH events_in_scope AS (
            SELECT * FROM (VALUES
                (1, 'g1', 'First', 'Right', 'Left', 'Default'),
                (2, 'g1', 'Unknown', NULL, 'Left', 'Middle'),
                (3, 'g2', NULL, NULL, 'Right', 'Unknown'),
                (4, 'g2', 'Shortstop', 'Middle', 'Middle', NULL),
                (5, 'g3', 'Catcher', 'All', 'Middle', 'Foul')
            ) AS t(event_key, game_id, recorded_location, recorded_location_side,
                   inferred_location_side, recorded_location_angle)
        ),
        {side},
        {angle}
        SELECT dimension, event_key, raw_value, mapped_value, deduced_value,
               sentinel_type, observed_status
        FROM location_side
        UNION ALL BY NAME
        SELECT dimension, event_key, raw_value, mapped_value, deduced_value,
               sentinel_type, observed_status
        FROM location_angle
        ORDER BY dimension, event_key
    """
    with duckdb.connect() as connection:
        rows = connection.execute(query).fetchall()
    assert rows == [
        ("location_angle", 1, "Default", None, None, "default", "default_code"),
        ("location_angle", 2, "Middle", None, None, "valid_value", "observed"),
        ("location_angle", 3, "Unknown", None, None, "unknown", "unknown_code"),
        ("location_angle", 4, None, None, None, "null", "missing"),
        ("location_angle", 5, "Foul", None, None, "valid_value", "observed"),
        ("location_side", 1, "First", "Right", None, "valid_value", "observed"),
        ("location_side", 2, "Unknown", None, "Left", "unknown", "derived"),
        ("location_side", 3, None, None, "Right", "null", "derived"),
        (
            "location_side",
            4,
            "Shortstop",
            "Middle",
            None,
            "valid_value",
            "observed",
        ),
        ("location_side", 5, "Catcher", "All", None, "valid_value", "observed"),
    ]


def test_default_sentinel_is_not_observed_truth() -> None:
    source = AUDIT_SQL.read_text(encoding="utf-8")
    query = source[source.index(");") + 2 :].replace("@this_model", "ledger")
    with duckdb.connect() as connection:
        connection.execute(
            "CREATE TABLE ledger(event_key INTEGER, dimension VARCHAR, "
            "sentinel_type VARCHAR, observed_status VARCHAR)"
        )
        connection.execute(
            "INSERT INTO ledger VALUES "
            "(1, 'location_angle', 'default', 'default_code'), "
            "(2, 'other_dimension', 'default', 'observed')"
        )
        assert connection.execute(query).fetchall() == []
        connection.execute(
            "UPDATE ledger SET observed_status = 'observed' WHERE event_key = 1"
        )
        assert connection.execute(query).fetchall() == [
            (1, "location_angle", "default", "observed")
        ]


def test_modeling_contract_maps_side_and_withholds_stale_inputs() -> None:
    for path in (GEOMETRY_SQL, OBSERVATION_SQL):
        source = path.read_text(encoding="utf-8")
        dl_probability = _expression(
            source,
            "CASE WHEN o.dimension = 'location_side' THEN NULL ELSE p.dl_p_class END",
            "dl_p_class",
        )
        dl_artifact = _expression(
            source,
            "CASE WHEN o.dimension = 'location_side' THEN NULL ELSE p.dl_artifact_id END",
            "dl_artifact_id",
        )
        propensity = _expression(
            source,
            "CASE WHEN o.dimension = 'location_side' THEN NULL ELSE sp.p_observed_mean END",
            "propensity_p_observed",
        )
        propensity_artifact = _expression(
            source,
            "CASE WHEN o.dimension = 'location_side' THEN NULL ELSE sp.artifact_id END",
            "propensity_artifact_id",
        )
        marker = _expression(
            source,
            "'geometry-v2-global-side'",
            "geometry_target_contract",
        )
        with duckdb.connect() as connection:
            rows = connection.execute(
                f"""
                WITH o(dimension) AS (VALUES ('location_side'), ('trajectory')),
                     p(dl_p_class, dl_artifact_id) AS (
                         VALUES ([0.25::DOUBLE, 0.75::DOUBLE], 'dl-old')
                     ),
                     sp(p_observed_mean, artifact_id) AS (
                         VALUES (0.8::DOUBLE, 'prop-old')
                     )
                SELECT o.dimension, {dl_probability}, {dl_artifact}, {propensity},
                       {propensity_artifact}, {marker}
                FROM o CROSS JOIN p CROSS JOIN sp ORDER BY o.dimension
                """
            ).fetchall()
        assert rows == [
            ("location_side", None, None, None, None, "geometry-v2-global-side"),
            (
                "trajectory",
                [0.25, 0.75],
                "dl-old",
                0.8,
                "prop-old",
                "geometry-v2-global-side",
            ),
        ]
    geometry = GEOMETRY_SQL.read_text(encoding="utf-8")
    target_class = _expression(
        geometry,
        "CASE\n        WHEN o.dimension = 'location_side'",
        "class",
    )
    with duckdb.connect() as connection:
        rows = connection.execute(
            f"""
            WITH o(dimension, observed_status, raw_value, mapped_value) AS (
                VALUES ('location_side', 'observed', 'First', 'Right'),
                       ('location_side', 'derived', 'Unknown', NULL),
                       ('trajectory', 'derived', 'Unknown', NULL),
                       ('trajectory', 'observed', 'Fly', NULL)
            )
            SELECT dimension, observed_status, {target_class}
            FROM o ORDER BY dimension, observed_status
            """
        ).fetchall()
    assert rows == [
        ("location_side", "derived", None),
        ("location_side", "observed", "Right"),
        ("trajectory", "derived", "Unknown"),
        ("trajectory", "observed", "Fly"),
    ]
