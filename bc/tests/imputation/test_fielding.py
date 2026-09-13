from __future__ import annotations

from collections.abc import Iterator
from typing import cast

import duckdb
import pytest
import sqlglot

from python_models.imputation.fielding import (
    OUTPUT_SCHEMA,
    FieldingCompletionConfig,
    build_fielding_completion_sql,
)


@pytest.fixture()
def connection() -> Iterator[duckdb.DuckDBPyConnection]:
    con = duckdb.connect(":memory:")
    _ = con.execute("CREATE SCHEMA fixture")
    _ = con.execute(
        """
        CREATE TABLE fixture.plays AS SELECT * FROM (VALUES
          ('G1', 1, 1, 1, 6, 'Putout'),
          ('G1', 2, 2, 1, 0, 'Putout'),
          ('G1', 3, 3, 1, 0, 'Assist'),
          ('G2', 4, 4, 1, 0, 'FieldersChoice'),
          ('OLD', 1, 5, 1, 0, 'Putout')
        ) t(game_id,event_id,event_key,sequence_id,fielding_position,fielding_play)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.events AS SELECT * FROM (VALUES
          (1,1903,'InPlayOut','GroundBall'),
          (2,1903,'InPlayOut','GroundBall'),
          (3,1903,'InPlayOut','GroundBall'),
          (4,1903,'FieldersChoice','GroundBall'),
          (5,1906,'InPlayOut','Unknown')
        ) t(event_key,season,plate_appearance_result,batted_trajectory)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.lookup AS SELECT * FROM (VALUES
          ('G1',1,1,10),('G1',2,2,10),('G1',3,3,10),('G2',4,4,20)
        ) t(game_id,event_id,event_key,personnel_fielding_key)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.personnel AS SELECT * FROM (VALUES
          ('G1',10,'first001',3),('G1',10,'second01',4),
          ('G1',10,'short001',6),('G1',10,'left0001',7),
          ('G2',20,'first002',3),('G2',20,'second02',4),
          ('G2',20,'short002',6),('G2',20,'left0002',7)
        ) t(game_id,personnel_fielding_key,player_id,fielding_position)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.imputed AS SELECT * FROM (VALUES
          (3,'second01',4,'assist',0.8),(3,'short001',6,'assist',0.2)
        ) t(event_key,player_id,fielding_position,credit_type,expected_share)
        """
    )
    _ = con.execute(
        """
        CREATE TABLE fixture.aggregate AS SELECT * FROM (VALUES
          ('G1','first001',3,'putouts','present_clean',0.0,0.0),
          ('G1','second01',4,'putouts','present_clean',0.0,0.0),
          ('G1','left0001',7,'putouts','present_clean',0.0,0.0),
          ('G1','short001',6,'putouts','present_clean',2.0,1.0)
        ) t(game_id,player_id,fielding_position,stat_name,aggregate_status,aggregate_value,residual_value)
        """
    )
    yield con
    con.close()


def _config(**changes: object) -> FieldingCompletionConfig:
    base: dict[str, object] = {
        "plays_relation": "fixture.plays",
        "events_relation": "fixture.events",
        "personnel_lookup_relation": "fixture.lookup",
        "personnel_states_relation": "fixture.personnel",
        "imputed_credit_relation": "fixture.imputed",
        "aggregate_relation": "fixture.aggregate",
    }
    base.update(changes)
    return FieldingCompletionConfig.model_validate(base)


def _rows(
    connection: duckdb.DuckDBPyConnection,
    config: FieldingCompletionConfig | None = None,
) -> duckdb.DuckDBPyRelation:
    return connection.sql(build_fielding_completion_sql(config or _config()))


def _row(rows: duckdb.DuckDBPyRelation, event_key: int) -> dict[str, object]:
    values = rows.filter(f"event_key={event_key}").fetchone()
    assert values is not None
    return {name: value for name, value in zip(rows.columns, values, strict=True)}


def test_query_schema_and_source_row_parity(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    sql = build_fielding_completion_sql(_config())
    _ = sqlglot.parse_one(sql, read="duckdb")
    description = connection.execute(f"DESCRIBE SELECT * FROM ({sql})").fetchall()
    assert tuple(row[0] for row in description) == tuple(OUTPUT_SCHEMA)
    assert _rows(connection).aggregate("count(*)").fetchone() == (5,)


def test_known_position_and_sequence_are_preserved(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    known = _row(_rows(connection), 1)
    assert known["raw_fielding_position"] == 6
    assert known["completed_fielding_position"] == 6
    assert known["player_id"] == "short001"
    assert known["sequence_id"] == 1
    assert known["completion_status"] == "observed"


def test_clean_aggregate_residual_constrains_compatible_assignment(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    constrained = _row(_rows(connection), 2)
    assert constrained["completed_fielding_position"] == 6
    assert constrained["player_id"] == "short001"
    assert constrained["candidate_positions"] == [6]
    assert constrained["candidate_probabilities"] == [1.0]
    assert (
        constrained["constraint_disposition"] == "aggregate_capacity_pending_assignment"
    )
    assert constrained["aggregate_constraint_target"] == 1.0
    assert constrained["aggregate_constraint_assigned"] is None
    assert constrained["aggregate_constraint_delta"] is None
    assert constrained["aggregate_constraint_complete"] is True


def test_noninteger_aggregate_residual_is_exposed_as_partial(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    _ = connection.execute(
        "UPDATE fixture.aggregate SET aggregate_value=1.5 WHERE player_id='short001'"
    )
    constrained = _row(_rows(connection), 2)
    assert constrained["constraint_disposition"] == "aggregate_constraint_partial"
    assert constrained["aggregate_constraint_target"] is None
    assert constrained["aggregate_constraint_assigned"] is None
    assert constrained["aggregate_constraint_delta"] is None
    assert constrained["aggregate_constraint_complete"] is False


def test_current_credit_artifact_and_empirical_fallback_are_normalized(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    rows = _rows(connection)
    artifact = _row(rows, 3)
    assert artifact["completion_method"] == "current_imputed_credit_distribution"
    assert (
        abs(sum(cast(list[float], artifact["candidate_probabilities"])) - 1.0) < 1e-12
    )
    fallback = _row(rows, 4)
    assert fallback["completion_method"] == "partially_pooled_empirical_distribution"
    assert (
        abs(sum(cast(list[float], fallback["candidate_probabilities"])) - 1.0) < 1e-12
    )
    eligible = dict(
        connection.execute(
            "SELECT fielding_position,player_id FROM fixture.personnel"
        ).fetchall()
    )
    assert eligible[fallback["completed_fielding_position"]] == fallback["player_id"]


def test_pre1910_and_missing_personnel_use_declared_broad_prior(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    historical = _row(_rows(connection), 5)
    assert historical["season"] == 1906
    assert cast(int, historical["completed_fielding_position"]) in range(1, 10)
    assert historical["player_id"] is None
    assert historical["constraint_disposition"] == "no_personnel_candidates_broad_prior"


def test_sampling_is_reproducible_and_whole_game_limited(
    connection: duckdb.DuckDBPyConnection,
) -> None:
    config = _config(sample_games=1, sample_seed="same")
    first = _rows(connection, config).order("event_key,sequence_id").fetchall()
    second = _rows(connection, config).order("event_key,sequence_id").fetchall()
    assert first == second
    assert len({row[2] for row in first}) == 1


def test_config_rejects_invalid_values() -> None:
    with pytest.raises(ValueError, match="start_season"):
        _ = FieldingCompletionConfig(start_season=2025, end_season=1903)
    with pytest.raises(ValueError, match="invalid relation"):
        _ = FieldingCompletionConfig(plays_relation="plays; DROP TABLE plays")
