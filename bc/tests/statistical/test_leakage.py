"""Split-registry leakage checks against synthetic Parquet snapshots."""

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from python_models.statistical.leakage import (
    ALL_LEAKAGE_UNITS,
    PRIMARY_FOLD_UNIT,
    STRESS_HOLDOUT_UNITS,
    check_split_leakage,
    summarize_violations,
    violations_to_dataframe,
)


def _holdout_struct(
    *,
    scorer: bool = False,
    park: bool = False,
    alignment_regime: bool = False,
    source_acquisition_block: bool = False,
    season_block: bool = False,
    aggregate_total: bool = False,
    player_group: bool = False,
) -> dict[str, bool]:
    return {
        "is_heldout_scorer": scorer,
        "is_heldout_park": park,
        "is_heldout_alignment_regime": alignment_regime,
        "is_heldout_source_acquisition_block": source_acquisition_block,
        "is_heldout_season_block": season_block,
        "is_heldout_aggregate_total": aggregate_total,
        "is_heldout_player_group": player_group,
    }


def _clean_row(
    *,
    event_key: int,
    game_id: str,
    primary_fold: str,
    scorer: str | None = "scorer_1",
    park_id: str | None = "PARK_A",
    season: int = 2020,
    alignment_regime: str | None = "shift_growth_era",
    source_type: str | None = "retrosheet",
    fielding_team_id: str | None = "TEAM_A",
    flags: dict[str, bool] | None = None,
) -> dict[str, object]:
    return {
        "event_key": event_key,
        "game_id": game_id,
        "primary_fold": primary_fold,
        "scorer": scorer,
        "park_id": park_id,
        "season": season,
        "alignment_regime": alignment_regime,
        "source_type": source_type,
        "fielding_team_id": fielding_team_id,
        "holdout_flags": flags if flags is not None else _holdout_struct(),
    }


def _write_parquet(path: Path, rows: list[dict[str, object]]) -> None:
    schema_overrides: dict[str, pl.DataType] = {
        "event_key": pl.UInt32(),
        "season": pl.Int32(),
    }
    df = pl.DataFrame(rows, schema_overrides=schema_overrides)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(path)


def test_check_split_leakage_clean_dataset(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    rows = [
        _clean_row(event_key=i, game_id=f"G{i:05d}", primary_fold="TRAIN")
        for i in range(50)
    ]
    _write_parquet(parquet, rows)
    assert check_split_leakage(parquet) == ()


def test_check_split_leakage_detects_primary_fold_split(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    rows: list[dict[str, object]] = []
    rows.append(_clean_row(event_key=1, game_id="GAME_BAD", primary_fold="TRAIN"))
    rows.append(_clean_row(event_key=2, game_id="GAME_BAD", primary_fold="TEST"))
    rows.append(_clean_row(event_key=3, game_id="GAME_OK", primary_fold="VALIDATE"))
    _write_parquet(parquet, rows)
    violations = check_split_leakage(parquet)
    by_kind = summarize_violations(violations)
    assert by_kind.get("primary_fold") == 1
    bad = [v for v in violations if v.unit_kind == "primary_fold"]
    assert bad[0].unit_id == "GAME_BAD"
    assert set(bad[0].distinct_values) == {"TRAIN", "TEST"}
    assert bad[0].row_count == 2


def test_check_split_leakage_detects_holdout_flag_split(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    rows: list[dict[str, object]] = []
    rows.append(
        _clean_row(
            event_key=1,
            game_id="G1",
            primary_fold="TRAIN",
            scorer="scorer_evil",
            flags=_holdout_struct(scorer=True),
        )
    )
    rows.append(
        _clean_row(
            event_key=2,
            game_id="G2",
            primary_fold="TRAIN",
            scorer="scorer_evil",
            flags=_holdout_struct(scorer=False),
        )
    )
    _write_parquet(parquet, rows)
    violations = check_split_leakage(parquet)
    by_kind = summarize_violations(violations)
    assert by_kind.get("is_heldout_scorer") == 1
    bad = [v for v in violations if v.unit_kind == "is_heldout_scorer"]
    assert bad[0].unit_id == "scorer_evil"
    assert set(bad[0].distinct_values) == {"true", "false"}


def test_check_split_leakage_park_unit_is_composite(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    rows = [
        _clean_row(
            event_key=1,
            game_id="G1",
            primary_fold="TRAIN",
            park_id="PARK_X",
            season=2020,
            flags=_holdout_struct(park=True),
        ),
        _clean_row(
            event_key=2,
            game_id="G2",
            primary_fold="TRAIN",
            park_id="PARK_X",
            season=2020,
            flags=_holdout_struct(park=False),
        ),
        _clean_row(
            event_key=3,
            game_id="G3",
            primary_fold="TRAIN",
            park_id="PARK_X",
            season=2021,
            flags=_holdout_struct(park=False),
        ),
    ]
    _write_parquet(parquet, rows)
    violations = [
        v for v in check_split_leakage(parquet) if v.unit_kind == "is_heldout_park"
    ]
    assert len(violations) == 1
    assert violations[0].unit_id == "PARK_X|2020"


def test_check_split_leakage_ignores_null_unit_columns(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    rows = [
        _clean_row(
            event_key=1,
            game_id="G1",
            primary_fold="TRAIN",
            scorer=None,
            flags=_holdout_struct(scorer=False),
        ),
        _clean_row(
            event_key=2,
            game_id="G2",
            primary_fold="TRAIN",
            scorer=None,
            flags=_holdout_struct(scorer=False),
        ),
    ]
    _write_parquet(parquet, rows)
    violations = [
        v for v in check_split_leakage(parquet) if v.unit_kind == "is_heldout_scorer"
    ]
    assert violations == []


def test_check_split_leakage_skips_units_missing_columns(tmp_path: Path) -> None:
    parquet = tmp_path / "dataset.parquet"
    df = pl.DataFrame(
        {
            "event_key": [1, 2],
            "game_id": ["G1", "G1"],
            "primary_fold": ["TRAIN", "TRAIN"],
        },
        schema_overrides={"event_key": pl.UInt32()},
    )
    parquet.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(parquet)
    violations = check_split_leakage(parquet)
    assert violations == ()


def test_violations_to_dataframe_empty_returns_typed_frame() -> None:
    df = violations_to_dataframe(())
    assert df.height == 0
    assert set(df.columns) == {
        "unit_kind",
        "unit_id",
        "distinct_value_count",
        "distinct_values",
        "row_count",
    }


def test_all_leakage_units_starts_with_primary_fold() -> None:
    assert ALL_LEAKAGE_UNITS[0] is PRIMARY_FOLD_UNIT
    assert set(ALL_LEAKAGE_UNITS[1:]) == set(STRESS_HOLDOUT_UNITS)


def test_check_split_leakage_missing_parquet_raises(tmp_path: Path) -> None:
    with pytest.raises(Exception):
        _ = check_split_leakage(tmp_path / "missing.parquet")
