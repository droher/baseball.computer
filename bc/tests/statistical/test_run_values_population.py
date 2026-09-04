"""Population-filter and shared-cell-key tests for the Model G preps."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

from pathlib import Path

import polars as pl
import pytest

from python_models.statistical.models._run_values_data import (
    MIN_EVENTS_PER_CELL,
    POPULATION_FILTER_COLUMNS,
    filter_event_population,
    parse_cell_label,
    prepare_run_expectancy_inputs,
)
from python_models.statistical.models._state_transition_data import (
    prepare_state_transition_inputs,
)
from tests.statistical.run_values_fixtures import event_row, train_games


def _tagged(
    tag: str,
    *,
    base: int = 1,
    end_outs: int,
    end_base: int,
    result_family: str | None = "out_in_play",
    runs_on_play: int = 0,
    game_type: str = "RegularSeason",
    inning_start: int = 5,
    denominator_policy: str = "include",
) -> dict[str, object]:
    row = event_row(
        game_id="G1",
        season=1948,
        league="NAL",
        outs=0,
        base=base,
        end_outs=end_outs,
        end_base=end_base,
        result_family=result_family,
        runs_on_play=runs_on_play,
        game_type=game_type,
        inning_start=inning_start,
        denominator_policy=denominator_policy,
    )
    return {"tag": tag, **row}


def _population_frame() -> pl.DataFrame:
    rows = [
        _tagged("pa", end_outs=1, end_base=1),
        _tagged("substitution", end_outs=0, end_base=1, result_family=None),
        _tagged("stolen_base", end_outs=0, end_base=2, result_family=None),
        _tagged("caught_stealing", end_outs=1, end_base=0, result_family=None),
        _tagged(
            "balk_scores",
            base=4,
            end_outs=0,
            end_base=0,
            result_family=None,
            runs_on_play=1,
        ),
        _tagged("postseason", end_outs=1, end_base=1, game_type="WorldSeries"),
        _tagged("ninth_inning", end_outs=1, end_base=1, inning_start=9),
        _tagged("truncated", end_outs=1, end_base=1, denominator_policy="exclude"),
    ]
    return pl.DataFrame(rows)


def test_filter_drops_no_op_rows_and_keeps_state_changing_non_pa_rows() -> None:
    frame = _population_frame()
    kept = filter_event_population(frame.lazy(), label="test").collect()
    tags = set(kept.get_column("tag").to_list())

    assert "pa" in tags
    assert "stolen_base" in tags
    assert "caught_stealing" in tags
    assert "balk_scores" in tags
    assert "substitution" not in tags
    assert "postseason" not in tags
    assert "ninth_inning" not in tags
    assert "truncated" not in tags

    surviving = kept.filter(
        pl.col("result_family").is_null()
        & (pl.col("run_expectancy_start_key") == pl.col("run_expectancy_end_key"))
        & (pl.col("runs_on_play") == 0)
    )
    assert surviving.height == 0


def test_filter_is_idempotent() -> None:
    frame = _population_frame().lazy()
    once = filter_event_population(frame, label="once").collect()
    twice = filter_event_population(once.lazy(), label="twice").collect()
    assert once.equals(twice)


@pytest.mark.parametrize("missing", list(POPULATION_FILTER_COLUMNS))
def test_missing_filter_column_raises(missing: str) -> None:
    frame = _population_frame().drop(missing).lazy()
    with pytest.raises(ValueError, match=missing):
        _ = filter_event_population(frame, label="test")


def test_prep_raises_on_dataset_without_filter_columns(tmp_path: Path) -> None:
    path = tmp_path / "legacy.parquet"
    _population_frame().drop("denominator_policy").write_parquet(path)
    with pytest.raises(ValueError, match="denominator_policy"):
        _ = prepare_run_expectancy_inputs(path)
    with pytest.raises(ValueError, match="denominator_policy"):
        _ = prepare_state_transition_inputs(path)


def _shared_dataset(tmp_path: Path) -> Path:
    rows: list[dict[str, object]] = []
    cells = [
        (1905, "NL", 0, 0),
        (1948, "NAL", 1, 3),
        (2015, "AL", 2, 7),
    ]
    for season, league, outs, base in cells:
        for gid in train_games(f"{season}{league}{outs}{base}", MIN_EVENTS_PER_CELL + 3):
            rows.append(
                event_row(
                    game_id=gid,
                    season=season,
                    league=league,
                    outs=outs,
                    base=base,
                    end_outs=min(outs + 1, 3),
                    end_base=base,
                    runs_to_end=1,
                )
            )
            rows.append(
                event_row(
                    game_id=gid,
                    season=season,
                    league=league,
                    outs=outs,
                    base=base,
                    end_outs=outs,
                    end_base=base,
                    result_family=None,
                )
            )
    path = tmp_path / "shared.parquet"
    pl.DataFrame(rows).write_parquet(path)
    return path


def test_run_expectancy_and_state_transition_share_cell_keys(tmp_path: Path) -> None:
    path = _shared_dataset(tmp_path)
    re_inputs = prepare_run_expectancy_inputs(path)
    st_inputs = prepare_state_transition_inputs(path)

    assert sorted(re_inputs.cell_labels) == sorted(st_inputs.cell_labels)

    re_keys = {
        label: (
            re_inputs.season_by_cell[i],
            re_inputs.league_by_cell[i],
            re_inputs.outs_by_cell[i],
            re_inputs.base_state_by_cell[i],
        )
        for i, label in enumerate(re_inputs.cell_labels)
    }
    st_keys = {
        label: (
            st_inputs.season_by_cell[i],
            st_inputs.league_by_cell[i],
            st_inputs.start_state_by_cell[i] // 8,
            st_inputs.start_state_by_cell[i] % 8,
        )
        for i, label in enumerate(st_inputs.cell_labels)
    }
    assert re_keys == st_keys
    for label, key in re_keys.items():
        assert parse_cell_label(label) == key

    seasons_and_leagues = set(zip(re_inputs.season_by_cell, re_inputs.league_by_cell))
    assert seasons_and_leagues == {(1905, "NL"), (1948, "NAL"), (2015, "AL")}
    assert re_inputs.n_events == st_inputs.n_events


def test_cell_league_is_the_dataset_league_not_the_key_group(tmp_path: Path) -> None:
    rows = [
        event_row(
            game_id=gid,
            season=1948,
            league="NAL",
            outs=0,
            base=0,
            end_outs=1,
            end_base=0,
        )
        for gid in train_games("NAL", MIN_EVENTS_PER_CELL + 1)
    ]
    frame = pl.DataFrame(rows).with_columns(
        pl.col("run_expectancy_start_key").str.replace("NAL", "Other"),
        pl.col("run_expectancy_end_key").str.replace("NAL", "Other"),
    )
    path = tmp_path / "grouped.parquet"
    frame.write_parquet(path)

    re_inputs = prepare_run_expectancy_inputs(path)
    st_inputs = prepare_state_transition_inputs(path)
    assert re_inputs.league_by_cell == ["NAL"]
    assert st_inputs.league_by_cell == ["NAL"]
    assert re_inputs.cell_labels == st_inputs.cell_labels
