"""Unit tests for the Polars FSM behind event_pitching_flags."""

from __future__ import annotations

import polars as pl
import pytest

from python_models.event_locality import (
    PITCHING_FLAGS_INPUT_COLUMNS,
    compute_pitching_flags,
)


def _row(
    *,
    game_id: str,
    event_id: int,
    pitcher_id: str,
    pitching_team_starting_pitcher_id: str,
    batting_team_margin_start: int,
    batting_team_margin_end: int,
    inning_in_outs_start: int,
    runners_count_start: int,
) -> dict[str, object]:
    return {
        "game_id": game_id,
        "event_key": (hash((game_id, event_id)) & 0xFFFFFFFF),
        "event_id": event_id,
        "batting_side": "Top",
        "pitcher_id": pitcher_id,
        "pitching_team_starting_pitcher_id": pitching_team_starting_pitcher_id,
        "batting_team_margin_start": batting_team_margin_start,
        "batting_team_margin_end": batting_team_margin_end,
        "inning_in_outs_start": inning_in_outs_start,
        "runners_count_start": runners_count_start,
    }


def _build_input() -> pl.DataFrame:
    rows: list[dict[str, object]] = []

    # sg1 — clean 3-IP save (RP1 finishes with lead intact).
    for ev, p, in_outs in [
        (1, "SP1", 0),
        (2, "SP1", 15),
        (3, "RP1", 18),
        (4, "RP1", 24),
    ]:
        rows.append(
            _row(
                game_id="sg1",
                event_id=ev,
                pitcher_id=p,
                pitching_team_starting_pitcher_id="SP1",
                batting_team_margin_start=-3,
                batting_team_margin_end=-3,
                inning_in_outs_start=in_outs,
                runners_count_start=0,
            )
        )

    # sg2 — blown save by RP1 (margin tied), team retakes lead, RP1 finishes.
    sg2 = [
        ("SP1", 1, 0, -1, -1),
        ("SP1", 2, 15, -1, -1),
        ("RP1", 3, 18, -1, -1),
        ("RP1", 4, 21, -1, 0),
        ("RP1", 5, 22, 0, -1),
        ("RP1", 6, 24, -1, -1),
    ]
    for p, ev, in_outs, m_start, m_end in sg2:
        rows.append(
            _row(
                game_id="sg2",
                event_id=ev,
                pitcher_id=p,
                pitching_team_starting_pitcher_id="SP1",
                batting_team_margin_start=m_start,
                batting_team_margin_end=m_end,
                inning_in_outs_start=in_outs,
                runners_count_start=0,
            )
        )

    # sg3 — hold (RP1) + save (RP2).
    sg3 = [
        ("SP1", 1, 0, -2, -2),
        ("SP1", 2, 12, -2, -2),
        ("RP1", 3, 15, -2, -2),
        ("RP1", 4, 18, -2, -1),
        ("RP2", 5, 21, -1, -1),
        ("RP2", 6, 24, -1, -1),
    ]
    for p, ev, in_outs, m_start, m_end in sg3:
        rows.append(
            _row(
                game_id="sg3",
                event_id=ev,
                pitcher_id=p,
                pitching_team_starting_pitcher_id="SP1",
                batting_team_margin_start=m_start,
                batting_team_margin_end=m_end,
                inning_in_outs_start=in_outs,
                runners_count_start=0,
            )
        )

    # sg4 — save situation start mid-inning (RP1 enters at out 20).
    sg4 = [
        ("SP1", 1, 0, -2, -2, 0),
        ("SP1", 2, 18, -2, -2, 0),
        ("RP1", 3, 20, -2, -2, 1),
        ("RP1", 4, 24, -2, -2, 0),
    ]
    for p, ev, in_outs, m_start, m_end, runners in sg4:
        rows.append(
            _row(
                game_id="sg4",
                event_id=ev,
                pitcher_id=p,
                pitching_team_starting_pitcher_id="SP1",
                batting_team_margin_start=m_start,
                batting_team_margin_end=m_end,
                inning_in_outs_start=in_outs,
                runners_count_start=runners,
            )
        )

    schema = {
        "game_id": pl.String,
        "event_key": pl.UInt32,
        "event_id": pl.UInt8,
        "batting_side": pl.String,
        "pitcher_id": pl.String,
        "pitching_team_starting_pitcher_id": pl.String,
        "batting_team_margin_start": pl.Int8,
        "batting_team_margin_end": pl.Int8,
        "inning_in_outs_start": pl.UInt8,
        "runners_count_start": pl.UInt8,
    }
    df = pl.DataFrame(rows, schema=schema)
    assert set(PITCHING_FLAGS_INPUT_COLUMNS).issubset(df.columns)
    return df


@pytest.fixture(scope="module")
def flags() -> pl.DataFrame:
    return compute_pitching_flags(_build_input()).sort(["game_id", "event_id"])


def _row_at(flags: pl.DataFrame, game_id: str, event_id: int) -> dict[str, object]:
    sel = flags.filter(
        (pl.col("game_id") == game_id) & (pl.col("event_id") == event_id)
    )
    assert sel.height == 1, f"no row {game_id}/{event_id}"
    return sel.row(0, named=True)


def test_clean_three_inning_save(flags: pl.DataFrame) -> None:
    enter = _row_at(flags, "sg1", 3)
    assert enter["new_relief_pitcher_flag"] is True
    assert enter["starting_pitcher_exit_flag"] is True
    assert enter["save_situation_start_flag"] is True
    assert enter["inherited_runners"] == 0
    assert enter["save_flag"] is False
    assert enter["blown_save_flag"] is False
    assert enter["hold_flag"] is False

    finish = _row_at(flags, "sg1", 4)
    assert finish["pitcher_finish_flag"] is True
    assert finish["pitcher_exit_flag"] is False
    assert finish["save_flag"] is True
    assert finish["hold_flag"] is False
    assert finish["blown_save_flag"] is False
    assert finish["blown_long_save_flag"] is False


def test_blown_save_then_finish_with_lead(flags: pl.DataFrame) -> None:
    enter = _row_at(flags, "sg2", 3)
    assert enter["save_situation_start_flag"] is True
    assert enter["blown_save_flag"] is False

    blown = _row_at(flags, "sg2", 4)
    assert blown["blown_save_flag"] is True
    assert blown["save_flag"] is False

    after_blow = _row_at(flags, "sg2", 5)
    assert after_blow["blown_save_flag"] is False, "blown_save fires only once"

    finish = _row_at(flags, "sg2", 6)
    assert finish["pitcher_finish_flag"] is True
    assert finish["save_flag"] is True
    assert finish["blown_save_flag"] is False


def test_hold(flags: pl.DataFrame) -> None:
    enter_rp1 = _row_at(flags, "sg3", 3)
    assert enter_rp1["new_relief_pitcher_flag"] is True
    assert enter_rp1["starting_pitcher_exit_flag"] is True
    assert enter_rp1["save_situation_start_flag"] is True
    assert enter_rp1["hold_flag"] is False

    exit_rp1 = _row_at(flags, "sg3", 4)
    assert exit_rp1["pitcher_exit_flag"] is True
    assert exit_rp1["pitcher_finish_flag"] is False
    assert exit_rp1["hold_flag"] is True
    assert exit_rp1["save_flag"] is False
    assert exit_rp1["blown_save_flag"] is False

    finish_rp2 = _row_at(flags, "sg3", 6)
    assert finish_rp2["pitcher_finish_flag"] is True
    assert finish_rp2["save_flag"] is True
    assert finish_rp2["hold_flag"] is False


def test_save_situation_start_mid_inning(flags: pl.DataFrame) -> None:
    enter = _row_at(flags, "sg4", 3)
    # RP1 entered at out 20 (mid-7th, %3 != 0). save_situation_1 still True
    # because inning_in_outs_start <= 24.
    assert enter["save_situation_start_flag"] is True
    assert enter["new_relief_pitcher_flag"] is True
    assert enter["inherited_runners"] == 1


def test_runner_counts_are_uint8(flags: pl.DataFrame) -> None:
    assert flags.schema["inherited_runners"] == pl.UInt8
    assert flags.schema["bequeathed_runners"] == pl.UInt8


def test_no_null_flags(flags: pl.DataFrame) -> None:
    bool_cols = [
        "starting_pitcher_flag",
        "new_relief_pitcher_flag",
        "pitcher_exit_flag",
        "pitcher_finish_flag",
        "starting_pitcher_exit_flag",
        "starting_pitcher_early_exit_flag",
        "save_situation_start_flag",
        "hold_flag",
        "save_flag",
        "blown_save_flag",
        "blown_long_save_flag",
    ]
    for c in bool_cols:
        assert flags[c].null_count() == 0, c
