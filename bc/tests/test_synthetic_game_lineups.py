from __future__ import annotations

from collections.abc import Iterable, Sequence

import polars as pl
import pytest

from python_models.synthetic_box_scores import (
    build_synthetic_batting_core,
    build_synthetic_fielding_core,
    build_synthetic_lineup_assignments,
    build_synthetic_lineup_report,
)


def _game(
    game_id: str,
    date: str,
    *,
    away_team_id: str = "AWY",
    home_team_id: str = "HOM",
    away_pitcher: str = "away_pitcher",
    home_pitcher: str = "home_pitcher",
    season: int = 1885,
    use_dh: bool = False,
) -> dict[str, object]:
    return {
        "game_id": game_id,
        "date": date,
        "season": season,
        "use_dh": use_dh,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home_starting_pitcher_id": home_pitcher,
        "away_starting_pitcher_id": away_pitcher,
    }


def _games(rows: Sequence[dict[str, object]] | None = None) -> pl.DataFrame:
    return pl.DataFrame(
        rows
        or [
            _game(
                "AAA188504010",
                "1885-04-01",
                away_pitcher="rf_modal",
            )
        ]
    )


def _lineups_for_team(
    team_id: str,
    players: dict[int, str] | None = None,
    *,
    season: int = 1885,
) -> list[dict[str, object]]:
    roster = players or {
        1: "modal_pitcher",
        2: "catcher",
        3: "first_base",
        4: "second_base",
        5: "third_base",
        6: "shortstop",
        7: "left_field",
        8: "center_field",
        9: "rf_modal",
    }
    return [
        {
            "season": season,
            "team_id": team_id,
            "lineup_position": position,
            "fielding_position": position,
            "player_id": player_id,
        }
        for position, player_id in roster.items()
    ]


def _lineups(rows: Iterable[dict[str, object]] | None = None) -> pl.DataFrame:
    return pl.DataFrame(list(rows or _lineups_for_team("AWY")))


def _candidate(
    player_id: str,
    position: int,
    games: int,
    *,
    team_id: str = "AWY",
    stint: int = 1,
    season: int = 1885,
    games_total: int | None = None,
    plate_appearances: int = 400,
    games_played: int | None = None,
    outs_played: int = 0,
) -> dict[str, object]:
    return {
        "season": season,
        "team_id": team_id,
        "player_id": player_id,
        "stint": stint,
        "fielding_position": position,
        "games_at_position": games,
        "games_total": games if games_total is None else games_total,
        "plate_appearances": plate_appearances,
        "games_played": games if games_played is None else games_played,
        "outs_played": outs_played,
    }


def _base_candidates(
    *,
    team_id: str = "AWY",
    games: int = 1,
    players: dict[int, str] | None = None,
) -> list[dict[str, object]]:
    roster = players or {
        1: "modal_pitcher",
        2: "catcher",
        3: "first_base",
        4: "second_base",
        5: "third_base",
        6: "shortstop",
        7: "left_field",
        8: "center_field",
        9: "rf_modal",
    }
    return [
        _candidate(player_id, position, games, team_id=team_id)
        for position, player_id in roster.items()
    ]


def _candidates(rows: Iterable[dict[str, object]] | None = None) -> pl.DataFrame:
    base = _base_candidates(games=100)
    base.extend(
        [
            _candidate("center_field", 9, 90, games_total=100, games_played=100),
            _candidate("right_backup", 9, 80, plate_appearances=300),
        ]
    )
    return pl.DataFrame(list(rows or base))


def test_fielding_pitcher_swap_uses_next_unoccupied_candidate() -> None:
    out = build_synthetic_fielding_core(_games(), _lineups(), _candidates())

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert away["fielder_id"].n_unique() == 9
    assert away["fielding_position"].n_unique() == 9
    assert (
        away.filter(pl.col("fielding_position") == 1).item(0, "fielder_id")
        == "rf_modal"
    )
    assert (
        away.filter(pl.col("fielding_position") == 9).item(0, "fielder_id")
        == "right_backup"
    )


def test_batting_pitcher_swap_uses_same_repaired_lineup() -> None:
    out = build_synthetic_batting_core(_games(), _lineups(), _candidates())

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert away["batter_id"].n_unique() == 9
    assert away["lineup_position"].n_unique() == 9
    assert "rf_modal" in away["batter_id"].to_list()
    assert "right_backup" in away["batter_id"].to_list()
    assert "modal_pitcher" not in away["batter_id"].to_list()


def test_batting_dh_game_does_not_insert_pitcher() -> None:
    out = build_synthetic_batting_core(
        _games([_game("AAA188504010", "1885-04-01", use_dh=True)]),
        _lineups(),
        _candidates(),
    )

    away = out.filter(pl.col("side") == "Away")

    assert away.shape[0] == 9
    assert "modal_pitcher" in away["batter_id"].to_list()
    assert "right_backup" not in away["batter_id"].to_list()


def test_modal_off_pitcher_assigned_lineup_position_nine() -> None:
    """With disable_modal=True, the pitcher must end up at lineup_position 9
    regardless of where the modal lineup placed him. Non-pitchers occupy
    lineup_position 1..8 ranked by season PA/G."""
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
        ]
    )
    out = build_synthetic_lineup_assignments(
        games,
        _lineups(),
        _candidates(),
        disable_modal=True,
    )
    away = out.filter(pl.col("side") == "Away")
    assert away.shape[0] == 9
    pitcher_row = away.filter(pl.col("fielding_position") == 1)
    assert pitcher_row.height == 1
    assert int(pitcher_row.item(0, "lineup_position")) == 9
    non_pitcher = away.filter(pl.col("fielding_position") != 1)
    assert sorted(int(p) for p in non_pitcher["lineup_position"].to_list()) == list(
        range(1, 9)
    )


def test_modal_off_two_catcher_team_allows_both_at_c_slot() -> None:
    """Two catchers with equal claims should each start once across two
    games when their per-position target is 1 each. The modal lineup pins
    one C, but disable_modal removes the bias so both can occupy the C slot."""
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows = [
        row
        for row in rows
        if not (row["player_id"] == "catcher" and row["fielding_position"] == 2)
    ]
    rows.extend(
        [
            _candidate("catcher", 2, 1, games_total=1),
            _candidate("backup_catcher", 2, 1, games_total=1),
        ]
    )

    out = build_synthetic_lineup_assignments(
        games,
        _lineups(),
        _candidates(rows),
        disable_modal=True,
    )
    away = out.filter(pl.col("side") == "Away")
    catcher_rows = away.filter(pl.col("fielding_position") == 2)
    assert catcher_rows.height == 2
    assert set(catcher_rows["player_id"].to_list()) == {"catcher", "backup_catcher"}


def test_modal_off_disable_modal_kwarg_does_not_change_default_path(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """disable_modal=False (default) must reproduce the existing behavior
    bit-for-bit. Locks in the no-op contract for the flag-off path."""
    monkeypatch.delenv("BC_OPTIMIZER_NO_MODAL", raising=False)
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
        ]
    )
    default_out = build_synthetic_lineup_assignments(
        games, _lineups(), _candidates()
    ).sort(["game_id", "side", "lineup_position"])
    explicit_off = build_synthetic_lineup_assignments(
        games, _lineups(), _candidates(), disable_modal=False
    ).sort(["game_id", "side", "lineup_position"])
    assert default_out.equals(explicit_off)


def test_solver_exactly_matches_feasible_position_targets() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows = [
        row
        for row in rows
        if not (row["player_id"] == "rf_modal" and row["fielding_position"] == 9)
    ]
    rows.extend(
        [
            _candidate("rf_modal", 9, 1, games_total=1),
            _candidate("right_backup", 9, 1, games_total=1),
        ]
    )

    out = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))
    away = out.filter(pl.col("side") == "Away")

    counts = {
        row["player_id"]: row["len"]
        for row in away.group_by("player_id").len().to_dicts()
    }
    assert counts["rf_modal"] == 1
    assert counts["right_backup"] == 1


def test_default_player_keeps_batting_slot_when_changing_position() -> None:
    rows = _base_candidates(games=1)
    rows = [
        row
        for row in rows
        if row["player_id"] not in {"catcher", "first_base"}
        or row["fielding_position"] not in {2, 3}
    ]
    rows.extend(
        [
            _candidate("catcher", 3, 1),
            _candidate("first_base", 2, 1),
        ]
    )

    out = build_synthetic_lineup_assignments(
        _games([_game("AAA188504010", "1885-04-01")]),
        _lineups(),
        _candidates(rows),
    )
    away = out.filter(pl.col("side") == "Away")

    catcher = away.filter(pl.col("player_id") == "catcher")
    first_base = away.filter(pl.col("player_id") == "first_base")
    assert catcher.item(0, "lineup_position") == 2
    assert catcher.item(0, "fielding_position") == 3
    assert first_base.item(0, "lineup_position") == 3
    assert first_base.item(0, "fielding_position") == 2


def test_starting_pitcher_excluded_from_non_pitcher_slots() -> None:
    games = _games([_game("AAA188504010", "1885-04-01", away_pitcher="catcher")])
    rows = _base_candidates(games=1)
    rows.append(_candidate("backup_catcher", 2, 1))

    out = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))
    away = out.filter(pl.col("side") == "Away")

    assert (
        away.filter(pl.col("fielding_position") == 1).item(0, "player_id") == "catcher"
    )
    assert (
        "catcher"
        not in away.filter(pl.col("fielding_position") > 1)["player_id"].to_list()
    )


def test_optimizer_falls_back_only_infeasible_game_side() -> None:
    games = _games(
        [
            _game(
                "AAA188504010",
                "1885-04-01",
                home_team_id="HOM",
                home_pitcher="h1",
            )
        ]
    )
    lineups = _lineups(
        [
            *_lineups_for_team("AWY"),
            *_lineups_for_team("HOM", {pos: f"h{pos}" for pos in range(1, 10)}),
        ]
    )
    rows = [
        row
        for row in _base_candidates(games=1)
        if not (row["player_id"] == "rf_modal" and row["fielding_position"] == 9)
    ]
    rows.append(_candidate("right_backup", 9, 1))

    out = build_synthetic_lineup_assignments(games, lineups, _candidates(rows))
    away = out.filter(pl.col("side") == "Away")
    home = out.filter(pl.col("side") == "Home")

    assert away.filter(pl.col("fielding_position") == 9).item(0, "player_id") == (
        "right_backup"
    )
    assert home.filter(pl.col("fielding_position") == 9).item(0, "player_id") == "h9"
    assert away.shape[0] == home.shape[0] == 9


def test_multi_stint_player_eligibility_follows_date_windows() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01", away_team_id="AAA"),
            _game("AAA188504020", "1885-04-02", away_team_id="AAA"),
            _game("BBB188504030", "1885-04-03", away_team_id="BBB"),
            _game("BBB188504040", "1885-04-04", away_team_id="BBB"),
        ]
    )
    lineups = _lineups(
        [
            *_lineups_for_team("AAA", {pos: f"aaa_p{pos}" for pos in range(1, 10)}),
            *_lineups_for_team("BBB", {pos: f"bbb_p{pos}" for pos in range(1, 10)}),
        ]
    )
    rows = [
        *_base_candidates(
            team_id="AAA",
            games=2,
            players={pos: f"aaa_p{pos}" for pos in range(1, 10)},
        ),
        *_base_candidates(
            team_id="BBB",
            games=2,
            players={pos: f"bbb_p{pos}" for pos in range(1, 10)},
        ),
    ]
    rows = [
        row
        for row in rows
        if not (
            row["fielding_position"] == 9 and row["player_id"] in {"aaa_p9", "bbb_p9"}
        )
    ]
    rows.extend(
        [
            _candidate("aaa_p9", 9, 1, team_id="AAA"),
            _candidate("bbb_p9", 9, 1, team_id="BBB"),
            _candidate("traded", 9, 1, team_id="AAA", stint=1),
            _candidate("traded", 9, 1, team_id="BBB", stint=2),
        ]
    )

    out = build_synthetic_lineup_assignments(games, lineups, _candidates(rows))
    traded = out.filter(pl.col("player_id") == "traded")

    assert traded.shape[0] == 2
    aaa_traded = traded.filter(pl.col("team_id") == "AAA")
    bbb_traded = traded.filter(pl.col("team_id") == "BBB")
    assert aaa_traded.shape[0] == 1
    assert bbb_traded.shape[0] == 1
    assert aaa_traded.item(0, "stint") == 1
    assert bbb_traded.item(0, "stint") == 2


def test_solver_output_is_seeded_and_deterministic() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows.append(_candidate("right_backup", 9, 1, games_total=1))

    first = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))
    second = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))

    assert first.equals(second)


def test_fielding_targets_scale_down_to_batting_games() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows = [
        row
        for row in rows
        if row["player_id"] not in {"catcher", "first_base"}
        or row["fielding_position"] not in {2, 3}
    ]
    rows.extend(
        [
            _candidate("catcher", 2, 1, games_total=1),
            _candidate("first_base", 3, 1, games_total=1),
            _candidate(
                "swing",
                2,
                2,
                games_total=2,
                games_played=2,
            ),
            _candidate(
                "swing",
                3,
                2,
                games_total=2,
                games_played=2,
            ),
        ]
    )

    out = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))
    swing = out.filter(pl.col("player_id") == "swing")

    assert swing.shape[0] == 2
    assert sorted(swing["fielding_position"].to_list()) == [2, 3]


def test_lineup_report_outputs_actual_vs_realized_errors() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01", away_pitcher="right_backup"),
        ]
    )
    rows = _base_candidates(games=1)
    rows.append(_candidate("right_backup", 9, 1, games_total=1))

    report = build_synthetic_lineup_report(games, _lineups(), _candidates(rows))
    right_backup_total = report.filter(
        (pl.col("player_id") == "right_backup") & (pl.col("metric_type") == "Total")
    )
    right_backup_position = report.filter(
        (pl.col("player_id") == "right_backup")
        & (pl.col("metric_type") == "Position")
        & (pl.col("fielding_position") == 9)
    )

    # rf_modal and right_backup share a single RF slot, so the per-position
    # target halves to 0.5. Total actual is the sum of the player's scaled
    # non-pitcher position targets, so it also lands at 0.5. right_backup is
    # the listed starting pitcher, so the optimizer excludes them from
    # non-pitcher slots and they end at realized 0.
    assert right_backup_total.item(0, "actual_games") == 0.5
    assert right_backup_total.item(0, "realized_games") == 0
    assert right_backup_total.item(0, "abs_error") == 0.5
    assert right_backup_total.item(0, "pct_error") == 0.5
    assert right_backup_position.item(0, "actual_games") == 0.5
    assert right_backup_position.item(0, "realized_games") == 0


def test_lineup_report_uses_scaled_fielding_targets() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows = [
        row
        for row in rows
        if row["player_id"] not in {"catcher", "first_base"}
        or row["fielding_position"] not in {2, 3}
    ]
    rows.extend(
        [
            _candidate("catcher", 2, 1, games_total=1),
            _candidate("first_base", 3, 1, games_total=1),
            _candidate("swing", 2, 2, games_total=2, games_played=2),
            _candidate("swing", 3, 2, games_total=2, games_played=2),
        ]
    )

    report = build_synthetic_lineup_report(games, _lineups(), _candidates(rows))
    swing_positions = report.filter(
        (pl.col("player_id") == "swing") & (pl.col("metric_type") == "Position")
    ).sort("fielding_position")
    swing_total = report.filter(
        (pl.col("player_id") == "swing") & (pl.col("metric_type") == "Total")
    )

    assert swing_total.item(0, "actual_games") == 2.0
    assert swing_total.item(0, "realized_games") == 2
    assert swing_positions["actual_games"].to_list() == [1.0, 1.0]


def test_player_with_full_game_outs_gets_at_least_one_appearance() -> None:
    games = _games(
        [
            _game("AAA188504010", "1885-04-01"),
            _game("AAA188504020", "1885-04-02"),
        ]
    )
    rows = _base_candidates(games=2)
    rows.append(
        _candidate(
            "deep_bench",
            9,
            1,
            games_total=1,
            games_played=1,
            plate_appearances=4,
            outs_played=27,
        )
    )

    out = build_synthetic_lineup_assignments(games, _lineups(), _candidates(rows))
    away = out.filter(pl.col("side") == "Away")

    deep_bench_assignments = away.filter(pl.col("player_id") == "deep_bench")
    assert deep_bench_assignments.height >= 1
