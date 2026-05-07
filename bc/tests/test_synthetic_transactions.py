from __future__ import annotations

from python_models.synthetic_box_scores.game_lineups import (
    _Candidate,
    _GameSide,
    _StintWindow,
    _build_stint_windows,
)
from python_models.synthetic_box_scores.transactions import (
    TeamChange,
    _last_index_at_or_before,
    _match_boundaries,
    transaction_stint_windows,
)


def _ordered_dates() -> list[str]:
    return [
        "1900-04-19",
        "1900-05-08",
        "1900-06-15",
        "1900-07-04",
        "1900-08-22",
        "1900-09-30",
    ]


def test_last_index_at_or_before_picks_inclusive_match() -> None:
    dates = _ordered_dates()
    assert _last_index_at_or_before(dates, "1900-04-18") == -1
    assert _last_index_at_or_before(dates, "1900-04-19") == 0
    assert _last_index_at_or_before(dates, "1900-06-15") == 2
    assert _last_index_at_or_before(dates, "1900-06-16") == 2
    assert _last_index_at_or_before(dates, "1900-12-01") == len(dates) - 1


def test_match_boundaries_returns_dates_for_clean_team_chain() -> None:
    txns = [
        TeamChange(
            season=1900,
            player_id="abcde001",
            primary_date="1900-06-15",
            from_team="BOS",
            to_team="CHI",
            txn_type="T",
        ),
        TeamChange(
            season=1900,
            player_id="abcde001",
            primary_date="1900-08-01",
            from_team="CHI",
            to_team="NYA",
            txn_type="P",
        ),
    ]
    boundaries = _match_boundaries(["BOS", "CHI", "NYA"], txns)
    assert boundaries == ["1900-06-15", "1900-08-01"]


def test_match_boundaries_returns_none_when_chain_breaks() -> None:
    txns = [
        TeamChange(
            season=1900,
            player_id="abcde001",
            primary_date="1900-06-15",
            from_team="BOS",
            to_team="DET",
            txn_type="T",
        ),
    ]
    assert _match_boundaries(["BOS", "CHI"], txns) is None


def test_transaction_stint_windows_emits_tight_dates_for_two_stint_player() -> None:
    season_dates = {1900: _ordered_dates()}
    txns = [
        TeamChange(
            season=1900,
            player_id="abcde001",
            primary_date="1900-06-15",
            from_team="BOS",
            to_team="CHI",
            txn_type="T",
        ),
    ]
    candidate_stints = [
        (1900, "BOS", "abcde001", 1),
        (1900, "CHI", "abcde001", 2),
    ]
    windows = transaction_stint_windows(
        candidate_stints=candidate_stints,
        season_dates=season_dates,
        team_changes=txns,
    )
    # Both windows include the trade date (index 2 = 1900-06-15) so the
    # MILP can allocate that game-day to either team.
    assert windows[(1900, "BOS", "abcde001", 1)] == (0, 2)
    assert windows[(1900, "CHI", "abcde001", 2)] == (2, len(_ordered_dates()) - 1)


def test_transaction_stint_windows_skips_single_stint_players() -> None:
    season_dates = {1900: _ordered_dates()}
    candidate_stints = [(1900, "BOS", "single0001", 1)]
    windows = transaction_stint_windows(
        candidate_stints=candidate_stints,
        season_dates=season_dates,
        team_changes=[],
    )
    assert windows == {}


def test_transaction_stint_windows_skips_when_chain_does_not_match() -> None:
    season_dates = {1900: _ordered_dates()}
    txns = [
        TeamChange(
            season=1900,
            player_id="zzz00001",
            primary_date="1900-06-15",
            from_team="BOS",
            to_team="DET",
            txn_type="T",
        ),
    ]
    candidate_stints = [
        (1900, "BOS", "zzz00001", 1),
        (1900, "CHI", "zzz00001", 2),
    ]
    windows = transaction_stint_windows(
        candidate_stints=candidate_stints,
        season_dates=season_dates,
        team_changes=txns,
    )
    assert windows == {}


def test_transaction_stint_windows_skips_round_trip_team_sequences() -> None:
    season_dates = {1900: _ordered_dates()}
    txns = [
        TeamChange(
            season=1900,
            player_id="rt001x01",
            primary_date="1900-05-08",
            from_team="BOS",
            to_team="CHI",
            txn_type="T",
        ),
        TeamChange(
            season=1900,
            player_id="rt001x01",
            primary_date="1900-08-22",
            from_team="CHI",
            to_team="BOS",
            txn_type="T",
        ),
    ]
    candidate_stints = [
        (1900, "BOS", "rt001x01", 1),
        (1900, "CHI", "rt001x01", 2),
        (1900, "BOS", "rt001x01", 3),
    ]
    windows = transaction_stint_windows(
        candidate_stints=candidate_stints,
        season_dates=season_dates,
        team_changes=txns,
    )
    assert windows == {}


def _candidate_for_stint(
    season: int,
    team_id: str,
    player_id: str,
    stint: int,
    *,
    games: int = 50,
) -> _Candidate:
    return _Candidate(
        season=season,
        team_id=team_id,
        player_id=player_id,
        stint=stint,
        fielding_position=2,
        games_at_position=games,
        games_total=games,
        plate_appearances=games * 3,
        games_played=games,
        outs_played=games * 27,
    )


def _game_side_at(index: int, date_iso: str, season: int, team_id: str) -> _GameSide:
    return _GameSide(
        index=index,
        game_id=f"GAM{season}{index:04d}",
        date_key=(date_iso, "0"),
        season=season,
        team_id=team_id,
        side="Home",
        starting_pitcher_id=None,
        use_dh=False,
    )


def test_build_stint_windows_consumes_transaction_override_for_keyed_stint() -> None:
    season = 1900
    dates = _ordered_dates()
    bos_sides = [
        _game_side_at(i, d, season, "BOS")
        for i, d in enumerate(dates[:3])
    ]
    chi_sides = [
        _game_side_at(10 + i, d, season, "CHI")
        for i, d in enumerate(dates[3:])
    ]
    candidates = [
        _candidate_for_stint(season, "BOS", "abcde001", 1, games=2),
        _candidate_for_stint(season, "CHI", "abcde001", 2, games=2),
    ]

    no_override_index, no_override_windows = _build_stint_windows(
        bos_sides + chi_sides,
        candidates,
    )

    override = {
        (season, "BOS", "abcde001", 1): (0, 0),
        (season, "CHI", "abcde001", 2): (5, 5),
    }
    overridden_index, overridden_windows = _build_stint_windows(
        bos_sides + chi_sides,
        candidates,
        transaction_windows=override,
    )

    assert no_override_index == overridden_index
    assert overridden_windows[(season, "BOS", "abcde001", 1)] == _StintWindow(
        start_index=0, end_index=0
    )
    assert overridden_windows[(season, "CHI", "abcde001", 2)] == _StintWindow(
        start_index=5, end_index=5
    )
    assert no_override_windows[(season, "BOS", "abcde001", 1)] != overridden_windows[
        (season, "BOS", "abcde001", 1)
    ]


def test_build_stint_windows_clamps_out_of_range_override() -> None:
    season = 1900
    dates = _ordered_dates()
    sides = [
        _game_side_at(i, d, season, "BOS")
        for i, d in enumerate(dates[:3])
    ] + [
        _game_side_at(10 + i, d, season, "CHI")
        for i, d in enumerate(dates[3:])
    ]
    candidates = [
        _candidate_for_stint(season, "BOS", "clamp0001", 1),
        _candidate_for_stint(season, "CHI", "clamp0001", 2),
    ]
    n = len(dates)

    override = {
        (season, "BOS", "clamp0001", 1): (-5, 99),
    }
    _, windows = _build_stint_windows(
        sides,
        candidates,
        transaction_windows=override,
    )
    assert windows[(season, "BOS", "clamp0001", 1)] == _StintWindow(
        start_index=0, end_index=n - 1
    )
