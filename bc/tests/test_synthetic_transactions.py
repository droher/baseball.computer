from __future__ import annotations

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
