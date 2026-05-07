"""Retrosheet tranDB.zip ad-hoc consumer for stint-window dating.

Step 4 lite of the synthetic-lineup-algorithm-improvements plan: rather
than register `retrosheet.transactions` as a SQLMesh external source, we
fetch tranDB.zip on the fly and parse it into a list of player→team
movements. The optimizer's stint-window builder consumes those to tighten
multi-stint date windows when a real transaction date is available.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
import csv
import io
import logging
import urllib.request
import zipfile
from pathlib import Path

_LOG = logging.getLogger(__name__)

_TRAN_DB_URL = "https://www.retrosheet.org/transactions/tranDB.zip"
_TRAN_DB_INNER = "tran.txt"

_TEAM_CHANGE_TYPES: frozenset[str] = frozenset(
    {
        "T",
        "Tn",
        "Tp",
        "P",
        "A",
        "F",
        "W",
        "Wf",
    }
)


@dataclass(frozen=True)
class TeamChange:
    """A real-MLB team transition for a player on a known calendar day."""

    season: int
    player_id: str
    primary_date: str
    from_team: str
    to_team: str
    txn_type: str


def fetch_tran_db(cache_path: Path) -> Path:
    """Download tranDB.zip if not cached, return the local path."""
    if not cache_path.exists():
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        _LOG.info("downloading %s -> %s", _TRAN_DB_URL, cache_path)
        _, _ = urllib.request.urlretrieve(_TRAN_DB_URL, cache_path)
    return cache_path


def _is_full_date(yyyymmdd: str) -> bool:
    if len(yyyymmdd) != 8:
        return False
    if not yyyymmdd.isdigit():
        return False
    return yyyymmdd[4:6] != "00" and yyyymmdd[6:8] != "00"


def parse_team_changes(zip_path: Path) -> list[TeamChange]:
    """Yield per-player team transitions with day-precision dates only."""
    out: list[TeamChange] = []
    with zipfile.ZipFile(zip_path) as zf:
        with zf.open(_TRAN_DB_INNER) as raw:
            text = io.TextIOWrapper(raw, encoding="latin-1", newline="")
            reader = csv.reader(text)
            for row in reader:
                if len(row) < 12:
                    continue
                primary_date = row[0]
                player = row[6]
                txn_type = row[7].strip()
                from_team = row[8]
                to_team = row[10]
                if not player or len(player) != 8:
                    continue
                if txn_type not in _TEAM_CHANGE_TYPES:
                    continue
                if not from_team or not to_team or from_team == to_team:
                    continue
                if not _is_full_date(primary_date):
                    continue
                out.append(
                    TeamChange(
                        season=int(primary_date[:4]),
                        player_id=player,
                        primary_date=f"{primary_date[:4]}-{primary_date[4:6]}-{primary_date[6:8]}",
                        from_team=from_team,
                        to_team=to_team,
                        txn_type=txn_type,
                    )
                )
    return out


def _team_change_iso_dates_by_player_season(
    team_changes: Iterable[TeamChange],
) -> dict[tuple[int, str], list[TeamChange]]:
    grouped: dict[tuple[int, str], list[TeamChange]] = defaultdict(list)
    for tc in team_changes:
        grouped[(tc.season, tc.player_id)].append(tc)
    for txns in grouped.values():
        txns.sort(key=lambda t: t.primary_date)
    return grouped


def transaction_stint_windows(
    *,
    candidate_stints: Iterable[tuple[int, str, str, int]],
    season_dates: dict[int, list[str]],
    team_changes: Iterable[TeamChange],
) -> dict[tuple[int, str, str, int], tuple[int, int]]:
    """Map (season, team_id, player_id, stint) to (start_index, end_index).

    For each multi-stint (player, season) where Lahman shows >1 team rows,
    we order the stints by their Lahman stint number and align them to the
    sorted transaction transitions that day-bound the player's team
    sequence. When the team sequence and transaction sequence agree,
    we emit tight date-bounded windows; otherwise we leave the player out
    so the caller falls back to proportional allocation.

    `season_dates` maps each season to the sorted list of date keys
    actually observed in the game-side input (so indices align with
    the optimizer's existing season_index).
    """
    stints_by_player_season: dict[tuple[int, str], list[tuple[int, str]]] = defaultdict(list)
    for season, team_id, player_id, stint in candidate_stints:
        entry = (stint, team_id)
        bucket = stints_by_player_season[(season, player_id)]
        if entry not in bucket:
            bucket.append(entry)

    txns_by_player_season = _team_change_iso_dates_by_player_season(team_changes)

    out: dict[tuple[int, str, str, int], tuple[int, int]] = {}
    for (season, player_id), stints in stints_by_player_season.items():
        if len(stints) < 2:
            continue
        ordered_dates = season_dates.get(season)
        if not ordered_dates:
            continue
        n = len(ordered_dates)
        stints.sort(key=lambda x: x[0])
        team_sequence = [team_id for _, team_id in stints]
        txns = list(txns_by_player_season.get((season, player_id), []))
        boundaries = _match_boundaries(team_sequence, txns)
        if boundaries is None:
            continue
        boundaries_with_terminal: list[str | None] = [*boundaries, None]
        cursor = 0
        for stint_index, ((stint_num, team_id), boundary) in enumerate(
            zip(stints, boundaries_with_terminal, strict=True)
        ):
            if boundary is None:
                end_index = n - 1
            else:
                # Include the trade date in both the outgoing and incoming
                # windows so the MILP can allocate that game-day to either
                # team without us needing the trade's intra-day time field.
                end_index = _last_index_at_or_before(ordered_dates, boundary)
                if end_index < cursor:
                    end_index = cursor
            start_index = cursor
            if end_index < start_index:
                end_index = start_index
            if end_index >= n:
                end_index = n - 1
            out[(season, team_id, player_id, stint_num)] = (start_index, end_index)
            if stint_index < len(stints) - 1 and boundary is not None:
                next_start = _first_index_at_or_after(ordered_dates, boundary)
                cursor = max(cursor, next_start)
    return out


def _match_boundaries(
    team_sequence: list[str],
    txns: list[TeamChange],
) -> list[str] | None:
    """Return primary_dates separating each consecutive (from, to) pair.

    Greedy left-to-right walk over txns. For each consecutive pair
    (team_sequence[i], team_sequence[i+1]) we look for the next txn
    matching from_team==team_sequence[i] and to_team==team_sequence[i+1].
    If we can't find one, return None (caller falls back).

    Bails on repeated team_ids in the sequence: if Lahman compresses a
    real round-trip (A → B → A → B) into a 2-stint [A, B], the greedy
    match would pick the earliest A→B date and lose the intervening
    appearances. Falling back to proportional is safer.
    """
    needed = len(team_sequence) - 1
    if needed == 0:
        return []
    if len(set(team_sequence)) != len(team_sequence):
        return None
    boundaries: list[str] = []
    txn_idx = 0
    for i in range(needed):
        from_t = team_sequence[i]
        to_t = team_sequence[i + 1]
        match: TeamChange | None = None
        while txn_idx < len(txns):
            tc = txns[txn_idx]
            txn_idx += 1
            if tc.from_team == from_t and tc.to_team == to_t:
                match = tc
                break
        if match is None:
            return None
        boundaries.append(match.primary_date)
    return boundaries


def _last_index_at_or_before(
    ordered_dates: list[str],
    cutoff_iso: str,
) -> int:
    """Last position whose date <= cutoff_iso (lexicographic ISO compare)."""
    if not ordered_dates:
        return -1
    if cutoff_iso < ordered_dates[0]:
        return -1
    if cutoff_iso >= ordered_dates[-1]:
        return len(ordered_dates) - 1
    lo, hi = 0, len(ordered_dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if ordered_dates[mid] <= cutoff_iso:
            lo = mid + 1
        else:
            hi = mid
    return lo - 1


def _first_index_at_or_after(
    ordered_dates: list[str],
    cutoff_iso: str,
) -> int:
    if not ordered_dates:
        return 0
    if cutoff_iso <= ordered_dates[0]:
        return 0
    if cutoff_iso > ordered_dates[-1]:
        return len(ordered_dates)
    lo, hi = 0, len(ordered_dates)
    while lo < hi:
        mid = (lo + hi) // 2
        if ordered_dates[mid] < cutoff_iso:
            lo = mid + 1
        else:
            hi = mid
    return lo
