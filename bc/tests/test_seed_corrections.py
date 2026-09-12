"""Shape checks on the hand-maintained franchise and game-type seeds.

Both seeds are edited by hand and applied by joins, so a bad row would
silently drop or double a team-season or a game. These checks read the
CSVs directly; no database is needed.
"""

from __future__ import annotations

import csv
import re
from datetime import date
from pathlib import Path

import yaml

SEEDS_DIR = Path(__file__).resolve().parents[1] / "seeds" / "misc"
FRANCHISES_CSV = SEEDS_DIR / "seed_franchises.csv"
OVERRIDES_CSV = SEEDS_DIR / "seed_game_type_overrides.csv"
GAME_TYPES_CSV = SEEDS_DIR / "seed_game_types.csv"
GAME_ID = re.compile(r"^[A-Z0-9]{3}\d{8}\d$")
FAR_FUTURE = date(9999, 12, 31)


def _rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="") as fh:
        return list(csv.DictReader(fh))


def _seed_columns(path: Path) -> list[str]:
    spec = yaml.safe_load(path.with_suffix(".yml").read_text())
    (seed,) = spec["seeds"]
    return [c["name"] for c in seed["columns"]]


def _span(row: dict[str, str]) -> tuple[date, date]:
    start = date.fromisoformat(row["date_start"])
    end = date.fromisoformat(row["date_end"]) if row["date_end"] else FAR_FUTURE
    return start, end


def test_seed_csv_headers_match_their_yml() -> None:
    for path in (FRANCHISES_CSV, OVERRIDES_CSV):
        with path.open(newline="") as fh:
            header = next(csv.reader(fh))
        assert header == _seed_columns(path), path.name


def test_franchise_spans_are_ordered_and_never_overlap() -> None:
    by_team: dict[str, list[tuple[date, date]]] = {}
    for row in _rows(FRANCHISES_CSV):
        start, end = _span(row)
        assert start <= end, row
        by_team.setdefault(row["team_id"], []).append((start, end))
    for team_id, spans in by_team.items():
        spans.sort()
        for (_s0, e0), (s1, _e1) in zip(spans, spans[1:], strict=False):
            assert e0 < s1, f"{team_id}: span ending {e0} overlaps span starting {s1}"
        open_ended = [s for s in spans if s[1] == FAR_FUTURE]
        assert len(open_ended) <= 1, team_id
        if open_ended:
            assert open_ended[0] == spans[-1], (
                f"{team_id}: open-ended span is not the latest"
            )


def test_every_active_team_has_exactly_one_open_span_ending_after_the_last_rename() -> (
    None
):
    rows = _rows(FRANCHISES_CSV)
    open_rows = [r for r in rows if not r["date_end"]]
    names = {(r["team_id"], r["location"], r["nickname"]) for r in open_rows}
    assert len(names) == len(open_rows)
    for row in open_rows:
        assert row["league"] or row["nickname"].endswith("All Stars"), row


def test_game_type_overrides_are_well_formed_and_change_something() -> None:
    game_types = {r["game_type"] for r in _rows(GAME_TYPES_CSV)}
    rows = _rows(OVERRIDES_CSV)
    assert rows
    ids = [r["game_id"] for r in rows]
    assert len(ids) == len(set(ids))
    for row in rows:
        assert GAME_ID.match(row["game_id"]), row["game_id"]
        assert row["game_type"] in game_types, row
        assert row["source_game_type"] in game_types, row
        assert row["game_type"] != row["source_game_type"], row
        assert row["reason"].strip(), row["game_id"]
