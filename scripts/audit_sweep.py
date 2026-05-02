"""Suggest audit additions for SQLMesh SQL models.

Walks bc/models/**/*.sql, parses the leading MODEL block, infers grain,
column set, and existing audits. Emits suggested audits per model: not_null
on grain, unique_values on grain (FULL/INCREMENTAL only), valid_baseball_season
on year-shaped cols, relationships on FK cols.

Dry-run only — prints suggestions, does not modify files.

Usage:
    uv run --group migration python scripts/audit_sweep.py [glob...]
        # default glob: bc/models/intermediate/event_level/*.sql
"""

from __future__ import annotations

import re
import sys
from dataclasses import dataclass
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DEFAULT_GLOB = "bc/models/intermediate/event_level/*.sql"

PEOPLE_FK = {
    "player_id", "batter_id", "pitcher_id", "responsible_pitcher_id",
    "responsible_batter_id", "fielder_id", "runner_id",
    "pinch_hitter_id", "pinch_runner_id",
}
TEAM_FK = {"team_id", "batting_team_id", "fielding_team_id", "home_team_id", "away_team_id"}
PARK_FK = {"park_id"}
EVENT_FK = {"event_key"}
GAME_FK = {"game_id"}

YEAR_COLS = {"season", "birth_year", "death_year", "debut_year", "final_year"}


@dataclass
class ModelInfo:
    path: Path
    name: str
    kind: str
    grain: list[str]
    columns: dict[str, str]
    existing_audits_text: str  # raw text of audits ( ... ) block, "" if none


def parse_model(text: str, path: Path) -> ModelInfo | None:
    m = re.search(r"MODEL\s*\((.*?)\n\s*\);", text, re.DOTALL)
    if not m:
        return None
    body = m.group(1)

    name_match = re.search(r"\bname\s+([\w.]+)", body)
    name = name_match.group(1) if name_match else ""
    kind_match = re.search(r"\bkind\s+([A-Z_]+)", body)
    kind = kind_match.group(1) if kind_match else ""

    grain: list[str] = []
    g = re.search(r"\bgrain\s*\(([^)]*)\)", body)
    if g:
        grain = [c.strip() for c in g.group(1).split(",") if c.strip()]

    columns: dict[str, str] = {}
    c = re.search(r"\bcolumns\s*\(\s*(.*?)\n\s*\)", body, re.DOTALL)
    if c:
        for line in c.group(1).splitlines():
            line = line.strip().rstrip(",")
            if not line:
                continue
            parts = line.split(None, 1)
            if len(parts) == 2:
                columns[parts[0]] = parts[1]

    cd = re.search(r"\bcolumn_descriptions\s*\(\s*(.*?)\n\s*\)", body, re.DOTALL)
    if cd:
        for line in cd.group(1).splitlines():
            m = re.match(r"\s*(\w+)\s*=", line)
            if m:
                columns.setdefault(m.group(1), "UNKNOWN")

    existing = ""
    a = re.search(r"\baudits\s*\(\s*(.*?)\n\s*\)", body, re.DOTALL)
    if a:
        existing = a.group(1)

    return ModelInfo(
        path=path, name=name, kind=kind, grain=grain,
        columns=columns, existing_audits_text=existing,
    )


def fk_target(col: str) -> tuple[str, str] | None:
    """Return (to_model, to_column) for a FK column, or None."""
    if col in PEOPLE_FK:
        return ("main_models.people", "player_id")
    if col in TEAM_FK:
        return ("main_seeds.seed_franchises", "team_id")
    if col in PARK_FK:
        return ("main_models.stg_parks", "park_id")
    if col in EVENT_FK:
        return ("main_models.stg_events", "event_key")
    if col in GAME_FK:
        return ("main_models.game_results", "game_id")
    return None


def has_audit(text: str, audit_name: str, col_hint: str = "") -> bool:
    if audit_name not in text:
        return False
    if not col_hint:
        return True
    return col_hint in text


def suggest(info: ModelInfo) -> list[str]:
    out: list[str] = []
    existing = info.existing_audits_text

    if info.grain and info.kind in {"FULL", "INCREMENTAL_BY_TIME_RANGE", "INCREMENTAL_BY_UNIQUE_KEY", "VIEW"}:
        gtuple = ", ".join(info.grain)
        if not has_audit(existing, "not_null", info.grain[0]):
            out.append(f"not_null(columns := ({gtuple}))")
        if info.kind in {"FULL", "VIEW"} and not has_audit(existing, "unique_values", info.grain[0]):
            out.append(f"unique_values(columns := ({gtuple}))")

    cols = set(info.columns) | set(info.grain)
    for col in sorted(cols & YEAR_COLS):
        if not has_audit(existing, "valid_baseball_season", col):
            out.append(f"valid_baseball_season(column := {col})")

    for col in sorted(cols):
        target = fk_target(col)
        if not target:
            continue
        if has_audit(existing, "relationships", col):
            continue
        to_model, to_col = target
        if to_model.endswith(f".{info.name.split('.')[-1]}"):
            continue
        out.append(
            f"relationships(column := {col}, to_model := {to_model}, to_column := {to_col})"
        )

    return out


def render(info: ModelInfo, suggestions: list[str]) -> str:
    rel = info.path.relative_to(REPO)
    if not suggestions:
        return f"# {rel}  [{info.name}]  no suggestions\n"
    lines = [f"# {rel}  [{info.name}]  kind={info.kind}  grain=({', '.join(info.grain)})"]
    if info.existing_audits_text:
        lines.append(f"#   existing audits: {info.existing_audits_text.strip()[:200]}")
    lines.append("  audits (")
    for s in suggestions:
        lines.append(f"    {s},")
    lines.append("  ),")
    return "\n".join(lines) + "\n"


def main() -> int:
    globs = sys.argv[1:] or [DEFAULT_GLOB]
    paths: list[Path] = []
    for g in globs:
        paths.extend(sorted(REPO.glob(g)))
    if not paths:
        print(f"no files matched: {globs}", file=sys.stderr)
        return 1

    for path in paths:
        info = parse_model(path.read_text(), path)
        if not info:
            print(f"# {path.relative_to(REPO)}  [no MODEL block parsed]")
            continue
        print(render(info, suggest(info)))

    return 0


if __name__ == "__main__":
    sys.exit(main())
