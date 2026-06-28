"""Diff two data-coverage baseline JSON snapshots.

Reads baselines produced by ``scripts/baseline_data_coverage.py`` and emits
a Markdown summary of scalar + grouped-query deltas. Used to check that
Phase-1 ledger snapshots match the most recent committed baseline (Phase-1
exit-gate items 247 + 249).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent))

from python_models.statistical.config import BASELINE_ROOT, resolve_db_path
from python_models.statistical.logging import configure as configure_logging

from baseline_data_coverage import collect_baseline

_log = logging.getLogger("compare_baseline")

_COUNT_COLUMNS: tuple[str, ...] = ("row_count", "team_season_count", "game_count")


def _load(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _list_baselines(baseline_dir: Path) -> list[Path]:
    if not baseline_dir.exists():
        return []
    return sorted(
        (p for p in baseline_dir.glob("baseline_*.json") if p.is_file()),
        key=lambda p: p.stat().st_mtime,
    )


def _find_baseline_by_id(baseline_dir: Path, baseline_id: str) -> Path:
    for path in _list_baselines(baseline_dir):
        payload = _load(path)
        if payload.get("baseline_id") == baseline_id:
            return path
    raise FileNotFoundError(
        f"baseline_id {baseline_id!r} not found under {baseline_dir}"
    )


def _scalar_deltas(left: dict[str, Any], right: dict[str, Any]) -> list[dict[str, Any]]:
    keys = sorted(set(left) | set(right))
    rows: list[dict[str, Any]] = []
    for key in keys:
        l_entry = left.get(key) or {}
        r_entry = right.get(key) or {}
        if l_entry.get("result") == r_entry.get("result") and l_entry.get(
            "error"
        ) == r_entry.get("error"):
            continue
        rows.append(
            {
                "label": key,
                "left": l_entry.get("result"),
                "right": r_entry.get("result"),
                "left_error": l_entry.get("error"),
                "right_error": r_entry.get("error"),
            }
        )
    return rows


def _row_key(row: dict[str, Any]) -> tuple[tuple[str, Any], ...]:
    return tuple(sorted((k, v) for k, v in row.items() if k not in _COUNT_COLUMNS))


def _row_count(row: dict[str, Any] | None) -> Any:
    if row is None:
        return None
    for key in _COUNT_COLUMNS:
        if key in row:
            return row[key]
    return None


def _grouped_deltas(
    left: dict[str, Any], right: dict[str, Any]
) -> list[dict[str, Any]]:
    keys = sorted(set(left) | set(right))
    deltas: list[dict[str, Any]] = []
    for key in keys:
        l_entry = left.get(key) or {}
        r_entry = right.get(key) or {}
        l_rows: list[dict[str, Any]] = list(l_entry.get("result") or [])
        r_rows: list[dict[str, Any]] = list(r_entry.get("result") or [])
        l_index = {_row_key(r): r for r in l_rows}
        r_index = {_row_key(r): r for r in r_rows}
        per_query: list[dict[str, Any]] = []
        for rk in sorted(set(l_index) | set(r_index)):
            l_count = _row_count(l_index.get(rk))
            r_count = _row_count(r_index.get(rk))
            if l_count == r_count and rk in l_index and rk in r_index:
                continue
            per_query.append(
                {"row_key": dict(rk), "left_count": l_count, "right_count": r_count}
            )
        if per_query:
            deltas.append({"label": key, "diffs": per_query})
    return deltas


def _render_markdown(
    *,
    left_meta: dict[str, Any],
    right_meta: dict[str, Any],
    scalar_diffs: list[dict[str, Any]],
    grouped_diffs: list[dict[str, Any]],
) -> str:
    lines: list[str] = []
    lines.append(f"# Baseline diff: {left_meta['label']} -> {right_meta['label']}")
    lines.append("")
    lines.append(
        f"- Left:  `{left_meta['source']}` (sha {left_meta.get('git_sha_short', '?')})"
    )
    lines.append(
        f"- Right: `{right_meta['source']}` (sha {right_meta.get('git_sha_short', '?')})"
    )
    lines.append("")
    lines.append("## Scalar deltas")
    if not scalar_diffs:
        lines.append("(none)")
    else:
        lines.append("| query | left | right | left_error | right_error |")
        lines.append("|-------|------|-------|------------|-------------|")
        for d in scalar_diffs:
            lines.append(
                f"| `{d['label']}` | {d['left']} | {d['right']} | {d.get('left_error') or ''} | {d.get('right_error') or ''} |"
            )
    lines.append("")
    lines.append("## Grouped deltas")
    if not grouped_diffs:
        lines.append("(none)")
    else:
        for query in grouped_diffs:
            lines.append(f"### `{query['label']}`")
            lines.append("| row_key | left | right |")
            lines.append("|---------|------|-------|")
            for diff in query["diffs"]:
                rk = ", ".join(f"{k}={v}" for k, v in diff["row_key"].items())
                lines.append(
                    f"| {rk or '(empty)'} | {diff['left_count']} | {diff['right_count']} |"
                )
            lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def _summary_label(payload: dict[str, Any], source: str) -> dict[str, Any]:
    return {
        "label": payload.get("baseline_id", "current"),
        "source": source,
        "git_sha_short": payload.get("git_sha_short"),
        "generated_at": payload.get("generated_at"),
    }


def _compare(
    left: dict[str, Any], right: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    l_queries = left.get("queries") or {}
    r_queries = right.get("queries") or {}
    scalar_diffs = _scalar_deltas(
        l_queries.get("scalar_queries") or {},
        r_queries.get("scalar_queries") or {},
    )
    grouped_diffs = _grouped_deltas(
        l_queries.get("grouped_queries") or {},
        r_queries.get("grouped_queries") or {},
    )
    return scalar_diffs, grouped_diffs


def _collect_current() -> dict[str, Any]:
    db_path = resolve_db_path()
    if not db_path.exists():
        raise FileNotFoundError(f"DuckDB file not found at {db_path}")
    _log.info("compare_collect_current db_path=%s", db_path)
    return {"queries": collect_baseline(db_path), "baseline_id": "live"}


def _resolve_pair(
    args: argparse.Namespace,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    baseline_dir = Path(args.baseline_dir)
    if args.against_current:
        baselines = _list_baselines(baseline_dir)
        if not baselines:
            raise FileNotFoundError(f"No baselines under {baseline_dir}")
        left_path = baselines[-1]
        return _load(left_path), _collect_current(), str(left_path), "<live DB>"
    if args.baseline_id:
        right_path = _find_baseline_by_id(baseline_dir, args.baseline_id)
        others = [p for p in _list_baselines(baseline_dir) if p != right_path]
        if not others:
            raise FileNotFoundError(
                f"Need a second baseline under {baseline_dir} to diff against {args.baseline_id}"
            )
        left_path = others[-1]
        return _load(left_path), _load(right_path), str(left_path), str(right_path)
    baselines = _list_baselines(baseline_dir)
    if len(baselines) < 2:
        raise FileNotFoundError(
            f"Need at least 2 baselines under {baseline_dir}; found {len(baselines)}"
        )
    return (
        _load(baselines[-2]),
        _load(baselines[-1]),
        str(baselines[-2]),
        str(baselines[-1]),
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Diff two data-coverage baseline snapshots."
    )
    subparsers = parser.add_subparsers(dest="cmd")
    cmp_parser = subparsers.add_parser("compare", help="Diff two baselines (default).")
    for p in (parser, cmp_parser):
        _ = p.add_argument(
            "--baseline-dir",
            default=str(BASELINE_ROOT),
            help="Directory holding baseline_*.json files (default: artifacts/statistical/baseline).",
        )
        mode = p.add_mutually_exclusive_group()
        _ = mode.add_argument(
            "--latest",
            action="store_true",
            help="Diff the two most-recent baselines (default).",
        )
        _ = mode.add_argument(
            "--baseline-id", help="Pin one side of the diff to a specific baseline_id."
        )
        _ = mode.add_argument(
            "--against-current",
            action="store_true",
            help="Diff the most-recent baseline against the live DB (no JSON write).",
        )
        _ = p.add_argument(
            "--allow-delta",
            action="store_true",
            help="Exit 0 even if deltas are present.",
        )
        _ = p.add_argument(
            "--log-level", default="INFO", help="stdlib logging level name."
        )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))

    left, right, left_src, right_src = _resolve_pair(args)
    scalar_diffs, grouped_diffs = _compare(left, right)
    markdown = _render_markdown(
        left_meta=_summary_label(left, left_src),
        right_meta=_summary_label(right, right_src),
        scalar_diffs=scalar_diffs,
        grouped_diffs=grouped_diffs,
    )
    sys.stdout.write(markdown)
    if (scalar_diffs or grouped_diffs) and not args.allow_delta:
        _log.error(
            "baseline_delta_detected scalar=%d grouped=%d",
            len(scalar_diffs),
            len(grouped_diffs),
        )
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
