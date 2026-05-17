"""Summarize Phase-3 v6 ablation: fit metrics, sidecar, perm-imp ratios.

Reads `logs/ablation_v6/*.log` and `artifacts/statistical/deep/event_universe/phase3-pretrain-v6-ab*/eval/eval_report.json`.
Emits a Markdown table to stdout.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
LOGS = REPO / "logs" / "ablation_v6"
DEEP = REPO / "artifacts" / "statistical" / "deep" / "event_universe"

VARIANTS: list[tuple[str, str]] = [
    ("phase3-pretrain-v6-ab1", "single-stage, val_loss ES"),
    ("phase3-pretrain-v6-ab2", "stage1=3 + stage2=3, val_loss ES"),
    ("phase3-pretrain-v6-ab3", "single-stage, hard-head ES"),
    ("phase3-pretrain-v6-ab4", "stage1=3 + stage2=3, hard-head ES"),
]

ENTITIES = ("batter_id", "pitcher_id", "park_id", "scorer")
PERM_LINE = re.compile(
    r"perm\s+(\S+)\s+(?:CE|KL|H)=(-?\d+\.\d+)\s+Δ=([+-]\d+\.\d+)"
)
BASELINE_CE = re.compile(r"baseline (?:CE|KL|H)=(-?\d+\.\d+)")


def parse_permimp(path: Path) -> tuple[float, dict[str, float]] | None:
    if not path.exists():
        return None
    txt = path.read_text(encoding="utf-8", errors="ignore")
    b = BASELINE_CE.search(txt)
    if not b:
        return None
    baseline = float(b.group(1))
    deltas: dict[str, float] = {}
    for match in PERM_LINE.finditer(txt):
        feat, _, delta = match.groups()
        if feat in ENTITIES:
            deltas[feat] = float(delta)
    return baseline, deltas


def parse_sidecar(artifact_id: str) -> dict[str, Any] | None:
    p = DEEP / artifact_id / "eval" / "eval_report.json"
    if not p.exists():
        return None
    payload: dict[str, Any] = json.loads(p.read_text(encoding="utf-8"))
    return payload


def parse_slash_lines(fit_log: Path) -> list[str]:
    """Pick final-epoch slash probe lines for the canonical roster."""
    if not fit_log.exists():
        return []
    txt = fit_log.read_text(encoding="utf-8", errors="ignore")
    lines = [
        line for line in txt.splitlines()
        if (
            "slash_probe" in line.lower()
            or "mc_probe" in line.lower()
            or " AVG=" in line
        )
    ]
    return lines[-12:] if lines else []


def fmt_ratio(v6: float | None, baseline: float | None) -> str:
    if v6 is None or baseline is None or baseline == 0:
        return "n/a"
    return f"×{v6 / baseline:.2f}"


def main() -> int:
    no_pretrain_traj = parse_permimp(LOGS / "no_pretrain_geometry_trajectory_permimp.log")
    no_pretrain_fc = parse_permimp(LOGS / "no_pretrain_fielding_credit_putout_permimp.log")

    print("# Phase-3 v6 mini ablation summary")
    print()
    print("## No-pretrain baselines (stripped layouts, time_forward fold)")
    print()
    for name, parsed in (
        ("geometry_trajectory", no_pretrain_traj),
        ("fielding_credit_putout", no_pretrain_fc),
    ):
        if parsed is None:
            print(f"- {name}: not run")
            continue
        base, deltas = parsed
        delta_str = " ".join(f"{e}={deltas.get(e, 0):+.4f}" for e in ENTITIES)
        print(f"- {name}: baseline CE = {base:.4f} | Δ_CE {delta_str}")
    print()

    print("## Variants")
    print()
    print("| Variant | stage1 | ES | sidecar proxy | traj batter ratio | traj scorer ratio | fc batter ratio | fc scorer ratio |")
    print("| --- | --- | --- | --- | --- | --- | --- | --- |")
    for artifact, desc in VARIANTS:
        sidecar = parse_sidecar(artifact)
        proxy = "n/a"
        if sidecar:
            ds_raw = sidecar.get("downstream_proxy")
            if isinstance(ds_raw, dict):
                score = ds_raw.get("downstream_proxy_score")
                if isinstance(score, (int, float)):
                    proxy = f"{float(score):.4f}"
        traj = parse_permimp(LOGS / f"{artifact}_geometry_trajectory_permimp.log")
        fc = parse_permimp(LOGS / f"{artifact}_fielding_credit_putout_permimp.log")

        def ratio(parsed: tuple[float, dict[str, float]] | None,
                  baseline_parsed: tuple[float, dict[str, float]] | None,
                  entity: str) -> str:
            if parsed is None or baseline_parsed is None:
                return "n/a"
            return fmt_ratio(parsed[1].get(entity), baseline_parsed[1].get(entity))

        print(
            f"| {artifact} | {desc} | {proxy} | "
            f"{ratio(traj, no_pretrain_traj, 'batter_id')} | "
            f"{ratio(traj, no_pretrain_traj, 'scorer')} | "
            f"{ratio(fc, no_pretrain_fc, 'batter_id')} | "
            f"{ratio(fc, no_pretrain_fc, 'scorer')} |"
        )

    print()
    print("## Final-epoch slash probe (per variant)")
    print()
    for artifact, _desc in VARIANTS:
        fit_log = LOGS / f"{artifact}_fit.log"
        rows = parse_slash_lines(fit_log)
        print(f"### {artifact}")
        print()
        if not rows:
            print("(no slash-probe rows found)")
        else:
            print("```")
            for r in rows:
                print(r)
            print("```")
        print()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
