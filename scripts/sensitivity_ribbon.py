"""MNAR sensitivity ribbon for the published geometry imputations.

Three modes:

``--publish`` (default): for each published ``geometry_*`` pointer, reweight its
per-event class shares over the selection-log-odds grid and write
``exports/sensitivity_ribbon.parquet`` + a ``[low, high]`` band beside the fit.
The ribbon is the honest band on each imputed class mix under plausible MNAR;
``delta = 0`` reproduces the published (MAR) marginal.

``--joint <anchor-run-dir>``: for the trajectory dimension only, sweep the
anchored offset direction ``t · delta_anchor`` (era-specific magnitude) plus
per-non-focal-class ±perturbations at ``t = 1``, grouped per era bucket, and
write ``exports/sensitivity_ribbon_joint.parquet`` + a summary JSON beside the
trajectory fit. The anchor is a SINGLE-CLASS GroundBall direction (the derived
slice trajectory deduction recovers only ground balls), so all other class
offsets are 0. The other four geometry dimensions have no anchor and stay
marginal-only (``sensitivity_ribbon.parquet``), same as ``--publish``.

CLASS-UNIVERSE DECISION (stated in the summary JSON and logged): the ribbon
uses the anchored GroundBall offset recomputed over the paper's classifiable
class universe — the observed-slice denominator excludes the unclassifiable
bunt labels UnspecifiedBunt / FoulBunt (as ``notes/paper/queries/
groundball_mnar.sql`` does), matching the geometry model's own trajectory
vocabulary {Bunt, Fly, GroundBall, LineDrive, PopUp}, which carries no such
labels. Those two labels are 0.01–0.14% of observed rows, so realigning moves
the offset by <0.001 nats. The larger anchor(0.289)-vs-paper(0.337) pre-1950
gap is a DIFFERENT decomposition, not a class-universe artifact: the paper's
broad "ground" share folds GroundBallBunt into the numerator, whereas the
anchor's single-class offset targets the pure GroundBall model class.

``--validate-backtest <run-dir>``: on the masked backtest, the real correction
is known (the per-class mask probabilities recover the true maskable-slice
mix). Confirms the default grid spans that correction — the oracle per-class
offset lands inside the grid and the focal-class band brackets the truth — so
the published ribbon is not too narrow to contain a realistic MNAR shift.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import sys
from pathlib import Path

import polars as pl

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import mnar_anchor  # noqa: E402
from python_models.statistical.duckdb_io import open_bc_db  # noqa: E402
from python_models.statistical.manifests import find_published_manifest  # noqa: E402
from python_models.statistical.sensitivity import (  # noqa: E402
    DEFAULT_GRID,
    JOINT_PERTURBATION_NATS,
    JOINT_T_GRID,
    SWEEP_JOINT_ANCHOR,
    joint_ribbon_band,
    joint_sensitivity_ribbon,
    offset_recovers_target,
    ribbon_band,
    sensitivity_ribbon,
)

log = logging.getLogger("sensitivity_ribbon")

GEOMETRY_MODELS: tuple[str, ...] = (
    "geometry_trajectory",
    "geometry_location_side",
    "geometry_location_depth",
    "geometry_location_edge",
    "geometry_general_location",
)
_LOGIT_CLIP = 1e-6

JOINT_DIMENSION = "trajectory"
JOINT_MODEL = "geometry_trajectory"
FOCAL_CLASS = "GroundBall"
EXCLUDED_CLASSES: tuple[str, ...] = ("UnspecifiedBunt", "FoulBunt")

CLASS_UNIVERSE_DECISION = (
    "The ribbon uses the anchored GroundBall selection offset recomputed over "
    "the paper's classifiable class universe: the observed-slice denominator "
    "excludes the unclassifiable bunt labels UnspecifiedBunt and FoulBunt (as "
    "notes/paper/queries/groundball_mnar.sql does), matching the geometry "
    "model's trajectory vocabulary {Bunt, Fly, GroundBall, LineDrive, PopUp}, "
    "which carries no such labels. Those two labels are 0.01-0.14% of observed "
    "rows, so realigning moves the offset by <0.001 nats. The larger anchor "
    "(pre-1950 p_obs 0.289) vs paper (ground share 0.337) gap is a DIFFERENT "
    "decomposition, not a class-universe artifact: the paper's broad 'ground' "
    "share folds GroundBallBunt into the numerator, whereas the anchor's "
    "single-class offset targets the pure GroundBall model class. The derived "
    "slice is 100% GroundBall, so the anchor is a single-class softmax "
    "direction with all other class offsets 0; softmax offsets are identified "
    "up to an additive constant, so the raw single-class offset (delta_raw, not "
    "the degenerate centered delta) is a valid joint direction."
)

_SEASON_QUERY = (
    "SELECT event_key, season FROM main_models.model_input_geometry "
    "WHERE geometry_dimension = '{dimension}'"
)


def _published_export(model_name: str) -> tuple[str, Path] | None:
    pointer = find_published_manifest(model_name)
    if pointer is None:
        log.warning("no published pointer for %s", model_name)
        return None
    manifest = json.loads(pointer.read_text())
    export = (
        Path(manifest["manifest_path"]).parent / "exports" / "geometry_probabilities.parquet"
    )
    if not export.exists():
        log.warning("%s pointer has no geometry export at %s", model_name, export)
        return None
    return manifest["artifact_id"], export


def _dimension_of(export_df: pl.DataFrame) -> str:
    return str(export_df.select("geometry_dimension").unique().item())


def _publish_marginal(model_name: str) -> bool:
    resolved = _published_export(model_name)
    if resolved is None:
        return False
    _artifact_id, export_path = resolved
    export_df = pl.read_parquet(export_path)
    dimension = _dimension_of(export_df)
    ribbon = sensitivity_ribbon(export_df, dimension=dimension)
    out_path = export_path.parent / "sensitivity_ribbon.parquet"
    ribbon.write_parquet(out_path)
    band = ribbon_band(ribbon)
    log.info("%s -> %s", model_name, out_path)
    for row in band.iter_rows(named=True):
        log.info(
            "  %-12s baseline=%.4f band=[%.4f, %.4f]",
            row["class_label"],
            row["baseline_share"],
            row["share_low"],
            row["share_high"],
        )
    return True


def publish_ribbons() -> int:
    written = sum(_publish_marginal(model_name) for model_name in GEOMETRY_MODELS)
    if written == 0:
        log.error("no published geometry exports resolved; nothing written")
        return 1
    return 0


def _aligned_anchor_offsets(
    run_dir: Path,
) -> tuple[dict[str, dict[str, float]], list[dict[str, object]]]:
    """Per-era single-class GroundBall offset, realigned to the classifiable universe.

    Reads the anchor artifact's paper-bucketing offsets and recomputes the
    GroundBall selection offset with the observed / masked shares taken over the
    classifiable class universe (UnspecifiedBunt / FoulBunt excluded). Returns
    the per-era anchor direction plus per-era diagnostics (raw vs aligned).
    """
    offsets = pl.read_parquet(run_dir / "anchor_offsets.parquet").filter(
        pl.col("bucketing") == "paper"
    )
    anchor_by_era: dict[str, dict[str, float]] = {}
    diagnostics: list[dict[str, object]] = []
    for era in offsets.get_column("era_bucket").unique().sort().to_list():
        era_rows = offsets.filter(pl.col("era_bucket") == era)
        classifiable = era_rows.filter(~pl.col("class_label").is_in(EXCLUDED_CLASSES))
        focal = classifiable.filter(pl.col("class_label") == FOCAL_CLASS)
        n_obs_focal = int(focal.get_column("n_obs").item())
        n_masked_focal = int(focal.get_column("n_masked").item())
        total_obs = int(classifiable.get_column("n_obs").sum())
        total_masked = int(classifiable.get_column("n_masked").sum())
        p_obs_aligned = n_obs_focal / total_obs
        p_masked_aligned = n_masked_focal / total_masked
        delta_aligned = math.log(p_masked_aligned / p_obs_aligned)
        anchor_by_era[str(era)] = {FOCAL_CLASS: delta_aligned}
        diagnostics.append(
            {
                "era_bucket": str(era),
                "p_obs_full_universe": float(focal.get_column("p_obs").item()),
                "p_obs_classifiable": p_obs_aligned,
                "delta_raw_full_universe": float(focal.get_column("delta_raw").item()),
                "delta_aligned": delta_aligned,
            }
        )
    return anchor_by_era, diagnostics


def _season_for_events(db_path: Path, dimension: str) -> pl.DataFrame:
    with open_bc_db(db_path, read_only=True) as con:
        _ = con.execute("PRAGMA disable_progress_bar")
        return con.execute(_SEASON_QUERY.format(dimension=dimension)).pl()


def _attach_era(export_df: pl.DataFrame, season_df: pl.DataFrame) -> pl.DataFrame:
    joined = export_df.join(season_df, on="event_key", how="left")
    missing = joined.filter(pl.col("season").is_null()).get_column("event_key").n_unique()
    if missing:
        log.warning(
            "%d events have no season in model_input_geometry; dropping them", missing
        )
        joined = joined.filter(pl.col("season").is_not_null())
    return joined.with_columns(
        pl.when(pl.col("season") < 1950)
        .then(pl.lit(mnar_anchor.PAPER_BUCKET_PRE_1950))
        .when(pl.col("season") <= 1987)
        .then(pl.lit(mnar_anchor.PAPER_BUCKET_1950_1987))
        .otherwise(pl.lit(mnar_anchor.PAPER_BUCKET_1988_PLUS))
        .alias("era_bucket")
    )


def _log_joint_table(ribbon: pl.DataFrame) -> None:
    band = joint_ribbon_band(ribbon)
    for era in ribbon.get_column("era_bucket").unique().sort().to_list():
        focal_t = (
            ribbon.filter(
                (pl.col("era_bucket") == era)
                & (pl.col("sweep_kind") == SWEEP_JOINT_ANCHOR)
                & (pl.col("class_label") == FOCAL_CLASS)
            )
            .sort("t")
            .select("t", "marginal_share", "baseline_share")
        )
        baseline = float(focal_t.get_column("baseline_share").item(0))
        log.info(
            "[%s] %s baseline(MAR t=0)=%.4f", era, FOCAL_CLASS, baseline
        )
        for row in focal_t.iter_rows(named=True):
            log.info("    t=%.2f  %s share=%.4f", row["t"], FOCAL_CLASS, row["marginal_share"])
        focal_band = band.filter(
            (pl.col("era_bucket") == era) & (pl.col("class_label") == FOCAL_CLASS)
        )
        if focal_band.height:
            b = focal_band.row(0, named=True)
            log.info(
                "    t=1 anchor share=%.4f  perturbation band=[%.4f, %.4f]",
                b["anchor_share"],
                b["share_low"],
                b["share_high"],
            )


def publish_joint_ribbons(anchor_dir: Path, db_path: Path) -> int:
    anchor_by_era, diagnostics = _aligned_anchor_offsets(anchor_dir)
    log.info("CLASS-UNIVERSE DECISION: %s", CLASS_UNIVERSE_DECISION)
    for diag in diagnostics:
        log.info(
            "[%s] p_obs full=%.4f classifiable=%.4f | delta raw=%.4f aligned=%.4f",
            diag["era_bucket"],
            diag["p_obs_full_universe"],
            diag["p_obs_classifiable"],
            diag["delta_raw_full_universe"],
            diag["delta_aligned"],
        )

    resolved = _published_export(JOINT_MODEL)
    if resolved is None:
        log.error("no published %s export; cannot build joint ribbon", JOINT_MODEL)
        return 1
    artifact_id, export_path = resolved
    export_df = pl.read_parquet(export_path)
    dimension = _dimension_of(export_df)
    season_df = _season_for_events(db_path, dimension)
    log.info(
        "attaching era via event_key join against main_models.model_input_geometry "
        "(%s dimension, read-only %s)",
        dimension,
        db_path,
    )
    export_df = _attach_era(export_df, season_df)

    ribbon = joint_sensitivity_ribbon(
        export_df,
        dimension=dimension,
        anchor_offset_by_era=anchor_by_era,
        focal_class=FOCAL_CLASS,
    )
    out_path = export_path.parent / "sensitivity_ribbon_joint.parquet"
    ribbon.write_parquet(out_path)
    log.info("%s -> %s (%d rows)", JOINT_MODEL, out_path, ribbon.height)
    _log_joint_table(ribbon)

    summary = {
        "focal_class": FOCAL_CLASS,
        "class_universe_decision": CLASS_UNIVERSE_DECISION,
        "excluded_classes": list(EXCLUDED_CLASSES),
        "anchor_run_dir": str(anchor_dir),
        "trajectory_artifact_id": artifact_id,
        "t_grid": list(JOINT_T_GRID),
        "perturbation_nats": JOINT_PERTURBATION_NATS,
        "era_offsets": diagnostics,
    }
    summary_path = export_path.parent / "sensitivity_ribbon_joint_summary.json"
    _ = summary_path.write_text(json.dumps(summary, indent=2, sort_keys=True))
    log.info("wrote %s", summary_path)

    for model_name in GEOMETRY_MODELS:
        if model_name == JOINT_MODEL:
            continue
        log.info("%s has no anchor; emitting marginal-only ribbon", model_name)
        _ = _publish_marginal(model_name)
    return 0


def _logit(p: float) -> float:
    p = min(max(p, _LOGIT_CLIP), 1.0 - _LOGIT_CLIP)
    return math.log(p / (1.0 - p))


def validate_backtest(run_dir: Path) -> int:
    mask_summary = json.loads((run_dir / "mask_summary.json").read_text())
    truth = mask_summary["truth_shares_maskable"]
    mask_prob = mask_summary["class_mask_probabilities"]
    focal = mask_summary["focal_class"]

    uncorrected = next(
        run_dir.glob("bayes/*/*-uncorrected/exports/geometry_probabilities.parquet"),
        None,
    )
    if uncorrected is None:
        log.error("no uncorrected export under %s", run_dir)
        return 1
    export_df = pl.read_parquet(uncorrected)

    raw_offset = {c: _logit(p) for c, p in mask_prob.items()}
    centre = sum(raw_offset.values()) / len(raw_offset)
    oracle = {c: v - centre for c, v in raw_offset.items()}

    grid_lo, grid_hi = min(DEFAULT_GRID), max(DEFAULT_GRID)
    in_grid = {c: grid_lo <= d <= grid_hi for c, d in oracle.items()}

    baseline_tv = offset_recovers_target(export_df, offset={}, target=truth)
    corrected_tv = offset_recovers_target(export_df, offset=oracle, target=truth)

    ribbon = sensitivity_ribbon(export_df, dimension=_dimension_of(export_df))
    band = ribbon_band(ribbon)
    focal_band = next(r for r in band.iter_rows(named=True) if r["class_label"] == focal)
    focal_truth = truth[focal]
    brackets = focal_band["share_low"] <= focal_truth <= focal_band["share_high"]

    log.info("focal class: %s (truth share %.4f)", focal, focal_truth)
    log.info("oracle per-class offset (centered, nats):")
    for c in sorted(oracle):
        log.info("  %-12s %+.3f  in-grid=%s", c, oracle[c], in_grid[c])
    log.info("TV vs truth: baseline(MAR)=%.4f  oracle-corrected=%.4f", baseline_tv, corrected_tv)
    log.info(
        "focal band [%.4f, %.4f] brackets truth %.4f: %s",
        focal_band["share_low"],
        focal_band["share_high"],
        focal_truth,
        brackets,
    )

    oracle_in_grid = all(in_grid.values())
    corrected_better = corrected_tv < baseline_tv
    ok = oracle_in_grid and corrected_better and brackets
    log.info(
        "VALIDATION %s (oracle-in-grid=%s, corrected<baseline=%s, brackets=%s)",
        "PASS" if ok else "FAIL",
        oracle_in_grid,
        corrected_better,
        brackets,
    )
    return 0 if ok else 1


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--validate-backtest",
        type=Path,
        default=None,
        metavar="RUN_DIR",
        help="validate grid coverage against a masked-backtest run dir instead of publishing",
    )
    _ = parser.add_argument(
        "--joint",
        type=Path,
        default=None,
        metavar="ANCHOR_RUN_DIR",
        help="build the joint anchored ribbon for trajectory from an anchor run dir",
    )
    _ = parser.add_argument(
        "--db",
        type=Path,
        default=REPO_ROOT / "bc.db",
        help="prod DuckDB for the era join (default: bc.db at repo root); READ-ONLY",
    )
    return parser.parse_args()


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    args = _parse_args()
    if args.validate_backtest is not None:
        return validate_backtest(args.validate_backtest)
    if args.joint is not None:
        return publish_joint_ribbons(args.joint, args.db)
    return publish_ribbons()


if __name__ == "__main__":
    raise SystemExit(main())
