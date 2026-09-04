"""MNAR masked-backtest harness for the gamma_propensity covariate.

Takes a frozen modeling dataset's observed rows in modern seasons (where
labels approximate the population) as a truth universe, applies a seeded
synthetic MNAR mask (class-weighted, scorer-bucket-intensity scaled,
never touching the game-hash holdout fold), fits a synthetic observation
propensity model on the masked universe, then fits the imputation target
twice on the same masked dataset — ``gamma_propensity_class`` vs
``gamma_propensity_zero`` — and scores both against the held-back truth.
The class flavor passes when it recovers the masked-slice class shares
better than the zero flavor without regressing on the never-masked fold,
with clean sampler diagnostics on both fits.

Three things to know before reading a result from this harness:

1. The oracle-offset arm is an algebraic identity for the ``w_class_intensity``,
   ``scorer_blocked`` and ``era_graded`` designs. When selection depends on
   class alone (or on nothing), ``P(c | masked) is proportional to
   P(c | observed) * odds_mask(c)`` holds at the marginal level for any
   per-event shares, including a constant model, so reweighting by the
   realized per-class masked rate recovers the masked-slice marginal by
   construction and cannot fail. Only ``covariate_joint``, whose selection
   depends on class and a covariate the model conditions on, tests the
   offset against something it does not already encode.
2. The results recorded in ``notes/paper/tables/mnar_backtest_robustness.md``
   are ``SMOKE_BUDGET=1000`` runs (50 draws x 50 tune x 2 chains) whose
   convergence gates fail (``ess_bulk_min`` 20-47 against the 100 floor;
   ``overall_pass`` False for every design). No run artifacts are checked in.
3. The sign of ``gamma_propensity_class``: ``z`` is the standardized logit of
   ``propensity_p_observed`` (``_geometry_data.py``), so masked events have
   LOW ``z``. A negative GroundBall coefficient therefore raises GroundBall on
   the masked slice, which is the direction a correction needs. The learned
   arm's -0.001 relative reduction means it is inert on the masked slice, not
   wrong-signed.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportUnknownParameterType=false, reportAny=false, reportExplicitAny=false

from __future__ import annotations

import json
import logging
import math
from pathlib import Path
from typing import Any, ClassVar, Literal

import numpy as np
import numpy.typing as npt
import polars as pl
from pydantic import BaseModel, ConfigDict

from python_models.statistical import config as cfg
from python_models.statistical.manifests import (
    find_published_manifest,
    read_manifest,
    read_published_pointer,
    utc_now,
    write_manifest,
)
from python_models.statistical.models._ball_handler_data import POSITION_LABELS
from python_models.statistical.models._geometry_data import (
    GEOMETRY_DIMENSIONS,
    HOLDOUT_FOLD_COUNT,
    HOLDOUT_FOLD_ID,
    PROPENSITY_COLUMN,
)
from python_models.statistical.schemas import ArtifactManifest
from python_models.statistical.splits import game_hash_fold

_log = logging.getLogger(__name__)

BoolArray = npt.NDArray[np.bool_]
FloatArray = npt.NDArray[np.float64]

BacktestModelName = Literal["geometry", "ball_handler"]

DEFAULT_SEED: int = 20260610
DEFAULT_BUDGET: int = 10_000
SMOKE_BUDGET: int = 1_000
DEFAULT_MIN_SEASON: int = 1988
DEFAULT_OUTPUT_ROOT: Path = cfg.ARTIFACT_ROOT / "backtests" / "mnar"

OBSERVED_STATUS: str = "observed"
TRUE_LABEL_COLUMN: str = "_true_label"
PROPENSITY_ARTIFACT_COLUMN: str = "propensity_artifact_id"

OVERALL_MASKED_RANGE: tuple[float, float] = (0.60, 0.80)

THRESHOLDS: dict[str, float] = {
    "focal_relative_reduction_min": 0.25,
    "top1_tolerance": 0.005,
    "log_loss_tolerance": 0.01,
    "rhat_max": 1.05,
    "ess_bulk_min": 100.0,
    "max_divergences": 0.0,
}


class BacktestVariant(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    name: str
    dimension: str
    dimension_column: str
    propensity_target: str
    imputation_target: str
    obs_dataset_name: str
    imputation_dataset_name: str
    pointer_name: str
    class_labels: tuple[str, ...]
    label_remap: dict[str, str]
    focal_class: str
    export_filename: str
    export_kind: Literal["geometry", "ball_handler"]
    masked_status: str
    observed_focal_range: tuple[float, float] | None


def _geometry_variant() -> BacktestVariant:
    spec = GEOMETRY_DIMENSIONS["trajectory"]
    assert spec.class_labels is not None
    return BacktestVariant(
        name="geometry",
        dimension="trajectory",
        dimension_column="geometry_dimension",
        propensity_target="trajectory_observedness",
        imputation_target="geometry_trajectory",
        obs_dataset_name="model_input_observation_batted_ball",
        imputation_dataset_name="model_input_geometry",
        pointer_name="geometry_trajectory",
        class_labels=spec.class_labels,
        label_remap=dict(spec.remap),
        focal_class="GroundBall",
        export_filename="geometry_probabilities.parquet",
        export_kind="geometry",
        masked_status="unknown_code",
        observed_focal_range=(0.34, 0.44),
    )


def _ball_handler_variant() -> BacktestVariant:
    return BacktestVariant(
        name="ball_handler",
        dimension="ball_handler_position",
        dimension_column="dimension",
        propensity_target="ball_handler_position_observedness",
        imputation_target="ball_handler_imputation",
        obs_dataset_name="model_input_observation_batted_ball",
        imputation_dataset_name="model_input_observation_batted_ball",
        pointer_name="ball_handler_imputation",
        class_labels=POSITION_LABELS,
        label_remap={},
        focal_class="6",
        export_filename="ball_handler_probabilities.parquet",
        export_kind="ball_handler",
        masked_status="unknown_code",
        observed_focal_range=None,
    )


VARIANTS: dict[str, BacktestVariant] = {
    "geometry": _geometry_variant(),
    "ball_handler": _ball_handler_variant(),
}


class MaskConfig(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(frozen=True)

    overall_masked_target: float = 0.70
    focal_depletion_ratio: float = 0.82
    intensity_tiers: tuple[float, ...] = (0.8, 1.0, 1.2)
    max_mask_probability: float = 0.98
    holdout_fold_count: int = HOLDOUT_FOLD_COUNT
    holdout_fold_id: int = HOLDOUT_FOLD_ID
    intensity_salt: str = "mnar-intensity"
    design: Literal[
        "w_class_intensity", "covariate_joint", "scorer_blocked", "era_graded"
    ] = "w_class_intensity"
    covariate_columns: tuple[str, ...] = ("batter_hand", "park_id", "frame_start")
    covariate_levels: tuple[float, float] = (0.7, 1.3)
    block_probability: float = 0.95
    block_salt: str = "mnar-block"
    block_bucket_count: int = 10_000


class MaskResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    masked: BoolArray
    holdout: BoolArray
    summary: dict[str, Any]


class BacktestResult(BaseModel):
    model_config: ClassVar[ConfigDict] = ConfigDict(
        arbitrary_types_allowed=True, frozen=True
    )

    run_dir: Path
    metrics_path: Path
    mask_summary_path: Path
    metrics: dict[str, Any]
    overall_pass: bool


def _json_default(value: object) -> object:
    if isinstance(value, (np.floating, np.integer)):
        return float(value)
    if isinstance(value, np.bool_):
        return bool(value)
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"unsupported JSON type {type(value)!r}")


def _write_json(payload: dict[str, Any], target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(
        json.dumps(payload, indent=2, default=_json_default), encoding="utf-8"
    )


def _read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def resolve_default_dataset_parquet(variant: BacktestVariant) -> Path:
    """Resolve the frozen dataset behind the variant's published fit pointer."""
    pointer_path = find_published_manifest(variant.pointer_name)
    if pointer_path is None:
        raise FileNotFoundError(
            f"no published pointer for {variant.pointer_name!r} under "
            f"BC_STATS_PUBLISHED_ROOT or {cfg.GLOBAL_PUBLISHED_ROOT}; pass "
            f"--dataset-parquet explicitly or set BC_STATS_PUBLISHED_ROOT to the "
            f"branch root that carries the pointer"
        )
    pointer = read_published_pointer(pointer_path)
    fit_manifest = read_manifest(pointer.manifest_path)
    if fit_manifest.dataset_artifact_id is None:
        raise ValueError(
            f"published {variant.pointer_name!r} manifest at "
            f"{pointer.manifest_path} carries no dataset_artifact_id; pass "
            f"--dataset-parquet explicitly"
        )
    dataset_parquet = (
        cfg.DATASETS_ROOT
        / variant.imputation_dataset_name
        / fit_manifest.dataset_artifact_id
        / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        raise FileNotFoundError(
            f"dataset parquet missing at {dataset_parquet} (resolved from the "
            f"published {variant.pointer_name!r} pointer); pass --dataset-parquet "
            f"explicitly"
        )
    _log.info(
        "resolved default dataset parquet via %s pointer: %s",
        variant.pointer_name,
        dataset_parquet,
    )
    return dataset_parquet


def load_truth_universe(
    parquet_path: Path,
    *,
    variant: BacktestVariant,
    min_season: int = DEFAULT_MIN_SEASON,
) -> pl.DataFrame:
    """Observed, training-eligible, modern-season rows with a vocab truth label.

    Filters the dataset to ``dimension=<variant>``, ``observed_status =
    'observed'``, ``training_weight > 0`` and ``season >= min_season``,
    deduplicates to one row per event, sorts by ``event_key`` for
    deterministic masking, drops any real published propensity columns
    (the synthetic-mask fit must be the only propensity source), and adds
    the remapped truth label column.
    """
    df = (
        pl.scan_parquet(parquet_path)
        .filter(
            (pl.col(variant.dimension_column) == variant.dimension)
            & (pl.col("observed_status") == OBSERVED_STATUS)
            & (pl.col("training_weight") > 0.0)
            & (pl.col("season").cast(pl.Int64, strict=False) >= min_season)
        )
        .collect()
        .unique(subset=["event_key"], keep="first", maintain_order=True)
        .sort("event_key")
    )
    if df.height == 0:
        raise ValueError(
            f"no observed truth-universe rows for dimension={variant.dimension!r} "
            f"with season >= {min_season} in {parquet_path}"
        )
    stale_columns = [
        c for c in (PROPENSITY_COLUMN, PROPENSITY_ARTIFACT_COLUMN) if c in df.columns
    ]
    if stale_columns:
        _log.info("dropping real propensity columns from universe: %s", stale_columns)
        df = df.drop(stale_columns)

    label = pl.col("raw_value").cast(pl.Utf8)
    if variant.label_remap:
        label = label.replace(variant.label_remap)
    df = df.with_columns(label.alias(TRUE_LABEL_COLUMN))
    in_vocab = df.get_column(TRUE_LABEL_COLUMN).is_in(list(variant.class_labels))
    dropped = int((~in_vocab).fill_null(True).sum())
    if dropped:
        _log.warning(
            "dropped %d universe rows whose remapped label is outside the %s vocab",
            dropped,
            variant.dimension,
        )
        df = df.filter(in_vocab)
    if df.height == 0:
        raise ValueError(
            f"every truth-universe row fell outside the {variant.dimension!r} vocab"
        )
    _log.info(
        "truth universe loaded: rows=%d games=%d seasons=%d dimension=%s",
        df.height,
        df.get_column("game_id").n_unique(),
        df.get_column("season").n_unique(),
        variant.dimension,
    )
    return df


def calibrate_class_mask_probabilities(
    truth_labels: npt.NDArray[np.str_],
    *,
    focal_class: str,
    class_labels: tuple[str, ...],
    config: MaskConfig,
) -> dict[str, float]:
    """Solve per-class mask probabilities from the universe class mix.

    With overall masked-share target ``m`` and focal depletion ratio
    ``rho`` (post-mask observed focal share = ``rho * truth focal
    share``), the focal mask probability is ``u_F = 1 - rho * (1 - m)``
    and every other class shares ``u_N = (m - f_F * u_F) / (1 - f_F)``
    where ``f_F`` is the focal truth share. Exact in expectation by
    construction, and ``u_F > u_N`` whenever ``rho < 1``.
    """
    n = int(truth_labels.shape[0])
    if n == 0:
        raise ValueError("cannot calibrate mask probabilities on an empty universe")
    f_focal = float(np.mean(truth_labels == focal_class))
    if f_focal <= 0.0:
        raise ValueError(
            f"focal class {focal_class!r} has zero share in the maskable universe"
        )
    if f_focal >= 1.0:
        raise ValueError(
            f"focal class {focal_class!r} is the entire maskable universe; "
            "non-focal mask probability is undefined"
        )
    m = config.overall_masked_target
    u_focal = 1.0 - config.focal_depletion_ratio * (1.0 - m)
    if not 0.0 < u_focal < 1.0:
        raise ValueError(
            f"infeasible focal mask probability {u_focal:.4f} from "
            f"overall_masked_target={m} focal_depletion_ratio="
            f"{config.focal_depletion_ratio}"
        )
    u_other = (m - f_focal * u_focal) / (1.0 - f_focal)
    if u_other < 0.0:
        raise ValueError(
            f"infeasible non-focal mask probability {u_other:.4f}: focal share "
            f"{f_focal:.4f} at u_focal={u_focal:.4f} already exceeds the overall "
            f"target {m}"
        )
    probabilities = {
        label: (u_focal if label == focal_class else u_other) for label in class_labels
    }
    _log.debug(
        "calibrated mask probabilities: focal=%s f_focal=%.4f u_focal=%.4f "
        "u_other=%.4f",
        focal_class,
        f_focal,
        u_focal,
        u_other,
    )
    return probabilities


def _share_by_class(
    labels: npt.NDArray[np.str_], class_labels: tuple[str, ...]
) -> dict[str, float]:
    n = int(labels.shape[0])
    if n == 0:
        return {label: 0.0 for label in class_labels}
    return {label: float(np.mean(labels == label)) for label in class_labels}


def _hash_bucket_intensity(
    game_ids: list[str], *, tiers: FloatArray, salt: str
) -> tuple[FloatArray, npt.NDArray[np.int64]]:
    """Salted per-game hash bucket into the intensity tiers, plus the bucket index."""
    n = len(game_ids)
    tier_idx = np.fromiter(
        (game_hash_fold(f"{salt}|{g}", fold_count=tiers.shape[0]) for g in game_ids),
        dtype=np.int64,
        count=n,
    )
    return tiers[tier_idx], tier_idx


def _era_graded_intensity(
    seasons: npt.NDArray[np.int64], *, tiers: FloatArray
) -> tuple[FloatArray, npt.NDArray[np.int64]]:
    """Per-row intensity from the season's era tier, oldest seasons at the highest tier."""
    n_tiers = tiers.shape[0]
    quantiles = np.linspace(0.0, 1.0, n_tiers + 1)[1:-1]
    edges = np.quantile(seasons.astype(np.float64), quantiles)
    tier_idx = np.clip(
        np.searchsorted(edges, seasons, side="right"), 0, n_tiers - 1
    ).astype(np.int64)
    descending = np.sort(tiers)[::-1]
    return descending[tier_idx], tier_idx


def _covariate_bucket(values: npt.NDArray[np.str_]) -> npt.NDArray[np.int64]:
    """Alternating-rank split of a covariate's distinct values into two buckets.

    A salted hash into 2 buckets is a coin flip per distinct value, so with
    only a handful of distinct values (``batter_hand`` is typically only
    ``{"L", "R"}`` plus nulls) it can land every real value in the same
    bucket by chance, silently degenerating the design to a level shift
    instead of a class × covariate interaction. Sorting the distinct
    values and assigning ``rank % 2`` guarantees a genuine split across
    both buckets whenever at least two distinct values are present.
    """
    distinct = sorted(set(values.tolist()))
    bucket_by_value = {value: rank % 2 for rank, value in enumerate(distinct)}
    return np.asarray([bucket_by_value[v] for v in values], dtype=np.int64)


def _resolve_covariate_column(
    universe: pl.DataFrame, *, candidates: tuple[str, ...]
) -> str:
    """First candidate column present on the universe frame, in preference order."""
    for column in candidates:
        if column in universe.columns:
            return column
    raise ValueError(
        f"covariate_joint design requires one of {candidates!r} in the universe "
        f"frame; available columns: {universe.columns}"
    )


def _blocked_games(game_ids: list[str], *, config: MaskConfig, n: int) -> BoolArray:
    """Whole-game blocked/not-blocked draw sized so the blocked share hits the overall target."""
    if config.block_probability <= 0.0:
        raise ValueError(
            f"scorer_blocked design requires block_probability > 0, got "
            f"{config.block_probability}"
        )
    block_share = float(
        np.clip(config.overall_masked_target / config.block_probability, 0.0, 1.0)
    )
    threshold = int(round(block_share * config.block_bucket_count))
    return np.fromiter(
        (
            game_hash_fold(
                f"{config.block_salt}|{g}", fold_count=config.block_bucket_count
            )
            < threshold
            for g in game_ids
        ),
        dtype=np.bool_,
        count=n,
    )


def generate_mask(
    universe: pl.DataFrame,
    *,
    variant: BacktestVariant,
    seed: int,
    config: MaskConfig | None = None,
) -> MaskResult:
    """Draw the seeded synthetic MNAR mask over the truth universe.

    Default design ``config.design == "w_class_intensity"``: ``P(masked)
    = clip(w_class[label] * intensity[bucket(game)], 0, max)``, where the
    bucket is a salted game-id hash into the intensity tiers (simulating
    scorer-dependent missingness) and the never-masked game-hash holdout
    fold gets probability zero. Three further designs are registered for
    backtest-robustness checks (referee major 2), selected by
    ``config.design``: ``"covariate_joint"`` multiplies in a two-level
    real-covariate effect so the true selection is no longer a pure
    per-class function; ``"scorer_blocked"`` masks whole games at a high,
    class-independent rate instead of per-class weighting;
    ``"era_graded"`` assigns the same intensity tiers by season quantile
    (oldest seasons highest) instead of a salted game hash. Rows must
    arrive sorted by ``event_key`` (``load_truth_universe`` guarantees
    it) so the draw is deterministic for a seed.
    """
    config = config if config is not None else MaskConfig()
    labels = np.asarray(universe.get_column(TRUE_LABEL_COLUMN).to_list(), dtype=np.str_)
    game_ids = universe.get_column("game_id").to_list()
    n = labels.shape[0]

    holdout = np.fromiter(
        (
            game_hash_fold(str(g), fold_count=config.holdout_fold_count)
            == config.holdout_fold_id
            for g in game_ids
        ),
        dtype=np.bool_,
        count=n,
    )
    maskable = ~holdout
    if not maskable.any():
        raise ValueError("every universe game landed in the never-masked holdout fold")

    covariate_column: str | None = None
    covariate_bucket_idx: npt.NDArray[np.int64] | None = None
    blocked: BoolArray | None = None

    if config.design == "w_class_intensity":
        tiers = np.asarray(config.intensity_tiers, dtype=np.float64)
        intensity, tier_idx = _hash_bucket_intensity(
            game_ids, tiers=tiers, salt=config.intensity_salt
        )
        class_probabilities = calibrate_class_mask_probabilities(
            labels[maskable],
            focal_class=variant.focal_class,
            class_labels=variant.class_labels,
            config=config,
        )
        base = np.asarray(
            [class_probabilities[label] for label in labels], dtype=np.float64
        )
        p = np.clip(base * intensity, 0.0, config.max_mask_probability)
        bucket_tiers_for_summary = tiers
    elif config.design == "era_graded":
        if "season" not in universe.columns:
            raise ValueError(
                "era_graded design requires a 'season' column in the universe frame"
            )
        tiers = np.asarray(config.intensity_tiers, dtype=np.float64)
        seasons = np.asarray(universe.get_column("season").to_list(), dtype=np.int64)
        intensity, tier_idx = _era_graded_intensity(seasons, tiers=tiers)
        class_probabilities = calibrate_class_mask_probabilities(
            labels[maskable],
            focal_class=variant.focal_class,
            class_labels=variant.class_labels,
            config=config,
        )
        base = np.asarray(
            [class_probabilities[label] for label in labels], dtype=np.float64
        )
        p = np.clip(base * intensity, 0.0, config.max_mask_probability)
        bucket_tiers_for_summary = np.sort(tiers)[::-1]
    elif config.design == "covariate_joint":
        tiers = np.asarray(config.intensity_tiers, dtype=np.float64)
        intensity, tier_idx = _hash_bucket_intensity(
            game_ids, tiers=tiers, salt=config.intensity_salt
        )
        covariate_column = _resolve_covariate_column(
            universe, candidates=config.covariate_columns
        )
        covariate_values = np.asarray(
            universe.get_column(covariate_column)
            .cast(pl.Utf8)
            .fill_null("__null__")
            .to_list(),
            dtype=np.str_,
        )
        covariate_bucket_idx = _covariate_bucket(covariate_values)
        covariate_multiplier = np.asarray(config.covariate_levels, dtype=np.float64)[
            covariate_bucket_idx
        ]
        class_probabilities = calibrate_class_mask_probabilities(
            labels[maskable],
            focal_class=variant.focal_class,
            class_labels=variant.class_labels,
            config=config,
        )
        base = np.asarray(
            [class_probabilities[label] for label in labels], dtype=np.float64
        )
        p = np.clip(
            base * intensity * covariate_multiplier, 0.0, config.max_mask_probability
        )
        bucket_tiers_for_summary = tiers
    elif config.design == "scorer_blocked":
        blocked = _blocked_games(game_ids, config=config, n=n)
        p = np.clip(
            np.where(blocked, config.block_probability, 0.0),
            0.0,
            config.max_mask_probability,
        )
        tier_idx = blocked.astype(np.int64)
        bucket_tiers_for_summary = np.asarray([0.0, config.block_probability])
        class_probabilities = {
            label: config.block_probability for label in variant.class_labels
        }
    else:
        raise ValueError(f"unknown mask design {config.design!r}")

    p[holdout] = 0.0

    rng = np.random.default_rng(seed)
    masked = rng.random(n) < p

    masked_maskable = masked[maskable]
    labels_maskable = labels[maskable]
    observed_after_mask = labels_maskable[~masked_maskable]
    overall_masked_share = float(np.mean(masked_maskable))
    observed_shares = _share_by_class(observed_after_mask, variant.class_labels)
    truth_shares = _share_by_class(labels_maskable, variant.class_labels)
    masked_share_by_class = {
        label: (
            float(np.mean(masked_maskable[labels_maskable == label]))
            if int((labels_maskable == label).sum())
            else 0.0
        )
        for label in variant.class_labels
    }
    masked_share_by_bucket = {
        f"tier_{bucket_tiers_for_summary[t]:.2f}": (
            float(np.mean(masked[maskable & (tier_idx == t)]))
            if int((maskable & (tier_idx == t)).sum())
            else 0.0
        )
        for t in range(bucket_tiers_for_summary.shape[0])
    }
    observed_focal_share = observed_shares[variant.focal_class]
    summary: dict[str, Any] = {
        "n_rows": int(n),
        "n_maskable_rows": int(maskable.sum()),
        "n_holdout_rows": int(holdout.sum()),
        "n_masked_rows": int(masked.sum()),
        "overall_masked_share": overall_masked_share,
        "overall_masked_target": config.overall_masked_target,
        "overall_masked_range": list(OVERALL_MASKED_RANGE),
        "overall_masked_in_range": bool(
            OVERALL_MASKED_RANGE[0] <= overall_masked_share <= OVERALL_MASKED_RANGE[1]
        ),
        "focal_class": variant.focal_class,
        "focal_depletion_ratio": config.focal_depletion_ratio,
        "truth_shares_maskable": truth_shares,
        "observed_shares_after_mask": observed_shares,
        "observed_focal_share": observed_focal_share,
        "observed_focal_range": (
            list(variant.observed_focal_range)
            if variant.observed_focal_range is not None
            else None
        ),
        "observed_focal_in_range": (
            bool(
                variant.observed_focal_range[0]
                <= observed_focal_share
                <= variant.observed_focal_range[1]
            )
            if variant.observed_focal_range is not None
            else None
        ),
        "masked_share_by_class": masked_share_by_class,
        "masked_share_by_bucket": masked_share_by_bucket,
        "class_mask_probabilities": class_probabilities,
        "intensity_tiers": list(config.intensity_tiers),
        "holdout_fold": {
            "fold_count": config.holdout_fold_count,
            "fold_id": config.holdout_fold_id,
        },
        "design": config.design,
    }
    if config.design == "covariate_joint":
        assert covariate_column is not None
        assert covariate_bucket_idx is not None
        bucket_maskable = covariate_bucket_idx[maskable]
        masked_share_by_covariate_level = {
            f"level_{config.covariate_levels[b]:.2f}": (
                float(np.mean(masked_maskable[bucket_maskable == b]))
                if int((bucket_maskable == b).sum())
                else 0.0
            )
            for b in (0, 1)
        }
        summary.update(
            {
                "covariate_column": covariate_column,
                "covariate_levels": list(config.covariate_levels),
                "masked_share_by_covariate_level": masked_share_by_covariate_level,
            }
        )
    elif config.design == "scorer_blocked":
        assert blocked is not None
        maskable_game_ids = {game_ids[i] for i in range(n) if maskable[i]}
        blocked_maskable_game_ids = {
            game_ids[i] for i in range(n) if maskable[i] and blocked[i]
        }
        summary.update(
            {
                "block_probability": config.block_probability,
                "n_blocked_games": len(blocked_maskable_game_ids),
                "n_total_maskable_games": len(maskable_game_ids),
            }
        )
    _log.info(
        "mask generated: masked=%d/%d (%.3f) holdout=%d observed_focal_share=%.3f "
        "truth_focal_share=%.3f",
        int(masked.sum()),
        int(maskable.sum()),
        overall_masked_share,
        int(holdout.sum()),
        observed_focal_share,
        truth_shares[variant.focal_class],
    )
    _log.debug("mask summary: %s", summary)
    return MaskResult(masked=masked, holdout=holdout, summary=summary)


def _with_masked_status(
    frame: pl.DataFrame, *, masked_column: str, masked_status: str
) -> pl.DataFrame:
    masked = pl.col(masked_column)
    exprs: list[pl.Expr] = [
        pl.when(masked)
        .then(pl.lit(masked_status))
        .otherwise(pl.col("observed_status"))
        .alias("observed_status"),
        pl.when(masked)
        .then(pl.lit(None))
        .otherwise(pl.col("raw_value"))
        .alias("raw_value"),
    ]
    for column in ("class", "deduced_value"):
        if column in frame.columns:
            exprs.append(
                pl.when(masked)
                .then(pl.lit(None))
                .otherwise(pl.col(column))
                .alias(column)
            )
    for column in ("is_observed", "is_observed_class"):
        if column in frame.columns:
            exprs.append((pl.col(column) & ~masked).alias(column))
    return frame.with_columns(exprs)


def build_observation_frame(
    universe: pl.DataFrame, *, masked: BoolArray, variant: BacktestVariant
) -> pl.DataFrame:
    """Obs-dataset-shaped frame for the synthetic propensity fit.

    Mirrors ``model_input_observation_batted_ball`` closely enough for
    ``prepare_event_observation_inputs`` / ``build_observation_scoring_frame``:
    the geometry universe already carries every random-effect, fixed-effect
    and continuous column the obs prep reads (both datasets join
    ``event_observation_context`` upstream), so the builder reuses the
    real columns, renames the dimension column, and sets ``is_observed =
    NOT masked`` with ``training_weight = 1.0`` on every row.
    """
    frame = universe.drop(TRUE_LABEL_COLUMN)
    if variant.dimension_column != "dimension":
        frame = frame.rename({variant.dimension_column: "dimension"})
    frame = frame.with_columns(
        pl.Series("_masked", masked),
        pl.lit(1.0).alias("training_weight"),
    )
    frame = frame.with_columns(pl.Series("is_observed", ~masked))
    frame = _with_masked_status(
        frame, masked_column="_masked", masked_status=variant.masked_status
    ).drop("_masked")
    _log.info(
        "observation frame built: rows=%d observed=%d masked=%d",
        frame.height,
        int((~masked).sum()),
        int(masked.sum()),
    )
    return frame


def build_imputation_frame(
    universe: pl.DataFrame,
    *,
    masked: BoolArray,
    holdout: BoolArray,
    variant: BacktestVariant,
    propensity: pl.DataFrame,
    propensity_artifact_id: str,
) -> tuple[pl.DataFrame, pl.DataFrame]:
    """Imputation-dataset-shaped frame plus the truth side table.

    Masked rows mirror the dataset's unobserved convention
    (``observed_status = 'unknown_code'``, ``raw_value`` / ``class``
    NULL) so the imputation prep trains on the unmasked rows and the
    production scoring slice is exactly the masked rows. The
    ``propensity_p_observed`` column is the synthetic-mask fit's
    ``p_observed_mean`` joined by ``event_key`` — never the real
    published Model A.
    """
    truth = pl.DataFrame(
        {
            "event_key": universe.get_column("event_key"),
            "true_label": universe.get_column(TRUE_LABEL_COLUMN),
            "is_masked": pl.Series(masked),
            "is_holdout": pl.Series(holdout),
        }
    )
    frame = universe.drop(TRUE_LABEL_COLUMN).with_columns(pl.Series("_masked", masked))
    frame = _with_masked_status(
        frame, masked_column="_masked", masked_status=variant.masked_status
    ).drop("_masked")
    propensity_join = propensity.select(
        pl.col("event_key").cast(pl.Int64),
        pl.col("p_observed_mean").cast(pl.Float64).alias(PROPENSITY_COLUMN),
    )
    frame = frame.with_columns(pl.col("event_key").cast(pl.Int64)).join(
        propensity_join, on="event_key", how="left"
    )
    frame = frame.with_columns(
        pl.lit(propensity_artifact_id).alias(PROPENSITY_ARTIFACT_COLUMN)
    )
    null_propensity = int(frame.get_column(PROPENSITY_COLUMN).null_count())
    if null_propensity == frame.height:
        raise ValueError(
            "synthetic propensity join produced 100% NULL "
            f"{PROPENSITY_COLUMN}; the propensity export does not cover the "
            "universe event keys"
        )
    _log.info(
        "imputation frame built: rows=%d masked=%d propensity_nulls=%d (%.4f)",
        frame.height,
        int(masked.sum()),
        null_propensity,
        null_propensity / max(frame.height, 1),
    )
    return frame, truth


def write_dataset_artifact(
    frame: pl.DataFrame,
    *,
    dataset_name: str,
    artifact_id: str,
    datasets_root: Path,
    source_snapshot_id: str,
) -> Path:
    dataset_dir = datasets_root / dataset_name / artifact_id
    dataset_dir.mkdir(parents=True, exist_ok=True)
    dataset_path = dataset_dir / "dataset.parquet"
    frame.write_parquet(dataset_path)
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="dataset",
        name=dataset_name,
        version="0.1.0",
        created_at=utc_now(),
        source_snapshot_id=source_snapshot_id,
        output_paths={"dataset": dataset_path},
        package_versions={},
    )
    write_manifest(manifest, dataset_dir / "manifest.json")
    _log.info(
        "wrote dataset artifact name=%s artifact_id=%s rows=%d path=%s",
        dataset_name,
        artifact_id,
        frame.height,
        dataset_path,
    )
    return dataset_path


def recovered_class_shares(
    export_df: pl.DataFrame, *, variant: BacktestVariant
) -> dict[str, float]:
    """Masked-slice class shares from a fit's production export.

    Equal-weight per event, matching the downstream gather aggregation:
    the recovered share of class ``k`` is the mean of ``expected_share``
    for ``k`` over the exported events.
    """
    if variant.export_kind == "geometry":
        grouped = export_df.group_by("class_label").agg(
            pl.col("expected_share").mean().alias("_share")
        )
        raw = dict(
            zip(
                grouped.get_column("class_label").to_list(),
                grouped.get_column("_share").to_list(),
            )
        )
    else:
        grouped = export_df.group_by("fielding_position").agg(
            pl.col("expected_share").mean().alias("_share")
        )
        raw = {
            str(int(position)): share
            for position, share in zip(
                grouped.get_column("fielding_position").to_list(),
                grouped.get_column("_share").to_list(),
            )
        }
    shares = {label: float(raw.get(label, 0.0)) for label in variant.class_labels}
    total = sum(shares.values())
    if not math.isclose(total, 1.0, abs_tol=1e-6):
        raise AssertionError(
            f"recovered class shares sum to {total:.8f}, expected 1.0 "
            f"(export rows={export_df.height})"
        )
    return shares


SELECTION_OFFSET_CLIP: float = 1e-6


def oracle_selection_offset(
    *,
    class_mask_probabilities: dict[str, float],
    mean_intensity: float,
    class_labels: tuple[str, ...],
    max_mask_probability: float,
) -> dict[str, float]:
    """Oracle per-class selection log-odds offset from the calibrated mask.

    ``delta_c = logit(clip(w_class[c] * mean_intensity, eps, max))`` — the
    log-odds of being masked for class ``c`` at average mask intensity.
    This is the quantity the production scoring offset would supply; here
    it is known exactly because the harness controls the mask.
    """
    offset: dict[str, float] = {}
    for label in class_labels:
        w = float(class_mask_probabilities.get(label, 0.0))
        p = float(
            np.clip(w * mean_intensity, SELECTION_OFFSET_CLIP, max_mask_probability)
        )
        offset[label] = float(math.log(p / (1.0 - p)))
    return offset


def realized_class_selection_offset(
    masked_share_by_class: dict[str, float],
    *,
    class_labels: tuple[str, ...],
    max_mask_probability: float,
) -> dict[str, float]:
    """Empirical per-class selection log-odds offset from the mask's realized rate.

    Unlike ``oracle_selection_offset``, which assumes the ``w_class_intensity``
    design's closed-form ``w_class[c] * mean_intensity``, this reads the
    mask's actually-realized empirical per-class masked rate directly, so it
    is the correct oracle for any design's true selection mechanism —
    including ``covariate_joint``, where the true process also depends on a
    covariate the imputation model conditions on, so a pure per-class offset
    is a deliberately partial correction there; and ``scorer_blocked``, where
    selection is class-independent by construction, so this offset should
    come out approximately equal across classes (a near-no-op after the
    reweight's per-event renormalization).
    """
    offset: dict[str, float] = {}
    for label in class_labels:
        rate = float(masked_share_by_class.get(label, 0.0))
        p = float(np.clip(rate, SELECTION_OFFSET_CLIP, max_mask_probability))
        offset[label] = float(math.log(p / (1.0 - p)))
    return offset


def reweight_class_shares_per_event(
    export_df: pl.DataFrame,
    *,
    variant: BacktestVariant,
    offset: dict[str, float],
) -> dict[str, float]:
    """Per-event ``shares_c · exp(delta_c)`` renormalized, then event-averaged.

    Applies the fixed per-class offset as a pure post-hoc reweight of an
    existing export's per-event class shares — equivalent to scoring a
    ``gamma_propensity_zero`` fit with ``selection_offset`` in the softmax
    reconstruction (the Task-1 mechanism). An all-zero offset is the
    identity.
    """
    if variant.export_kind == "geometry":
        class_column, share_column = "class_label", "expected_share"
        label_expr = pl.col(class_column).cast(pl.Utf8)
    else:
        class_column, share_column = "fielding_position", "expected_share"
        label_expr = pl.col(class_column).cast(pl.Int64).cast(pl.Utf8)

    weight_by_label = {
        label: math.exp(offset.get(label, 0.0)) for label in variant.class_labels
    }
    reweighted = (
        export_df.with_columns(label_expr.alias("_label"))
        .with_columns(
            pl.col("_label")
            .replace_strict(weight_by_label, default=1.0, return_dtype=pl.Float64)
            .alias("_weight")
        )
        .with_columns((pl.col(share_column) * pl.col("_weight")).alias("_num"))
    )
    per_event = reweighted.group_by("event_key").agg(
        pl.col("_num").sum().alias("_denom")
    )
    reweighted = reweighted.join(per_event, on="event_key", how="left").with_columns(
        (pl.col("_num") / pl.col("_denom")).alias("_corrected")
    )
    grouped = reweighted.group_by("_label").agg(
        pl.col("_corrected").mean().alias("_share")
    )
    raw = {
        str(label): float(share)
        for label, share in zip(
            grouped.get_column("_label").to_list(),
            grouped.get_column("_share").to_list(),
        )
    }
    shares = {label: float(raw.get(label, 0.0)) for label in variant.class_labels}
    total = sum(shares.values())
    if not math.isclose(total, 1.0, abs_tol=1e-6):
        raise AssertionError(
            f"reweighted class shares sum to {total:.8f}, expected 1.0"
        )
    return shares


def assert_export_covers_masked_slice(
    export_df: pl.DataFrame, *, masked_event_keys: set[int]
) -> None:
    export_keys = {int(k) for k in export_df.get_column("event_key").unique().to_list()}
    if export_keys != masked_event_keys:
        missing = len(masked_event_keys - export_keys)
        extra = len(export_keys - masked_event_keys)
        raise AssertionError(
            f"production export does not match the masked slice: "
            f"{missing} masked events missing from the export, "
            f"{extra} exported events not in the masked slice"
        )


def _total_variation(
    a: dict[str, float], b: dict[str, float], class_labels: tuple[str, ...]
) -> float:
    return 0.5 * float(sum(abs(a[label] - b[label]) for label in class_labels))


def _diagnostics_clean(
    diagnostics: dict[str, Any], thresholds: dict[str, float]
) -> bool:
    try:
        divergences = int(diagnostics["divergences"])
        rhat_max = float(diagnostics["rhat_max"])
        ess_bulk_min = float(diagnostics["ess_bulk_min"])
    except (KeyError, TypeError, ValueError):
        return False
    return (
        divergences <= int(thresholds["max_divergences"])
        and rhat_max <= thresholds["rhat_max"]
        and ess_bulk_min >= thresholds["ess_bulk_min"]
    )


def _held_out_scalar(held_out: dict[str, Any], key: str) -> float | None:
    value = held_out.get(key)
    if isinstance(value, (int, float)) and math.isfinite(float(value)):
        return float(value)
    return None


def evaluate_criteria(
    *,
    truth_shares: dict[str, float],
    corrected_shares: dict[str, float],
    uncorrected_shares: dict[str, float],
    class_labels: tuple[str, ...],
    focal_class: str,
    corrected_held_out: dict[str, Any],
    uncorrected_held_out: dict[str, Any],
    corrected_diagnostics: dict[str, Any],
    uncorrected_diagnostics: dict[str, Any],
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Score the paired fits against the held-back truth and gate to booleans.

    Criteria: (1) the corrected fit's focal-class share error on the
    masked slice is smaller than the uncorrected fit's, with relative
    reduction at or above the floor; (2) the corrected total-variation
    distance to the truth shares is smaller; (3) no regression on the
    never-masked fold (top-1 within tolerance below, log-loss within
    tolerance above); (4) both fits' sampler diagnostics are clean.
    Overall pass requires all four.
    """
    thresholds = dict(THRESHOLDS if thresholds is None else thresholds)

    focal_error_corrected = abs(
        corrected_shares[focal_class] - truth_shares[focal_class]
    )
    focal_error_uncorrected = abs(
        uncorrected_shares[focal_class] - truth_shares[focal_class]
    )
    if focal_error_uncorrected > 0.0:
        relative_reduction = (
            focal_error_uncorrected - focal_error_corrected
        ) / focal_error_uncorrected
    else:
        relative_reduction = None
    focal_share_error_reduced = (
        relative_reduction is not None
        and focal_error_corrected < focal_error_uncorrected
        and relative_reduction >= thresholds["focal_relative_reduction_min"]
    )

    tv_corrected = _total_variation(corrected_shares, truth_shares, class_labels)
    tv_uncorrected = _total_variation(uncorrected_shares, truth_shares, class_labels)
    total_variation_reduced = tv_corrected < tv_uncorrected

    top1_corrected = _held_out_scalar(corrected_held_out, "top1_accuracy")
    top1_uncorrected = _held_out_scalar(uncorrected_held_out, "top1_accuracy")
    log_loss_corrected = _held_out_scalar(corrected_held_out, "log_loss")
    log_loss_uncorrected = _held_out_scalar(uncorrected_held_out, "log_loss")
    held_out_non_regression = (
        top1_corrected is not None
        and top1_uncorrected is not None
        and log_loss_corrected is not None
        and log_loss_uncorrected is not None
        and top1_corrected >= top1_uncorrected - thresholds["top1_tolerance"]
        and log_loss_corrected
        <= log_loss_uncorrected + thresholds["log_loss_tolerance"]
    )

    sampler_diagnostics_clean = _diagnostics_clean(
        corrected_diagnostics, thresholds
    ) and _diagnostics_clean(uncorrected_diagnostics, thresholds)

    criteria = {
        "focal_share_error_reduced": bool(focal_share_error_reduced),
        "total_variation_reduced": bool(total_variation_reduced),
        "held_out_non_regression": bool(held_out_non_regression),
        "sampler_diagnostics_clean": bool(sampler_diagnostics_clean),
    }
    overall_pass = all(criteria.values())
    payload: dict[str, Any] = {
        "masked_slice": {
            "focal_class": focal_class,
            "class_labels": list(class_labels),
            "truth_shares": truth_shares,
            "corrected_shares": corrected_shares,
            "uncorrected_shares": uncorrected_shares,
            "focal_share_error": {
                "corrected": focal_error_corrected,
                "uncorrected": focal_error_uncorrected,
                "relative_reduction": relative_reduction,
            },
            "total_variation": {
                "corrected": tv_corrected,
                "uncorrected": tv_uncorrected,
            },
        },
        "held_out": {
            "corrected": {
                "top1_accuracy": top1_corrected,
                "log_loss": log_loss_corrected,
            },
            "uncorrected": {
                "top1_accuracy": top1_uncorrected,
                "log_loss": log_loss_uncorrected,
            },
        },
        "diagnostics": {
            "corrected": corrected_diagnostics,
            "uncorrected": uncorrected_diagnostics,
        },
        "thresholds": thresholds,
        "criteria": criteria,
        "overall_pass": overall_pass,
    }
    _log.info(
        "criteria evaluated: focal_err corrected=%.4f uncorrected=%.4f "
        "tv corrected=%.4f uncorrected=%.4f criteria=%s overall_pass=%s",
        focal_error_corrected,
        focal_error_uncorrected,
        tv_corrected,
        tv_uncorrected,
        criteria,
        overall_pass,
    )
    return payload


def evaluate_offset_arm(
    *,
    truth_shares: dict[str, float],
    offset_shares: dict[str, float],
    uncorrected_shares: dict[str, float],
    class_labels: tuple[str, ...],
    focal_class: str,
    uncorrected_held_out: dict[str, Any],
    uncorrected_diagnostics: dict[str, Any],
    selection_offset: dict[str, float],
    thresholds: dict[str, float] | None = None,
) -> dict[str, Any]:
    """Score the oracle-offset arm against the same gate as the class arm.

    The offset is a pure post-hoc reweight of the uncorrected (``noprop``)
    arm's exported shares, so there is no separate fit: held-out metrics
    and sampler diagnostics are the uncorrected fit's (the reweight only
    touches the masked-slice imputation, never the held-out top-1 /
    log-loss). Focal-share relative reduction, total variation, and the
    diagnostics gate are evaluated exactly as in ``evaluate_criteria``.
    """
    thresholds = dict(THRESHOLDS if thresholds is None else thresholds)

    focal_error_offset = abs(offset_shares[focal_class] - truth_shares[focal_class])
    focal_error_uncorrected = abs(
        uncorrected_shares[focal_class] - truth_shares[focal_class]
    )
    if focal_error_uncorrected > 0.0:
        relative_reduction = (
            focal_error_uncorrected - focal_error_offset
        ) / focal_error_uncorrected
    else:
        relative_reduction = None
    focal_share_error_reduced = (
        relative_reduction is not None
        and focal_error_offset < focal_error_uncorrected
        and relative_reduction >= thresholds["focal_relative_reduction_min"]
    )

    tv_offset = _total_variation(offset_shares, truth_shares, class_labels)
    tv_uncorrected = _total_variation(uncorrected_shares, truth_shares, class_labels)
    total_variation_reduced = tv_offset < tv_uncorrected

    sampler_diagnostics_clean = _diagnostics_clean(uncorrected_diagnostics, thresholds)

    criteria = {
        "focal_share_error_reduced": bool(focal_share_error_reduced),
        "total_variation_reduced": bool(total_variation_reduced),
        "held_out_non_regression": True,
        "sampler_diagnostics_clean": bool(sampler_diagnostics_clean),
    }
    arm_pass = all(criteria.values())
    payload: dict[str, Any] = {
        "selection_offset": dict(selection_offset),
        "shares": offset_shares,
        "focal_share_error": {
            "offset": focal_error_offset,
            "uncorrected": focal_error_uncorrected,
            "relative_reduction": relative_reduction,
        },
        "tv": {"offset": tv_offset, "uncorrected": tv_uncorrected},
        "held_out": dict(uncorrected_held_out),
        "criteria": criteria,
        "pass": arm_pass,
    }
    _log.info(
        "offset arm evaluated: focal_err offset=%.4f uncorrected=%.4f "
        "relative_reduction=%s tv offset=%.4f uncorrected=%.4f pass=%s",
        focal_error_offset,
        focal_error_uncorrected,
        relative_reduction,
        tv_offset,
        tv_uncorrected,
        arm_pass,
    )
    return payload


def run_backtest(
    *,
    model: BacktestModelName,
    seed: int = DEFAULT_SEED,
    budget: int | None = None,
    smoke: bool = False,
    run_id: str | None = None,
    dataset_parquet: Path | None = None,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    min_season: int = DEFAULT_MIN_SEASON,
    mask_config: MaskConfig | None = None,
) -> BacktestResult:
    """End-to-end masked backtest: mask, synthetic propensity fit, paired fits.

    All temp dataset and fit artifacts land under
    ``<output_root>/<run_id>/`` alongside ``mask_summary.json`` and
    ``metrics.json``. The run id defaults to a function of the model,
    seed, and budget — never wall clock — so reruns overwrite in place.
    """
    from python_models.statistical.bayes.specs import GammaPropensityFlavor
    from python_models.statistical.bayes.training import run_bayes_model

    variant = VARIANTS[model]
    resolved_budget = (
        budget if budget is not None else (SMOKE_BUDGET if smoke else DEFAULT_BUDGET)
    )
    resolved_mask_config = mask_config if mask_config is not None else MaskConfig()
    _base_run_id = f"{model}-seed{seed}-budget{resolved_budget}" + (
        "-smoke" if smoke else ""
    )
    resolved_run_id = run_id or (
        _base_run_id
        if resolved_mask_config.design == "w_class_intensity"
        else f"{resolved_mask_config.design}-{_base_run_id}"
    )
    run_dir = output_root / resolved_run_id
    datasets_root = run_dir / "datasets"
    bayes_root = run_dir / "bayes"
    run_dir.mkdir(parents=True, exist_ok=True)
    _log.info(
        "mnar masked backtest start: model=%s run_id=%s seed=%d budget=%d smoke=%s "
        "run_dir=%s",
        model,
        resolved_run_id,
        seed,
        resolved_budget,
        smoke,
        run_dir,
    )

    resolved_parquet = (
        dataset_parquet
        if dataset_parquet is not None
        else resolve_default_dataset_parquet(variant)
    )
    universe = load_truth_universe(
        resolved_parquet, variant=variant, min_season=min_season
    )
    mask = generate_mask(universe, variant=variant, seed=seed, config=mask_config)
    mask_summary = dict(mask.summary)
    mask_summary.update({"run_id": resolved_run_id, "model": model, "seed": seed})
    mask_summary_path = run_dir / "mask_summary.json"
    _write_json(mask_summary, mask_summary_path)
    _log.info("wrote %s", mask_summary_path)

    obs_dataset_id = f"{resolved_run_id}-obs"
    obs_frame = build_observation_frame(universe, masked=mask.masked, variant=variant)
    write_dataset_artifact(
        obs_frame,
        dataset_name=variant.obs_dataset_name,
        artifact_id=obs_dataset_id,
        datasets_root=datasets_root,
        source_snapshot_id=resolved_run_id,
    )

    propensity_artifact_id = f"{resolved_run_id}-prop"
    _log.info(
        "fitting synthetic propensity model %s (artifact_id=%s)",
        variant.propensity_target,
        propensity_artifact_id,
    )
    run_bayes_model(
        model_name=variant.propensity_target,
        dataset_artifact_id=obs_dataset_id,
        artifact_id=propensity_artifact_id,
        source_snapshot_id=resolved_run_id,
        smoke=smoke,
        smoke_limit=resolved_budget,
        seed=seed,
        artifact_root=bayes_root,
        dataset_root=datasets_root,
    )
    propensity_dir = bayes_root / variant.propensity_target / propensity_artifact_id
    propensity_export = pl.read_parquet(
        propensity_dir / "exports" / "event_propensity.parquet"
    )
    propensity_diagnostics = _read_json(
        propensity_dir / "validation" / "diagnostics.json"
    )
    _log.info(
        "synthetic propensity fit complete: scored_rows=%d", propensity_export.height
    )

    imputation_dataset_id = f"{resolved_run_id}-impute"
    imputation_frame, truth = build_imputation_frame(
        universe,
        masked=mask.masked,
        holdout=mask.holdout,
        variant=variant,
        propensity=propensity_export,
        propensity_artifact_id=propensity_artifact_id,
    )
    imputation_dataset_path = write_dataset_artifact(
        imputation_frame,
        dataset_name=variant.imputation_dataset_name,
        artifact_id=imputation_dataset_id,
        datasets_root=datasets_root,
        source_snapshot_id=resolved_run_id,
    )
    truth_path = imputation_dataset_path.parent / "truth.parquet"
    truth.write_parquet(truth_path)
    _log.info("wrote truth side table rows=%d path=%s", truth.height, truth_path)

    fit_artifacts: dict[str, str] = {}
    arms: tuple[tuple[str, GammaPropensityFlavor], ...] = (
        ("corrected", "gamma_propensity_class"),
        ("uncorrected", "gamma_propensity_zero"),
    )
    for arm, flavor in arms:
        fit_artifact_id = f"{resolved_run_id}-{arm}"
        fit_artifacts[arm] = fit_artifact_id
        _log.info(
            "fitting %s imputation model %s flavor=%s (artifact_id=%s)",
            arm,
            variant.imputation_target,
            flavor,
            fit_artifact_id,
        )
        run_bayes_model(
            model_name=variant.imputation_target,
            dataset_artifact_id=imputation_dataset_id,
            artifact_id=fit_artifact_id,
            source_snapshot_id=resolved_run_id,
            smoke=smoke,
            smoke_limit=resolved_budget,
            seed=seed,
            gamma_dl_flavor="gamma_dl_zero",
            gamma_propensity_flavor=flavor,
            artifact_root=bayes_root,
            dataset_root=datasets_root,
        )

    masked_keys = {
        int(k)
        for k in truth.filter(pl.col("is_masked")).get_column("event_key").to_list()
    }
    truth_labels_masked = np.asarray(
        truth.filter(pl.col("is_masked")).get_column("true_label").to_list(),
        dtype=np.str_,
    )
    truth_shares = _share_by_class(truth_labels_masked, variant.class_labels)

    shares: dict[str, dict[str, float]] = {}
    held_out: dict[str, dict[str, Any]] = {}
    diagnostics: dict[str, dict[str, Any]] = {}
    exports: dict[str, pl.DataFrame] = {}
    for arm in ("corrected", "uncorrected"):
        fit_dir = bayes_root / variant.imputation_target / fit_artifacts[arm]
        export_df = pl.read_parquet(fit_dir / "exports" / variant.export_filename)
        assert_export_covers_masked_slice(export_df, masked_event_keys=masked_keys)
        exports[arm] = export_df
        shares[arm] = recovered_class_shares(export_df, variant=variant)
        held_out[arm] = _read_json(fit_dir / "validation" / "held_out_metrics.json")
        diagnostics[arm] = _read_json(fit_dir / "validation" / "diagnostics.json")
        _log.debug("%s arm recovered shares: %s", arm, shares[arm])

    if resolved_mask_config.design == "w_class_intensity":
        mean_intensity = float(np.mean(resolved_mask_config.intensity_tiers))
        selection_offset = oracle_selection_offset(
            class_mask_probabilities=mask.summary["class_mask_probabilities"],
            mean_intensity=mean_intensity,
            class_labels=variant.class_labels,
            max_mask_probability=resolved_mask_config.max_mask_probability,
        )
    else:
        selection_offset = realized_class_selection_offset(
            mask.summary["masked_share_by_class"],
            class_labels=variant.class_labels,
            max_mask_probability=resolved_mask_config.max_mask_probability,
        )
    offset_shares = reweight_class_shares_per_event(
        exports["uncorrected"], variant=variant, offset=selection_offset
    )
    offset_metrics = evaluate_offset_arm(
        truth_shares=truth_shares,
        offset_shares=offset_shares,
        uncorrected_shares=shares["uncorrected"],
        class_labels=variant.class_labels,
        focal_class=variant.focal_class,
        uncorrected_held_out=held_out["uncorrected"],
        uncorrected_diagnostics=diagnostics["uncorrected"],
        selection_offset=selection_offset,
    )

    metrics = evaluate_criteria(
        truth_shares=truth_shares,
        corrected_shares=shares["corrected"],
        uncorrected_shares=shares["uncorrected"],
        class_labels=variant.class_labels,
        focal_class=variant.focal_class,
        corrected_held_out=held_out["corrected"],
        uncorrected_held_out=held_out["uncorrected"],
        corrected_diagnostics=diagnostics["corrected"],
        uncorrected_diagnostics=diagnostics["uncorrected"],
    )
    metrics.update(
        {
            "run_id": resolved_run_id,
            "model": model,
            "seed": seed,
            "budget": resolved_budget,
            "smoke": smoke,
            "min_season": min_season,
            "dataset_parquet": str(resolved_parquet),
            "n_masked_events": len(masked_keys),
            "artifacts": {
                "obs_dataset_artifact_id": obs_dataset_id,
                "imputation_dataset_artifact_id": imputation_dataset_id,
                "propensity": {
                    "model_name": variant.propensity_target,
                    "artifact_id": propensity_artifact_id,
                },
                "corrected": {
                    "model_name": variant.imputation_target,
                    "artifact_id": fit_artifacts["corrected"],
                },
                "uncorrected": {
                    "model_name": variant.imputation_target,
                    "artifact_id": fit_artifacts["uncorrected"],
                },
            },
            "propensity_diagnostics": propensity_diagnostics,
            "offset": offset_metrics,
        }
    )
    metrics_path = run_dir / "metrics.json"
    _write_json(metrics, metrics_path)
    _log.info(
        "mnar masked backtest complete: run_id=%s overall_pass=%s metrics=%s",
        resolved_run_id,
        metrics["overall_pass"],
        metrics_path,
    )
    return BacktestResult(
        run_dir=run_dir,
        metrics_path=metrics_path,
        mask_summary_path=mask_summary_path,
        metrics=metrics,
        overall_pass=bool(metrics["overall_pass"]),
    )
