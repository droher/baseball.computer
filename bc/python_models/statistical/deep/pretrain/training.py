"""Single-fit shared-embedding pretrainer over multi-head pretext targets."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.ml.model_factory import (
    PretrainHeadBuild,
    PretrainModel,
    build_pretrain_model,
    make_pretrain_optimizers,
)
from python_models.statistical.config import DEEP_ROOT
from python_models.statistical.deep.io import (
    load_dataset_parquet,
    partition_by_split,
)
from python_models.statistical.deep.pretrain.artifacts import (
    pretrain_artifact_dir,
    write_pretrain_artifact,
)
from python_models.statistical.deep.pretrain.heads import (
    apply_target_remap,
    build_class_labels,
    encode_head,
)
from python_models.statistical.deep.pretrain.probes import (
    build_fielder_slot_probe_inputs,
    build_fielding_probe_callback,
    build_mc_probe_inputs,
    build_mc_slash_probe_callback,
    build_outfield_arm_probe_callback,
    build_pa_flags,
    build_probe_inputs,
    db_path_for_probe,
    fielding_probe_enabled,
    grounder_to_ss_predicate,
    of_arm_probe_enabled,
    of_fly_with_runner_predicate,
    resolve_canonical_ids,
    resolve_right_fielder_ids,
    resolve_shortstop_ids,
    sample_context_row_index,
    sample_mc_context_indices,
    slash_probe_enabled,
)
from python_models.statistical.deep.pretrain.targets import (
    ADVANCEMENT_CLASS_LABELS,
)
from python_models.statistical.deep.pretrain.spec import HeadSpec, PretrainSpec

_log = logging.getLogger(__name__)

DEFAULT_EPOCHS: int = 15
DEFAULT_STAGE1_EPOCHS: int = 0
DEFAULT_KERAS_BATCH_SIZE: int = 2048
DEFAULT_EARLY_STOPPING_PATIENCE: int = 5
OFFSET_COL_PREFIX: str = "__off_"
HARD_HEAD_NAMES: tuple[str, ...] = (
    "pa_result",
    "trajectory_remapped",
    "batted_to_fielder_class",
)


@dataclass(frozen=True)
class PretrainRunResult:
    artifact_dir: Path
    manifest_path: Path
    train_rows: int
    validate_rows: int
    test_rows: int
    head_val_metrics: dict[str, dict[str, float]]


def _collect_input_stats(
    train_df: pl.DataFrame,
    *,
    layout: FeatureLayout,
) -> tuple[dict[str, Vocabulary], dict[str, float], dict[str, float]]:
    """Build vocabularies keyed by embedding-unit name.

    For grouped columns we union the distinct values across every member of
    the group and emit ONE vocabulary keyed by the group name. Ungrouped
    high-card + low-card columns get a vocabulary keyed by column name.
    Numeric stats are unchanged.
    """
    vocabularies: dict[str, Vocabulary] = {}
    grouped_cols = layout.grouped_columns()

    for group_name, cols in layout.embedding_groups:
        series_list: list[pl.Series] = []
        for col in cols:
            series_list.append(train_df[col].cast(pl.Utf8))
        unioned = pl.concat(series_list)
        values = unioned.drop_nulls().unique().sort().to_list()
        vocabularies[group_name] = Vocabulary(
            column=group_name, values=tuple(str(v) for v in values)
        )

    for col in layout.high_card_columns:
        if col in grouped_cols:
            continue
        values = (
            train_df[col].cast(pl.Utf8).drop_nulls().unique().sort().to_list()
        )
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(str(v) for v in values)
        )
    for col in layout.low_card_columns:
        values = (
            train_df[col].cast(pl.Utf8).drop_nulls().unique().sort().to_list()
        )
        vocabularies[col] = Vocabulary(
            column=col, values=tuple(str(v) for v in values)
        )

    numeric_means: dict[str, float] = {}
    numeric_variances: dict[str, float] = {}
    for col in layout.numeric_columns:
        arr = (
            train_df[col]
            .cast(pl.Float64)
            .fill_null(0.0)
            .to_numpy()
            .astype(np.float64)
        )
        numeric_means[col] = float(arr.mean()) if arr.size > 0 else 0.0
        numeric_variances[col] = (
            max(float(arr.var()), 1e-6) if arr.size > 0 else 1e-6
        )
    return vocabularies, numeric_means, numeric_variances


def _encode_inputs(
    df: pl.DataFrame,
    *,
    layout: FeatureLayout,
    vocabularies: dict[str, Vocabulary],
) -> dict[str, NDArray[Any]]:
    inputs: dict[str, NDArray[Any]] = {}
    for col in layout.high_card_columns:
        unit = layout.embedding_unit_for_column(col)
        encoded = vocabularies[unit].encode(df[col]).to_numpy()
        inputs[col] = encoded.astype(np.int64).reshape(-1, 1)
    for col in layout.low_card_columns:
        encoded = vocabularies[col].encode(df[col]).to_numpy()
        inputs[col] = encoded.astype(np.int64).reshape(-1, 1)
    for col in layout.numeric_columns:
        inputs[col] = (
            df[col]
            .cast(pl.Float32)
            .fill_null(0.0)
            .to_numpy()
            .reshape(-1, 1)
        )
    return inputs


def _vocab_sizes(vocabularies: dict[str, Vocabulary]) -> dict[str, int]:
    return {col: v.size for col, v in vocabularies.items()}


def _apply_all_remaps(
    df: pl.DataFrame, head_specs: tuple[HeadSpec, ...]
) -> pl.DataFrame:
    out = df
    for head in head_specs:
        out = apply_target_remap(out, head)
    return out


def _load_pa_seed_rows() -> list[dict[str, Any]]:
    """Load ``seed_plate_appearance_result_types.csv`` rows for probe flags."""
    import csv

    repo_root = Path(__file__).resolve().parents[5]
    seed_path = (
        repo_root
        / "bc"
        / "seeds"
        / "misc"
        / "seed_plate_appearance_result_types.csv"
    )
    if not seed_path.exists():
        _log.warning("slash_probe seed not found at %s", seed_path)
        return []
    rows: list[dict[str, Any]] = []
    with seed_path.open(encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            rows.append(dict(row))
    return rows


def _offset_logit_cols(head_name: str, num_classes: int) -> tuple[str, ...]:
    return tuple(f"{OFFSET_COL_PREFIX}{head_name}_{i}" for i in range(num_classes))


def _load_offset_artifact(
    *,
    offset_artifact_id: str,
    artifact_root: Path,
) -> tuple[Path, dict[str, int], dict[str, tuple[str, ...]], pl.DataFrame]:
    """Resolve a stage-1 artifact + load its per-head offset parquets.

    Returns artifact dir, head→num_classes, head→class_labels, and a
    single polars frame keyed by ``event_key`` whose columns are
    ``__off_<head>_<i>`` floats per head/class.
    """
    matches = list(artifact_root.rglob(f"{offset_artifact_id}/manifest.json"))
    if not matches:
        raise FileNotFoundError(
            f"offset artifact manifest not found for id={offset_artifact_id!r} "
            f"under {artifact_root}"
        )
    artifact_dir = matches[0].parent
    manifest_payload = json.loads(matches[0].read_text(encoding="utf-8"))
    metadata = manifest_payload.get("metadata", {})
    raw_heads = metadata.get("head_specs_json")
    if not isinstance(raw_heads, str):
        raise ValueError(
            f"offset artifact manifest missing head_specs_json: {matches[0]}"
        )
    head_meta = json.loads(raw_heads)
    head_classes: dict[str, int] = {}
    head_class_labels: dict[str, tuple[str, ...]] = {}
    for entry in head_meta:
        name = str(entry["name"])
        num_classes = int(entry["num_classes"])
        head_classes[name] = num_classes
        labels_raw = entry.get("class_labels") or []
        labels_tuple = tuple(str(x) for x in labels_raw)
        if len(labels_tuple) != num_classes:
            raise ValueError(
                f"offset artifact {offset_artifact_id!r} head {name!r}: "
                f"class_labels length {len(labels_tuple)} != num_classes {num_classes}. "
                "Stage-1 logit columns would be misaligned with stage-2 labels."
            )
        head_class_labels[name] = labels_tuple

    offsets_dir = artifact_dir / "offsets"
    if not offsets_dir.exists():
        raise FileNotFoundError(
            f"offset artifact missing offsets/ directory: {offsets_dir}"
        )
    combined: pl.DataFrame | None = None
    for head_name, num_classes in head_classes.items():
        parquet_path = offsets_dir / f"{head_name}.parquet"
        if not parquet_path.exists():
            raise FileNotFoundError(
                f"offset parquet missing for head={head_name!r}: {parquet_path}"
            )
        frame = pl.read_parquet(parquet_path)
        if "event_key" not in frame.columns:
            raise ValueError(
                f"offset parquet for {head_name!r} lacks event_key column: {parquet_path}"
            )
        expected = _offset_logit_cols(head_name, num_classes)
        present = [c for c in frame.columns if c != "event_key"]
        if tuple(present) != tuple(f"logit_{i}" for i in range(num_classes)):
            raise ValueError(
                f"offset parquet for {head_name!r} has columns {present!r}; "
                f"expected logit_0..logit_{num_classes - 1}"
            )
        rename_map = {f"logit_{i}": expected[i] for i in range(num_classes)}
        frame = frame.rename(rename_map)
        frame = frame.with_columns(
            [pl.col(c).cast(pl.Float32) for c in expected]
        )
        combined = (
            frame
            if combined is None
            else combined.join(frame, on="event_key", how="inner")
        )
    assert combined is not None  # offsets present
    return artifact_dir, head_classes, head_class_labels, combined


def _extract_offsets(
    df: pl.DataFrame,
    *,
    head_classes: dict[str, int],
) -> dict[str, NDArray[np.float32]]:
    out: dict[str, NDArray[np.float32]] = {}
    for head_name, num_classes in head_classes.items():
        cols = _offset_logit_cols(head_name, num_classes)
        sub = df.select(list(cols)).to_numpy().astype(np.float32, copy=False)
        out[f"offset_{head_name}"] = sub
    return out


def _build_metric_log_callback(csv_path: Path) -> list[Any]:
    import keras

    class EpochMetricLogger(keras.callbacks.Callback):
        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            logs = logs or {}
            train_keys: list[tuple[str, float]] = []
            val_keys: list[tuple[str, float]] = []
            other_keys: list[tuple[str, float]] = []
            for k, v in logs.items():
                try:
                    fv = float(v)
                except (TypeError, ValueError):
                    continue
                if k.startswith("val_"):
                    val_keys.append((k, fv))
                elif k in ("loss",) or "_accuracy" in k or "_acc" in k:
                    train_keys.append((k, fv))
                else:
                    other_keys.append((k, fv))
            train_keys.sort()
            val_keys.sort()
            other_keys.sort()
            parts: list[str] = [f"epoch={epoch + 1}"]
            for k, v in train_keys + val_keys + other_keys:
                parts.append(f"{k}={v:.4f}")
            _log.info("epoch_metrics %s", " ".join(parts))

    csv_path.parent.mkdir(parents=True, exist_ok=True)
    return [
        EpochMetricLogger(),
        keras.callbacks.CSVLogger(filename=str(csv_path), append=True),
    ]


def _build_early_stopping_callback(
    *,
    patience: int,
    best_save_path: Path | None = None,
) -> Any:
    """MVPB default: stock val_loss EarlyStopping + ModelCheckpoint.

    Switch back to ``_build_hard_head_callback`` via
    ``BC_PRETRAIN_USE_HARD_HEAD_ES=1`` (kept for the v4-comparison ablation).
    """
    import keras

    use_hh = os.environ.get("BC_PRETRAIN_USE_HARD_HEAD_ES", "0") in (
        "1", "true", "TRUE"
    )
    if use_hh:
        hard_heads_env = os.environ.get("BC_PRETRAIN_HARD_HEADS")
        if hard_heads_env:
            hard_heads = tuple(h.strip() for h in hard_heads_env.split(",") if h.strip())
        else:
            hard_heads = HARD_HEAD_NAMES
        return _build_hard_head_callback(
            hard_heads=hard_heads,
            patience=patience,
            best_save_path=best_save_path,
        )
    callbacks: list[Any] = [
        keras.callbacks.EarlyStopping(
            monitor="val_loss",
            patience=patience,
            restore_best_weights=True,
        )
    ]
    if best_save_path is not None:
        best_save_path.parent.mkdir(parents=True, exist_ok=True)
        callbacks.append(
            keras.callbacks.ModelCheckpoint(
                filepath=str(best_save_path),
                monitor="val_loss",
                save_best_only=True,
                save_weights_only=False,
            )
        )
    return callbacks


def _build_hard_head_callback(
    *,
    hard_heads: tuple[str, ...],
    patience: int,
    best_save_path: Path | None = None,
) -> Any:
    import keras

    class HardHeadEarlyStopping(keras.callbacks.Callback):
        """Early-stop on the weighted mean of hard-head val accuracies.

        Hard heads = the head-specs whose accuracy we treat as the primary
        signal for embedding quality. ``val_loss`` is dominated by easier
        heads (binary hit_or_out) under uncertainty weighting, so it stops
        too early; this callback watches the hard heads directly. When
        ``best_save_path`` is set, the full Keras model is also written to
        disk whenever a new best score is observed, so a mid-fit process
        crash still leaves the best checkpoint behind.
        """

        def __init__(self) -> None:
            super().__init__()
            self.hard_heads = hard_heads
            self.patience = patience
            self.best = float("-inf")
            self.best_epoch = -1
            self.wait = 0
            self.best_weights: list[Any] | None = None
            self.stopped_epoch: int | None = None
            self.best_save_path = best_save_path

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            logs = logs or {}
            scores: list[float] = []
            for h in self.hard_heads:
                v = logs.get(f"val_{h}_sparse_categorical_accuracy")
                if v is None:
                    v = logs.get(f"val_{h}_binary_accuracy")
                if v is not None:
                    scores.append(float(v))
            if not scores:
                return
            score = sum(scores) / len(scores)
            _log.info(
                "hard_head_es epoch=%d score=%.4f best=%.4f wait=%d/%d",
                epoch,
                score,
                self.best,
                self.wait,
                self.patience,
            )
            assert self.model is not None, "callback set_model not called"
            if score > self.best:
                self.best = score
                self.best_epoch = epoch
                self.wait = 0
                self.best_weights = self.model.get_weights()
                if self.best_save_path is not None:
                    self.best_save_path.parent.mkdir(parents=True, exist_ok=True)
                    try:
                        self.model.save(str(self.best_save_path))
                        _log.info(
                            "hard_head_es saved best model epoch=%d path=%s",
                            epoch,
                            self.best_save_path,
                        )
                    except Exception as exc:
                        _log.warning(
                            "hard_head_es best-save failed epoch=%d err=%s",
                            epoch,
                            exc,
                        )
            else:
                self.wait += 1
                if self.wait >= self.patience:
                    self.model.stop_training = True
                    self.stopped_epoch = epoch
                    if self.best_weights is not None:
                        self.model.set_weights(self.best_weights)
                        _log.info(
                            "hard_head_es triggered epoch=%d restored_best_epoch=%d",
                            epoch,
                            self.best_epoch,
                        )

    return HardHeadEarlyStopping()


def _set_stage1_trainable(model: PretrainModel, head_names: set[str]) -> None:
    """Freeze trunk (cross / deep / layernorm); leave embed_* + head Dense + log_sigma trainable."""
    for layer in model.layers:
        if layer.name.startswith("embed_"):
            layer.trainable = True
        elif layer.name in head_names:
            layer.trainable = True
        else:
            layer.trainable = False


def _set_all_trainable(model: PretrainModel) -> None:
    for layer in model.layers:
        layer.trainable = True


def run_pretrain(
    spec: PretrainSpec,
    *,
    dataset_parquet: Path,
    artifact_id: str,
    layout: FeatureLayout,
    source_snapshot_id: str,
    dataset_artifact_id: str | None = None,
    artifact_root: Path = DEEP_ROOT,
    epochs: int = DEFAULT_EPOCHS,
    stage1_epochs: int = DEFAULT_STAGE1_EPOCHS,
    keras_batch_size: int = DEFAULT_KERAS_BATCH_SIZE,
    early_stopping_patience: int = DEFAULT_EARLY_STOPPING_PATIENCE,
    extra_callbacks: tuple[Any, ...] = (),
) -> PretrainRunResult:
    import keras

    artifact_dir = pretrain_artifact_dir(spec.name, artifact_id, root=artifact_root)
    manifest_path = artifact_dir / "manifest.json"
    if manifest_path.exists():
        raise FileExistsError(
            f"pretrain artifact already present at {manifest_path}; "
            "delete the artifact directory to rebuild."
        )

    _log.info(
        "run_pretrain start name=%s artifact_id=%s dataset=%s",
        spec.name,
        artifact_id,
        spec.dataset_name,
    )
    df = load_dataset_parquet(str(dataset_parquet))
    df = _apply_all_remaps(df, spec.head_specs)

    if spec.row_filter_predicate:
        before_rows = int(df.height)
        df = pl.SQLContext({"df": df}).execute(
            f"SELECT * FROM df WHERE {spec.row_filter_predicate}"
        ).collect()
        _log.info(
            "row_filter_predicate applied predicate=%r rows %d -> %d",
            spec.row_filter_predicate,
            before_rows,
            int(df.height),
        )

    limit_raw = os.environ.get("BC_PRETRAIN_DATASET_LIMIT")
    if limit_raw:
        limit = int(limit_raw)
        if 0 < limit < int(df.height):
            df = df.sample(n=limit, seed=0)
            _log.info("BC_PRETRAIN_DATASET_LIMIT=%d sub-sampled to %d rows", limit, int(df.height))

    offset_artifact_id = os.environ.get("BC_PRETRAIN_OFFSET_ARTIFACT")
    offset_head_classes: dict[str, int] | None = None
    offset_head_class_labels: dict[str, tuple[str, ...]] | None = None
    if offset_artifact_id:
        offset_artifact_dir, offset_head_classes, offset_head_class_labels, offsets_df = (
            _load_offset_artifact(
                offset_artifact_id=offset_artifact_id,
                artifact_root=artifact_root,
            )
        )
        before_rows = int(df.height)
        df = df.join(offsets_df, on="event_key", how="inner")
        _log.info(
            "offsets joined artifact=%s dir=%s rows_before=%d rows_after=%d heads=%s",
            offset_artifact_id,
            offset_artifact_dir,
            before_rows,
            int(df.height),
            sorted(offset_head_classes.keys()),
        )
        if int(df.height) == 0:
            raise ValueError(
                f"offset join produced 0 rows; check event_key alignment for "
                f"artifact={offset_artifact_id}"
            )
        if int(df.height) != before_rows:
            raise ValueError(
                f"offset join dropped rows ({before_rows} -> {int(df.height)}); "
                f"stage-1 emit and stage-2 fit must use identical row filtering "
                f"(check spec.row_filter_predicate + BC_PRETRAIN_DATASET_LIMIT) "
                f"for artifact={offset_artifact_id}"
            )

    train_df, validate_df, test_df = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )
    if train_df.height == 0:
        raise ValueError("TRAIN partition is empty")

    vocabularies, numeric_means, numeric_variances = _collect_input_stats(
        train_df, layout=layout
    )

    head_class_labels: dict[str, tuple[str, ...]] = {}
    head_class_index: dict[str, dict[str, int]] = {}
    head_builds: list[PretrainHeadBuild] = []
    for head in spec.head_specs:
        if head.kind == "multiclass":
            if offset_head_class_labels is not None and head.name in offset_head_class_labels:
                labels = offset_head_class_labels[head.name]
                if not labels:
                    raise ValueError(
                        f"offset artifact has empty class_labels for head {head.name!r}; "
                        "stage-1 class universe must be non-empty"
                    )
            else:
                labels = build_class_labels(train_df, head)
                if not labels:
                    raise ValueError(
                        f"head {head.name!r} has empty class universe after train_distinct"
                    )
            head_class_labels[head.name] = labels
            head_class_index[head.name] = {lbl: i for i, lbl in enumerate(labels)}
            head_builds.append(
                PretrainHeadBuild(
                    name=head.name,
                    kind="multiclass",
                    num_classes=len(labels),
                    loss_weight=head.loss_weight,
                )
            )
        else:
            head_class_labels[head.name] = ()
            head_builds.append(
                PretrainHeadBuild(
                    name=head.name,
                    kind="binary",
                    num_classes=1,
                    loss_weight=head.loss_weight,
                )
            )

    if offset_head_classes is not None:
        for build in head_builds:
            expected = offset_head_classes.get(build.name)
            if expected is None:
                raise ValueError(
                    f"offset artifact missing head {build.name!r} required by spec {spec.name!r}"
                )
            if int(expected) != int(build.num_classes):
                raise ValueError(
                    f"offset/head class-count mismatch for {build.name!r}: "
                    f"offset={expected} vs spec={build.num_classes}"
                )

    train_rows = int(train_df.height)
    steps_per_epoch = max(1, -(-train_rows // keras_batch_size))
    stage1_epochs_eff = max(0, min(stage1_epochs, epochs))
    stage2_epochs_eff = max(0, epochs - stage1_epochs_eff)
    offset_inputs_dict: dict[str, Any] | None = None
    if offset_head_classes is not None:
        offset_inputs_dict = {
            build.name: keras.Input(
                shape=(int(build.num_classes),),
                dtype="float32",
                name=f"offset_{build.name}",
            )
            for build in head_builds
        }
    loss_type_env = os.environ.get("BC_PRETRAIN_LOSS", "cross_entropy").strip().lower()
    focal_gamma_env = float(os.environ.get("BC_PRETRAIN_FOCAL_GAMMA", "2.0"))
    model = build_pretrain_model(
        layout=layout,
        vocab_sizes=_vocab_sizes(vocabularies),
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
        head_builds=tuple(head_builds),
        steps_per_epoch=steps_per_epoch,
        total_epochs=stage1_epochs_eff if stage1_epochs_eff > 0 else stage2_epochs_eff,
        name=spec.name,
        offset_inputs=offset_inputs_dict,
        loss_type=loss_type_env,
        focal_gamma=focal_gamma_env,
    )
    _log.info("pretrain loss config loss_type=%s focal_gamma=%s", loss_type_env, focal_gamma_env)
    head_names: set[str] = {h.name for h in spec.head_specs}

    from python_models.statistical.deep.embeddings import vocabulary_entity_ids
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)
    embedding_units = layout.embedding_unit_names()
    early_vocab_payload = {
        unit: vocabulary_entity_ids(vocabularies[unit]) for unit in embedding_units
    }
    (exports_dir / "vocab.json").write_text(
        json.dumps(early_vocab_payload, indent=2), encoding="utf-8"
    )
    _log.info(
        "wrote early vocab.json units=%s path=%s",
        list(early_vocab_payload.keys()),
        exports_dir / "vocab.json",
    )

    train_x = _encode_inputs(train_df, layout=layout, vocabularies=vocabularies)
    if offset_head_classes is not None:
        train_x.update(
            _extract_offsets(train_df, head_classes=offset_head_classes)
        )
    train_y: dict[str, NDArray[Any]] = {}
    train_w: dict[str, NDArray[np.float32]] = {}
    for head in spec.head_specs:
        y, w = encode_head(
            train_df,
            head,
            head_class_index.get(head.name),
        )
        train_y[head.name] = y
        train_w[head.name] = w
        eligible = float(w.sum())
        _log.info(
            "encode train head=%s rows=%d eligible=%d (%.1f%%)",
            head.name,
            int(w.shape[0]),
            int(eligible),
            100.0 * eligible / max(1, int(w.shape[0])),
        )

    validation_data: tuple[Any, Any, Any] | None = None
    if validate_df.height > 0:
        val_x = _encode_inputs(
            validate_df, layout=layout, vocabularies=vocabularies
        )
        if offset_head_classes is not None:
            val_x.update(
                _extract_offsets(validate_df, head_classes=offset_head_classes)
            )
        val_y: dict[str, NDArray[Any]] = {}
        val_w: dict[str, NDArray[np.float32]] = {}
        for head in spec.head_specs:
            y, w = encode_head(
                validate_df,
                head,
                head_class_index.get(head.name),
            )
            val_y[head.name] = y
            val_w[head.name] = w
        validation_data = (val_x, val_y, val_w)

    head_loss_fns = dict(model._head_loss_fns)
    head_metric_objs = {k: list(v) for k, v in model._head_metric_objs.items()}

    slash_probe_cb: Any | None = None
    if slash_probe_enabled():
        db_path = db_path_for_probe()
        if db_path is None:
            _log.warning("BC_PRETRAIN_SLASH_PROBE=1 but BC_DB_PATH unset; skipping probe")
        else:
            try:
                probe_rows = resolve_canonical_ids(db_path)
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning("slash_probe roster resolve failed: %s", exc)
                probe_rows = ()
            pa_labels = head_class_labels.get("pa_result", ())
            if probe_rows and pa_labels:
                seed_rows = _load_pa_seed_rows()
                pa_flags = build_pa_flags(pa_labels, seed_rows)
                vocab_lookup = {
                    name: {val: i + 1 for i, val in enumerate(vocab.values)}
                    for name, vocab in vocabularies.items()
                }
                n_ctx_env = int(os.environ.get("BC_PRETRAIN_SLASH_PROBE_CONTEXTS", "256"))
                mc_ctx = sample_mc_context_indices(
                    train_df, n_contexts=n_ctx_env, seed=0
                )
                if mc_ctx is None or mc_ctx.size == 0:
                    _log.warning("slash_probe: MC context sample empty; skipping probe")
                else:
                    probe_x = build_mc_probe_inputs(
                        layout=layout,
                        panel_rows=probe_rows,
                        encoded_train=train_x,
                        context_indices=mc_ctx,
                        vocab_lookup=vocab_lookup,
                    )
                    slash_probe_cb = build_mc_slash_probe_callback(
                        panel_rows=probe_rows,
                        probe_x=probe_x,
                        n_contexts=int(mc_ctx.size),
                        pa_flags=pa_flags,
                    )

    base_extras: tuple[Any, ...] = tuple(extra_callbacks)
    metric_log_cbs = _build_metric_log_callback(artifact_dir / "training_metrics.csv")
    base_extras = (*metric_log_cbs, *base_extras)
    if slash_probe_cb is not None:
        base_extras = (*base_extras, slash_probe_cb)

    vocab_lookup_shared: dict[str, dict[str, int]] = {
        name: {val: i + 1 for i, val in enumerate(vocab.values)}
        for name, vocab in vocabularies.items()
    }

    if fielding_probe_enabled():
        db_path = db_path_for_probe()
        if db_path is None:
            _log.warning(
                "BC_PRETRAIN_FIELDING_PROBE=1 but BC_DB_PATH unset; skipping probe"
            )
        elif "fielder_pos_6" not in layout.high_card_columns:
            _log.warning(
                "fielding_probe: fielder_pos_6 not in high_card_columns; skipping"
            )
        else:
            try:
                ss_rows = resolve_shortstop_ids(db_path)
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning("fielding_probe roster resolve failed: %s", exc)
                ss_rows = ()
            ctx_idx = sample_context_row_index(
                train_df,
                predicate=grounder_to_ss_predicate(),
                label="fielding_probe",
                seed=0,
            )
            if ss_rows and ctx_idx is not None:
                fielding_probe_x = build_fielder_slot_probe_inputs(
                    layout=layout,
                    probe_rows=ss_rows,
                    encoded_train=train_x,
                    slot_column="fielder_pos_6",
                    vocab_lookup=vocab_lookup_shared,
                    context_row_index=ctx_idx,
                )
                fielding_probe_cb = build_fielding_probe_callback(
                    probe_rows=ss_rows,
                    probe_x=fielding_probe_x,
                )
                base_extras = (*base_extras, fielding_probe_cb)

    if of_arm_probe_enabled():
        db_path = db_path_for_probe()
        if db_path is None:
            _log.warning(
                "BC_PRETRAIN_OF_ARM_PROBE=1 but BC_DB_PATH unset; skipping probe"
            )
        elif "fielder_pos_9" not in layout.high_card_columns:
            _log.warning(
                "of_arm_probe: fielder_pos_9 not in high_card_columns; skipping"
            )
        elif "r1_advancement" not in head_class_labels:
            _log.warning(
                "of_arm_probe: r1_advancement head missing from spec; skipping"
            )
        else:
            try:
                rf_rows = resolve_right_fielder_ids(db_path)
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning("of_arm_probe roster resolve failed: %s", exc)
                rf_rows = ()
            ctx_idx = sample_context_row_index(
                train_df,
                predicate=of_fly_with_runner_predicate(),
                label="of_arm_probe",
                seed=1,
            )
            if rf_rows and ctx_idx is not None:
                of_arm_probe_x = build_fielder_slot_probe_inputs(
                    layout=layout,
                    probe_rows=rf_rows,
                    encoded_train=train_x,
                    slot_column="fielder_pos_9",
                    vocab_lookup=vocab_lookup_shared,
                    context_row_index=ctx_idx,
                )
                adv_labels = head_class_labels.get(
                    "r1_advancement", ADVANCEMENT_CLASS_LABELS
                )
                of_arm_probe_cb = build_outfield_arm_probe_callback(
                    probe_rows=rf_rows,
                    probe_x=of_arm_probe_x,
                    advancement_class_labels=adv_labels,
                )
                base_extras = (*base_extras, of_arm_probe_cb)

    use_vicreg = os.environ.get("BC_PRETRAIN_USE_VICREG", "0") in ("1", "true", "TRUE")
    use_siglip = os.environ.get("BC_PRETRAIN_USE_SIGLIP", "0") in ("1", "true", "TRUE")
    use_split_optimizer = os.environ.get(
        "BC_PRETRAIN_USE_SPLIT_OPTIMIZER", "0"
    ) in ("1", "true", "TRUE")
    if use_vicreg:
        vicreg_target_layer_names: tuple[str, ...] = tuple(
            f"embed_{group_name}" for group_name, _ in layout.embedding_groups
        )
    else:
        vicreg_target_layer_names = ()
    siglip_input_col: str | None = None
    siglip_layer_name: str | None = None
    if use_siglip:
        for group_name, group_cols in layout.embedding_groups:
            if "batter_id" in group_cols:
                siglip_input_col = "batter_id"
                siglip_layer_name = f"embed_{group_name}"
                break
        if siglip_input_col is None and "batter_id" in layout.high_card_columns:
            siglip_input_col = "batter_id"
            siglip_layer_name = "embed_batter_id"
    if siglip_input_col is not None:
        _log.info(
            "siglip enabled input_col=%s layer=%s",
            siglip_input_col,
            siglip_layer_name,
        )
    if not use_vicreg:
        _log.info("vicreg disabled (BC_PRETRAIN_USE_VICREG=0)")
    if not use_siglip:
        _log.info("siglip disabled (BC_PRETRAIN_USE_SIGLIP=0)")
    if not use_split_optimizer:
        _log.info("split_optimizer disabled (BC_PRETRAIN_USE_SPLIT_OPTIMIZER=0)")

    fit_start = time.perf_counter()
    merged_hist: dict[str, list[float]] = {}

    def _merge_history(stage_hist: dict[str, list[Any]]) -> None:
        for k, vs in stage_hist.items():
            merged_hist.setdefault(k, []).extend(float(v) for v in vs)

    best_model_path = artifact_dir / "exports" / "model_best.keras"

    if stage1_epochs_eff > 0:
        _log.info(
            "stage1 freeze_trunk epochs=%d steps_per_epoch=%d batch=%d",
            stage1_epochs_eff,
            steps_per_epoch,
            keras_batch_size,
        )
        _set_stage1_trainable(model, head_names)
        s1_trunk_opt, s1_embed_opt = make_pretrain_optimizers(
            total_steps=stage1_epochs_eff * steps_per_epoch,
            trunk_lr=1e-3,
            embed_lr=5e-3,
        )
        model.configure_pretrain(
            trunk_optimizer=s1_trunk_opt,
            embed_optimizer=s1_embed_opt,
            head_losses=head_loss_fns,
            head_metrics=head_metric_objs,
            vicreg_target_layer_names=vicreg_target_layer_names,
            siglip_input_col=siglip_input_col,
            siglip_layer_name=siglip_layer_name,
            vicreg_stride=4,
            siglip_subsample=1024,
        )
        s1_callbacks: list[Any] = list(base_extras)
        if validation_data is not None:
            es_cb = _build_early_stopping_callback(
                patience=early_stopping_patience,
                best_save_path=best_model_path,
            )
            if isinstance(es_cb, list):
                s1_callbacks.extend(es_cb)
            else:
                s1_callbacks.append(es_cb)
        history1 = model.fit(
            train_x,
            train_y,
            sample_weight=train_w,
            epochs=stage1_epochs_eff,
            batch_size=keras_batch_size,
            validation_data=validation_data,
            callbacks=s1_callbacks,
            verbose="2",
        )
        _merge_history(history1.history)

    if stage2_epochs_eff > 0:
        _log.info(
            "stage2 unfreeze epochs=%d steps_per_epoch=%d batch=%d",
            stage2_epochs_eff,
            steps_per_epoch,
            keras_batch_size,
        )
        _set_all_trainable(model)
        if use_split_optimizer:
            s2_trunk_opt, s2_embed_opt = make_pretrain_optimizers(
                total_steps=stage2_epochs_eff * steps_per_epoch,
                trunk_lr=1e-3,
                embed_lr=1.5e-3,
            )
        else:
            s2_trunk_opt, _s2_embed_unused = make_pretrain_optimizers(
                total_steps=stage2_epochs_eff * steps_per_epoch,
                trunk_lr=1e-3,
                embed_lr=1e-3,
            )
            s2_embed_opt = s2_trunk_opt
        model.configure_pretrain(
            trunk_optimizer=s2_trunk_opt,
            embed_optimizer=s2_embed_opt,
            head_losses=head_loss_fns,
            head_metrics=head_metric_objs,
            vicreg_target_layer_names=vicreg_target_layer_names,
            siglip_input_col=siglip_input_col,
            siglip_layer_name=siglip_layer_name,
            vicreg_stride=4,
            siglip_subsample=1024,
        )
        s2_callbacks: list[Any] = list(base_extras)
        if validation_data is not None:
            es_cb = _build_early_stopping_callback(
                patience=early_stopping_patience,
                best_save_path=best_model_path,
            )
            if isinstance(es_cb, list):
                s2_callbacks.extend(es_cb)
            else:
                s2_callbacks.append(es_cb)
        history2 = model.fit(
            train_x,
            train_y,
            sample_weight=train_w,
            epochs=stage2_epochs_eff,
            batch_size=keras_batch_size,
            validation_data=validation_data,
            callbacks=s2_callbacks,
            verbose="2",
        )
        _merge_history(history2.history)

    fit_elapsed = time.perf_counter() - fit_start

    head_val_metrics: dict[str, dict[str, float]] = {}
    hist = merged_hist
    epochs_ran = len(hist.get("loss", []))
    if epochs_ran == 0:
        _log.warning("model.fit produced no epoch history")
    val_losses = hist.get("val_loss", [])
    best_epoch = int(np.argmin(val_losses)) if val_losses else max(0, epochs_ran - 1)
    for head in spec.head_specs:
        metrics: dict[str, float] = {}
        for metric_key, values in hist.items():
            if not metric_key.startswith(f"val_{head.name}"):
                continue
            if not values:
                continue
            metrics[metric_key] = float(values[min(best_epoch, len(values) - 1)])
        head_val_metrics[head.name] = metrics

    _log.info(
        "run_pretrain fit done epochs_ran=%d best_epoch=%d elapsed=%.1fs",
        epochs_ran,
        best_epoch,
        fit_elapsed,
    )
    for head_name, metrics in head_val_metrics.items():
        _log.info("pretext head=%s val_metrics=%s", head_name, metrics)

    paths = write_pretrain_artifact(
        spec=spec,
        artifact_id=artifact_id,
        artifact_dir=artifact_dir,
        model=model,
        layout=layout,
        vocabularies=vocabularies,
        head_specs=spec.head_specs,
        head_class_labels=head_class_labels,
        head_val_metrics=head_val_metrics,
        source_snapshot_id=source_snapshot_id,
        dataset_artifact_id=dataset_artifact_id,
        train_rows=train_rows,
        validate_rows=int(validate_df.height),
        test_rows=int(test_df.height),
    )

    return PretrainRunResult(
        artifact_dir=artifact_dir,
        manifest_path=paths["manifest"],
        train_rows=train_rows,
        validate_rows=int(validate_df.height),
        test_rows=int(test_df.height),
        head_val_metrics=head_val_metrics,
    )
