"""Pretrain artifact writer: embeddings.parquet + vocab.json + pretrain_manifest.json."""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
from numpy.typing import NDArray

from python_models.ml.features import FeatureLayout, Vocabulary
from python_models.statistical.deep.embeddings import (
    assemble_embeddings_frame,
    vocabulary_entity_ids,
)
from python_models.statistical.deep.pretrain.spec import HeadSpec, PretrainSpec
from python_models.statistical.manifests import (
    package_versions,
    query_hash,
    write_manifest,
)
from python_models.statistical.schemas import ArtifactManifest

_log = logging.getLogger(__name__)


def pretrain_artifact_dir(name: str, artifact_id: str, *, root: Path) -> Path:
    return root / name / artifact_id


def _atomic_write_text(path: Path, payload: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            _ = fh.write(payload)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def extract_embedding_matrices(
    model: Any,
    embedding_units: tuple[str, ...],
) -> dict[str, NDArray[np.float64]]:
    out: dict[str, NDArray[np.float64]] = {}
    for unit in embedding_units:
        layer = model.get_layer(f"embed_{unit}")
        weights = layer.get_weights()
        if not weights:
            raise RuntimeError(f"embed_{unit} has no weights after training")
        out[unit] = np.asarray(weights[0], dtype=np.float64)
    return out


def extract_role_bias_diagnostics(
    model: Any,
    layout: FeatureLayout,
    matrices: dict[str, NDArray[np.float64]],
) -> dict[str, Any]:
    """For each grouped column, pull the matching ``role_<col>`` layer bias.

    Reports per-slot L2 norm, cosine vs batter (when present), and a
    summary flag indicating whether RoleBias is operating below a useful
    threshold (max L2 < 5% of mean embedding row norm in its group).
    """
    grouped = layout.grouped_columns()
    slots: dict[str, dict[str, Any]] = {}
    norms_by_group: dict[str, list[tuple[str, NDArray[np.float64]]]] = {}
    for col in grouped:
        unit = layout.embedding_unit_for_column(col)
        layer_name = f"role_{col}"
        try:
            layer = model.get_layer(layer_name)
        except ValueError:
            continue
        weights = layer.get_weights()
        if not weights:
            continue
        bias = np.asarray(weights[0], dtype=np.float64).reshape(-1)
        l2 = float(np.linalg.norm(bias))
        slots[col] = {"l2": l2, "embedding_unit": unit}
        norms_by_group.setdefault(unit, []).append((col, bias))

    for unit, entries in norms_by_group.items():
        batter_bias: NDArray[np.float64] | None = None
        for col, bias in entries:
            if col == "batter_id":
                batter_bias = bias
                break
        if batter_bias is None:
            continue
        denom_b = float(np.linalg.norm(batter_bias))
        if denom_b < 1e-12:
            continue
        for col, bias in entries:
            denom_a = float(np.linalg.norm(bias))
            if denom_a < 1e-12:
                slots[col]["cos_vs_batter"] = float("nan")
                continue
            slots[col]["cos_vs_batter"] = float(
                np.dot(bias, batter_bias) / (denom_a * denom_b)
            )

    flags: dict[str, dict[str, Any]] = {}
    for unit, entries in norms_by_group.items():
        max_bias_l2 = max((float(np.linalg.norm(b)) for _, b in entries), default=0.0)
        mat = matrices.get(unit)
        mean_row_norm = (
            float(np.mean(np.linalg.norm(mat, axis=1))) if mat is not None else 0.0
        )
        ratio = max_bias_l2 / mean_row_norm if mean_row_norm > 1e-12 else 0.0
        flags[unit] = {
            "max_bias_l2": max_bias_l2,
            "mean_embed_row_norm": mean_row_norm,
            "bias_to_embed_ratio": ratio,
            "inert": bool(ratio < 0.05),
        }
        if ratio < 0.05:
            _log.warning(
                "role_bias_diagnostic: unit=%s appears inert (max_bias_l2=%.4f vs mean_row_norm=%.4f, ratio=%.4f). Consider dropping RoleBias.",
                unit,
                max_bias_l2,
                mean_row_norm,
                ratio,
            )
    return {"slots": slots, "groups": flags}


def write_pretrain_artifact(
    *,
    spec: PretrainSpec,
    artifact_id: str,
    artifact_dir: Path,
    model: Any,
    layout: FeatureLayout,
    vocabularies: dict[str, Vocabulary],
    head_specs: tuple[HeadSpec, ...],
    head_class_labels: dict[str, tuple[str, ...]],
    head_val_metrics: dict[str, dict[str, float]],
    source_snapshot_id: str,
    dataset_artifact_id: str | None,
    train_rows: int,
    validate_rows: int,
    test_rows: int,
) -> dict[str, Path]:
    exports_dir = artifact_dir / "exports"
    exports_dir.mkdir(parents=True, exist_ok=True)

    embedding_units = layout.embedding_unit_names()
    matrices = extract_embedding_matrices(model, embedding_units)
    unit_vocabs = {unit: vocabularies[unit] for unit in embedding_units}
    embeddings_df = assemble_embeddings_frame(
        layout=layout,
        embedding_matrices=matrices,
        vocabularies=unit_vocabs,
    )
    embeddings_path = exports_dir / "embeddings.parquet"
    fd, tmp_name = tempfile.mkstemp(
        dir=str(exports_dir), prefix=".embeddings.", suffix=".parquet"
    )
    try:
        os.close(fd)
        embeddings_df.write_parquet(tmp_name, compression="zstd")
        os.replace(tmp_name, embeddings_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise

    model_path = exports_dir / "model.keras"
    try:
        model.save(str(model_path))
    except Exception as exc:
        _log.warning("pretrain model.save failed (continuing without): %s", exc)
        model_path = None  # type: ignore[assignment]

    try:
        role_bias_diag = extract_role_bias_diagnostics(model, layout, matrices)
        role_bias_path = exports_dir / "role_bias_norms.json"
        _atomic_write_text(role_bias_path, json.dumps(role_bias_diag, indent=2))
    except Exception as exc:
        _log.warning("role_bias_diagnostic failed (continuing): %s", exc)
        role_bias_path = None  # type: ignore[assignment]

    vocab_payload: dict[str, list[str]] = {}
    for unit in embedding_units:
        vocab_payload[unit] = vocabulary_entity_ids(unit_vocabs[unit])
    vocab_path = exports_dir / "vocab.json"
    _atomic_write_text(vocab_path, json.dumps(vocab_payload, indent=2))

    head_metadata: list[dict[str, Any]] = []
    for head in head_specs:
        labels = head_class_labels.get(head.name, ())
        metrics = head_val_metrics.get(head.name, {})
        head_metadata.append(
            {
                "name": head.name,
                "target_column": head.target_column,
                "kind": head.kind,
                "num_classes": len(labels) if head.kind == "multiclass" else 1,
                "class_labels": list(labels),
                "loss_weight": head.loss_weight,
                "val_metrics": metrics,
            }
        )
    metadata: dict[str, str | int | float | bool] = {
        "train_rows": train_rows,
        "validate_rows": validate_rows,
        "test_rows": test_rows,
        "high_card_columns_json": json.dumps(list(layout.high_card_columns)),
        "low_card_columns_json": json.dumps(list(layout.low_card_columns)),
        "numeric_columns_json": json.dumps(list(layout.numeric_columns)),
        "embedding_groups_json": json.dumps(
            [[name, list(cols)] for name, cols in layout.embedding_groups]
        ),
        "head_specs_json": json.dumps(head_metadata),
        "vocab_sizes_json": json.dumps(
            {unit: len(vocabulary_entity_ids(unit_vocabs[unit])) for unit in embedding_units}
        ),
    }
    manifest = ArtifactManifest(
        artifact_id=artifact_id,
        kind="pretrain",
        name=spec.name,
        version="0.1.0",
        created_at=datetime.now(tz=timezone.utc),
        source_snapshot_id=source_snapshot_id,
        query_hash=query_hash(
            f"pretrain|{spec.name}|{spec.dataset_name}|"
            + "|".join(sorted(h.name for h in head_specs))
        ),
        dataset_artifact_id=dataset_artifact_id,
        output_paths={
            "artifact_dir": artifact_dir,
            "embeddings": embeddings_path,
            "vocab": vocab_path,
            **({"model": model_path} if model_path is not None else {}),
        },
        package_versions=package_versions(),
        metadata=metadata,
    )
    manifest_path = artifact_dir / "manifest.json"
    write_manifest(manifest, manifest_path)
    paths = {
        "artifact_dir": artifact_dir,
        "embeddings": embeddings_path,
        "vocab": vocab_path,
        "manifest": manifest_path,
    }
    if model_path is not None:
        paths["model"] = model_path
    if role_bias_path is not None:
        paths["role_bias"] = role_bias_path
    return paths
