"""Emit per-head pre-softmax logits from a stage-1 pretrain artifact.

For Phase-3 v7 residual decomposition. Loads a saved stage-1 Keras model
(``artifacts/statistical/deep/<spec>/<artifact_id>/exports/model.keras``),
builds an inference-only model whose outputs are the per-head
``{head}_logits`` intermediate Dense layers, streams the dataset parquet
through ``.predict()``, and writes one parquet per head under
``<artifact_dir>/offsets/<head>.parquet`` with columns ``event_key`` +
``logit_0`` ... ``logit_{K-1}`` as float16.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from typing import Any, cast

import numpy as np
from numpy.typing import NDArray
import polars as pl

import python_models.statistical.deep  # noqa: F401  # sets KERAS_BACKEND=torch
import python_models.ml.model_factory  # noqa: F401  # registers custom layers

from python_models.statistical.config import DATASETS_ROOT, DEEP_ROOT
from python_models.statistical.deep.io import (
    load_dataset_parquet,
    partition_by_split,
)
from python_models.statistical.deep.pretrain.targets import (
    get_pretrain_layout,
    get_pretrain_spec,
)
from python_models.statistical.deep.pretrain.training import (
    _apply_all_remaps,
    _collect_input_stats,
    _encode_inputs,
)

_log = logging.getLogger(__name__)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="pretrain_emit_offsets",
        description=__doc__,
    )
    _ = parser.add_argument("artifact_id")
    _ = parser.add_argument(
        "--pretrain-target",
        default="event_universe_context",
        help="Stage-1 pretrain spec name (default: event_universe_context).",
    )
    _ = parser.add_argument(
        "--dataset-artifact",
        required=True,
        help="Dataset artifact id under DATASETS_ROOT/<dataset_name>/.",
    )
    _ = parser.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override dataset artifact root.",
    )
    _ = parser.add_argument(
        "--output-root",
        default=None,
        help="Override pretrain artifact root.",
    )
    _ = parser.add_argument(
        "--predict-batch-size",
        type=int,
        default=4096,
        help="Keras predict batch size (default 4096).",
    )
    _ = parser.add_argument(
        "--chunk-size",
        type=int,
        default=500_000,
        help="Rows per polars slice before encoding (default 500K).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)
    import keras

    spec = get_pretrain_spec(str(args.pretrain_target))
    layout = get_pretrain_layout(str(args.pretrain_target))

    artifact_root = Path(args.output_root) if args.output_root else DEEP_ROOT
    artifact_dir = artifact_root / spec.name / str(args.artifact_id)
    if not artifact_dir.exists():
        raise FileNotFoundError(f"stage-1 artifact dir missing: {artifact_dir}")

    dataset_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_parquet = (
        dataset_root / spec.dataset_name / str(args.dataset_artifact) / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        raise FileNotFoundError(f"dataset parquet missing: {dataset_parquet}")

    manifest_path = artifact_dir / "manifest.json"
    manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    head_meta = json.loads(manifest_payload["metadata"]["head_specs_json"])
    head_classes: dict[str, int] = {
        str(h["name"]): int(h["num_classes"]) for h in head_meta
    }
    head_names: list[str] = list(head_classes.keys())

    model_path = artifact_dir / "exports" / "model.keras"
    if not model_path.exists():
        alt = artifact_dir / "exports" / "model_best.keras"
        if not alt.exists():
            raise FileNotFoundError(
                f"no saved stage-1 model at {model_path} or {alt}"
            )
        model_path = alt
    _log.info("loading stage-1 model from %s", model_path)
    full_model = cast(Any, keras.models.load_model(str(model_path), compile=False))

    logits_outputs = {
        h: full_model.get_layer(f"{h}_logits").output for h in head_names
    }
    intermediate = cast(
        Any, keras.Model(inputs=full_model.inputs, outputs=logits_outputs)
    )

    _log.info("loading dataset %s", dataset_parquet)
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
            _log.info(
                "BC_PRETRAIN_DATASET_LIMIT=%d sub-sampled to %d rows", limit, int(df.height)
            )

    train_df, _, _ = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )
    if train_df.height == 0:
        raise ValueError("TRAIN partition is empty; cannot rebuild vocabularies")
    vocabularies, _, _ = _collect_input_stats(train_df, layout=layout)

    n_rows = int(df.height)
    _log.info(
        "predict head_count=%d rows=%d chunk=%d predict_batch=%d",
        len(head_names),
        n_rows,
        int(args.chunk_size),
        int(args.predict_batch_size),
    )

    head_arrays: dict[str, list[NDArray[np.float16]]] = {h: [] for h in head_names}
    event_key_chunks: list[NDArray[Any]] = []
    start = 0
    while start < n_rows:
        end = min(n_rows, start + int(args.chunk_size))
        chunk = df.slice(start, end - start)
        x = _encode_inputs(chunk, layout=layout, vocabularies=vocabularies)
        preds = intermediate.predict(
            x, batch_size=int(args.predict_batch_size), verbose="0"
        )
        for h in head_names:
            arr = np.asarray(preds[h], dtype=np.float32)
            if np.isnan(arr).any() or np.isinf(arr).any():
                raise ValueError(
                    f"head {h!r} chunk={start}:{end} produced NaN/Inf logits"
                )
            head_arrays[h].append(arr.astype(np.float16, copy=False))
        event_key_chunks.append(chunk["event_key"].to_numpy())
        _log.info("predicted rows=%d/%d", end, n_rows)
        start = end

    event_keys = np.concatenate(event_key_chunks)
    offsets_dir = artifact_dir / "offsets"
    offsets_dir.mkdir(parents=True, exist_ok=True)

    for h in head_names:
        arr = np.concatenate(head_arrays[h], axis=0)
        if arr.shape[0] != event_keys.shape[0]:
            raise ValueError(
                f"row-count mismatch for head {h!r}: arr={arr.shape[0]} events={event_keys.shape[0]}"
            )
        num_classes = head_classes[h]
        cols: dict[str, NDArray[Any]] = {"event_key": event_keys}
        for i in range(num_classes):
            cols[f"logit_{i}"] = arr[:, i]
        frame = pl.DataFrame(cols)
        out_path = offsets_dir / f"{h}.parquet"
        tmp = offsets_dir / f".{h}.parquet.tmp"
        frame.write_parquet(str(tmp), compression="zstd")
        os.replace(tmp, out_path)
        _log.info(
            "wrote offset parquet head=%s rows=%d classes=%d path=%s",
            h,
            int(frame.height),
            num_classes,
            out_path,
        )

    return 0


if __name__ == "__main__":
    sys.exit(main())
