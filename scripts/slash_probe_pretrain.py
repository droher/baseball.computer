"""Run the slash-line probe against a saved pretrain artifact.

Usage:
    BC_DB_PATH=$(pwd)/bc_dev.db PYTHONPATH=$(pwd)/bc uv run --group ml \\
        python scripts/slash_probe_pretrain.py <artifact_id>

Loads ``artifacts/statistical/deep/event_universe/<artifact_id>/exports/model.keras``,
resolves the canonical roster via DuckDB, derives implied AVG / OBP / SLG from
the ``pa_result`` softmax, and prints per-tier rows.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, cast

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np


def _build_probe_x(
    layout: Any,
    train_x: dict[str, np.ndarray],
    probe_rows: tuple[Any, ...],
    vocab_lookup: dict[str, dict[str, int]],
) -> dict[str, np.ndarray]:
    from python_models.statistical.deep.pretrain.probes import build_probe_inputs

    return build_probe_inputs(
        layout=layout,
        spec=None,  # type: ignore[arg-type]
        probe_rows=probe_rows,
        encoded_train=train_x,
        vocab_lookup=vocab_lookup,
    )


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("artifact_id")
    parser.add_argument(
        "--db-path",
        default=os.environ.get("BC_DB_PATH"),
        help="DuckDB path (defaults to BC_DB_PATH env var)",
    )
    parser.add_argument(
        "--dataset-artifact",
        default=None,
        help="Dataset artifact id; overrides pretrain manifest value",
    )
    parser.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override DATASETS_ROOT",
    )
    args = parser.parse_args()

    if not args.db_path:
        sys.stderr.write("ERROR: --db-path or BC_DB_PATH required\n")
        return 1

    repo_root = Path(__file__).resolve().parent.parent
    artifact_dir = (
        repo_root
        / "artifacts"
        / "statistical"
        / "deep"
        / "event_universe"
        / args.artifact_id
    )
    best_path = artifact_dir / "exports" / "model_best.keras"
    final_path = artifact_dir / "exports" / "model.keras"
    if best_path.exists():
        model_path = best_path
    elif final_path.exists():
        model_path = final_path
    else:
        sys.stderr.write(
            f"ERROR: neither model_best.keras nor model.keras found in "
            f"{artifact_dir / 'exports'}\n"
        )
        return 2

    import json

    manifest_path = artifact_dir / "manifest.json"
    manifest_dataset_artifact: str | None = None
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        raw = manifest.get("dataset_artifact_id")
        if raw is not None:
            manifest_dataset_artifact = str(raw)
    dataset_artifact = args.dataset_artifact or manifest_dataset_artifact
    if not dataset_artifact:
        sys.stderr.write(
            "ERROR: manifest missing or dataset_artifact_id unset; pass --dataset-artifact\n"
        )
        return 2

    import keras

    sys.path.insert(0, str(repo_root / "bc"))
    from python_models.ml.features import Vocabulary  # noqa: F401  # register
    from python_models.ml.model_factory import (  # noqa: F401  # register
        CrossLayer,
        PretrainModel,
        RoleBias,
    )
    from python_models.statistical.config import DATASETS_ROOT
    from python_models.statistical.deep.io import (
        load_dataset_parquet,
        partition_by_split,
    )
    from python_models.statistical.deep.pretrain.probes import (
        build_pa_flags,
        build_probe_inputs,
        neutral_pa_context_predicate,
        resolve_canonical_ids,
        sample_context_row_index,
    )
    from python_models.statistical.deep.pretrain.targets import (
        EVENT_UNIVERSE_LAYOUT,
        EVENT_UNIVERSE_SPEC,
        PA_RESULT_CLASS_LABELS,
    )
    from python_models.statistical.deep.pretrain.training import (
        _apply_all_remaps,
        _collect_input_stats,
        _encode_inputs,
    )

    layout = EVENT_UNIVERSE_LAYOUT
    spec = EVENT_UNIVERSE_SPEC

    seed_path = (
        repo_root / "bc" / "seeds" / "misc" / "seed_plate_appearance_result_types.csv"
    )
    import csv

    with seed_path.open(encoding="utf-8") as fh:
        seed_rows = [dict(row) for row in csv.DictReader(fh)]
    pa_flags = build_pa_flags(PA_RESULT_CLASS_LABELS, seed_rows)

    probe_rows = resolve_canonical_ids(args.db_path)
    if not probe_rows:
        sys.stderr.write("ERROR: zero probe rows resolved\n")
        return 3

    datasets_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_parquet = (
        datasets_root / spec.dataset_name / dataset_artifact / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        sys.stderr.write(f"ERROR: dataset parquet missing at {dataset_parquet}\n")
        return 2

    log = logging.getLogger("slash_probe")
    log.info("loading dataset %s", dataset_parquet)
    df = load_dataset_parquet(str(dataset_parquet))
    df = _apply_all_remaps(df, spec.head_specs)
    train_df, _val_df, _test_df = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )
    log.info("building vocabularies (train rows=%d)", train_df.height)
    vocabularies, _means, _vars = _collect_input_stats(train_df, layout=layout)
    log.info("encoding train inputs")
    train_x = _encode_inputs(train_df, layout=layout, vocabularies=vocabularies)
    vocab_lookup: dict[str, dict[str, int]] = {
        name: {val: i + 1 for i, val in enumerate(vocab.values)}
        for name, vocab in vocabularies.items()
    }

    ctx_idx = sample_context_row_index(
        train_df,
        predicate=neutral_pa_context_predicate(),
        label="slash_probe",
        seed=0,
    )
    if ctx_idx is None:
        sys.stderr.write("ERROR: no neutral PA context matched\n")
        return 4

    n = len(probe_rows)
    probe_x = build_probe_inputs(
        layout=layout,
        spec=spec,
        probe_rows=probe_rows,
        encoded_train=train_x,
        vocab_lookup=vocab_lookup,
        context_row_index=ctx_idx,
    )

    log.info("loading model %s", model_path)
    model = cast(Any, keras.models.load_model(
        str(model_path),
        custom_objects={
            "CrossLayer": CrossLayer,
            "PretrainModel": PretrainModel,
            "RoleBias": RoleBias,
        },
        compile=False,
    ))
    preds = model.predict(probe_x, verbose=0)
    pa_pred = preds["pa_result"] if isinstance(preds, dict) else preds
    pa_pred = np.asarray(pa_pred, dtype=np.float64)
    if pa_pred.ndim == 3 and pa_pred.shape[1] == 1:
        pa_pred = pa_pred[:, 0, :]

    eps = 1e-9
    hits = pa_pred @ pa_flags.is_hit.astype(np.float64)
    at_bats = pa_pred @ pa_flags.is_at_bat.astype(np.float64)
    on_base = pa_pred @ pa_flags.is_on_base_success.astype(np.float64)
    on_base_opp = pa_pred @ pa_flags.is_on_base_opportunity.astype(np.float64)
    total_bases = pa_pred @ pa_flags.total_bases.astype(np.float64)
    avg = hits / np.maximum(at_bats, eps)
    obp = on_base / np.maximum(on_base_opp, eps)
    slg = total_bases / np.maximum(at_bats, eps)

    print(f"artifact={args.artifact_id} n={n}")
    print(f"{'tier':<11s} {'side':<7s} {'player':<25s} {'pid':<10s}  AVG    OBP    SLG")
    for i, r in enumerate(probe_rows):
        print(
            f"{r.tier:<11s} {r.side:<7s} {r.player_label:<25s} {r.player_id:<10s} "
            f"{avg[i]:.3f}  {obp[i]:.3f}  {slg[i]:.3f}"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
