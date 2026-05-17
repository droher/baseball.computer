"""Run fielding + outfield-arm probes against a saved pretrain artifact.

Usage::

    BC_DB_PATH=$(pwd)/bc_dev.db PYTHONPATH=$(pwd)/bc uv run --group ml \\
        python scripts/fielding_probe_pretrain.py <artifact_id>

For each artifact the script:

1. Loads ``artifacts/statistical/deep/event_universe/<artifact_id>/exports/
   model_best.keras`` (falling back to ``model.keras``).
2. Reads the dataset parquet referenced in the artifact manifest.
3. Applies pretrain target remaps, partitions to TRAIN, and rebuilds
   the vocabularies the way ``run_pretrain`` does.
4. Samples a real ``trajectory_remapped='GroundBall'`` → SS context and
   a real OF-fly-with-runner context.
5. Sweeps SS (``fielder_pos_6``) and RF (``fielder_pos_9``) through the
   canonical rosters, predicting ``hit_or_out`` and ``r1_advancement``
   respectively, and prints per-tier rows.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any, cast

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np


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
        help="Override dataset_artifact_id (defaults to manifest value)",
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
    sys.path.insert(0, str(repo_root / "bc"))

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
    from python_models.statistical.deep.pretrain.heads import apply_target_remap
    from python_models.statistical.deep.pretrain.probes import (
        build_fielder_slot_probe_inputs,
        grounder_to_ss_predicate,
        of_fly_with_runner_predicate,
        resolve_right_fielder_ids,
        resolve_shortstop_ids,
        sample_context_row_index,
    )
    from python_models.statistical.deep.pretrain.targets import (
        ADVANCEMENT_CLASS_LABELS,
        EVENT_UNIVERSE_LAYOUT,
        EVENT_UNIVERSE_SPEC,
    )
    from python_models.statistical.deep.pretrain.training import (
        _apply_all_remaps,
        _collect_input_stats,
        _encode_inputs,
    )

    layout = EVENT_UNIVERSE_LAYOUT
    spec = EVENT_UNIVERSE_SPEC

    datasets_root = (
        Path(args.dataset_output_root)
        if args.dataset_output_root
        else DATASETS_ROOT
    )
    dataset_parquet = (
        datasets_root / spec.dataset_name / dataset_artifact / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        sys.stderr.write(f"ERROR: dataset parquet missing at {dataset_parquet}\n")
        return 2

    log = logging.getLogger("fielding_probe")
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
    if train_df.height == 0:
        sys.stderr.write("ERROR: empty TRAIN partition\n")
        return 3

    log.info("building vocabularies (train rows=%d)", train_df.height)
    vocabularies, _means, _vars = _collect_input_stats(train_df, layout=layout)
    log.info("encoding train inputs")
    train_x = _encode_inputs(train_df, layout=layout, vocabularies=vocabularies)
    vocab_lookup: dict[str, dict[str, int]] = {
        name: {val: i + 1 for i, val in enumerate(vocab.values)}
        for name, vocab in vocabularies.items()
    }

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

    ss_rows = resolve_shortstop_ids(args.db_path)
    rf_rows = resolve_right_fielder_ids(args.db_path)

    if ss_rows:
        ctx_idx = sample_context_row_index(
            train_df,
            predicate=grounder_to_ss_predicate(),
            label="fielding_probe",
            seed=0,
        )
        if ctx_idx is None:
            log.warning("fielding_probe: no GroundBall→SS context found")
        else:
            probe_x = build_fielder_slot_probe_inputs(
                layout=layout,
                probe_rows=ss_rows,
                encoded_train=train_x,
                slot_column="fielder_pos_6",
                vocab_lookup=vocab_lookup,
                context_row_index=ctx_idx,
            )
            preds = model.predict(probe_x, verbose=0)
            head = preds.get("hit_or_out") if isinstance(preds, dict) else None
            if head is None:
                log.warning("fielding_probe: hit_or_out head missing from preds")
            else:
                arr = np.asarray(head, dtype=np.float64).reshape(len(ss_rows), -1)
                p_hit = arr[:, 0] if arr.shape[1] >= 1 else np.zeros(len(ss_rows))
                print(f"artifact={args.artifact_id} probe=fielding slot=ss n={len(ss_rows)}")
                print(
                    f"{'tier':<11s} {'player':<25s} {'pid':<10s}  P(out)  P(hit)"
                )
                for i, r in enumerate(ss_rows):
                    print(
                        f"{r.tier:<11s} {r.player_label:<25s} {r.player_id:<10s} "
                        f"{1.0 - float(p_hit[i]):.3f}   {float(p_hit[i]):.3f}"
                    )

    if rf_rows:
        ctx_idx = sample_context_row_index(
            train_df,
            predicate=of_fly_with_runner_predicate(),
            label="of_arm_probe",
            seed=1,
        )
        if ctx_idx is None:
            log.warning("of_arm_probe: no OF-fly-with-runner context found")
        else:
            probe_x = build_fielder_slot_probe_inputs(
                layout=layout,
                probe_rows=rf_rows,
                encoded_train=train_x,
                slot_column="fielder_pos_9",
                vocab_lookup=vocab_lookup,
                context_row_index=ctx_idx,
            )
            preds = model.predict(probe_x, verbose=0)
            head = preds.get("r1_advancement") if isinstance(preds, dict) else None
            if head is None:
                log.warning("of_arm_probe: r1_advancement head missing from preds")
            else:
                arr = np.asarray(head, dtype=np.float64)
                if arr.ndim == 3 and arr.shape[1] == 1:
                    arr = arr[:, 0, :]
                labels = ADVANCEMENT_CLASS_LABELS
                try:
                    out_idx = labels.index("OutAdvancing")
                    scored_idx = labels.index("Scored")
                    adv1_idx = labels.index("Advanced1")
                except ValueError:
                    sys.stderr.write(
                        "ERROR: advancement class labels missing expected entries\n"
                    )
                    return 4
                print(
                    f"artifact={args.artifact_id} probe=of_arm slot=rf n={len(rf_rows)}"
                )
                print(
                    f"{'tier':<11s} {'player':<25s} {'pid':<10s}  P(OutAdv) P(Scored) P(Adv1)"
                )
                for i, r in enumerate(rf_rows):
                    p = arr[i]
                    print(
                        f"{r.tier:<11s} {r.player_label:<25s} {r.player_id:<10s} "
                        f"{float(p[out_idx]):.3f}     {float(p[scored_idx]):.3f}     {float(p[adv1_idx]):.3f}"
                    )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
