"""Full evaluation suite against a saved pretrain artifact.

Computes the four v5 quality metrics defined in
``notes/data-coverage-implementation/04-deep-learning-supplements.md``:

- M1 ``downstream_proxy_score`` — frozen-embedding linear probe on a
  time-forward-fold held-out slice.
- M2 ``confound_leak_auc`` — multiclass AUC of LR predicting era /
  league from the player embedding matrix.
- M3 ``mc_marginal_slash`` — Monte-Carlo averaged AVG/OBP/SLG for the
  panel rosters across stratified random PA contexts.
- M4 ``embed_health`` — per-layer eff_rank / cosine / variance.

Writes ``<artifact_dir>/eval/eval_report.json`` atomically.

Usage:
    BC_DB_PATH=$(pwd)/bc_dev.db PYTHONPATH=$(pwd)/bc \\
        uv run --group ml python scripts/pretrain_eval_pretrain.py <artifact_id>
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path
from typing import Any, cast

os.environ.setdefault("KERAS_BACKEND", "torch")

import numpy as np


_LOG = logging.getLogger("pretrain_eval")

_DEFAULT_MC_CONTEXTS = 500
_DEFAULT_PROBE_TRAIN_ROWS = 80_000
_DEFAULT_PROBE_VAL_ROWS = 20_000


def _atomic_write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=f".{path.name}.", suffix=".tmp"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2, default=str)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def _resolve_artifact_paths(repo_root: Path, artifact_id: str) -> tuple[Path, Path]:
    deep_root = repo_root / "artifacts" / "statistical" / "deep"
    matches = sorted(deep_root.rglob(f"{artifact_id}/manifest.json"))
    if not matches:
        raise FileNotFoundError(
            f"No artifact found with id={artifact_id!r} under {deep_root}"
        )
    artifact_dir = matches[0].parent
    best = artifact_dir / "exports" / "model_best.keras"
    final = artifact_dir / "exports" / "model.keras"
    if best.exists():
        return artifact_dir, best
    if final.exists():
        return artifact_dir, final
    raise FileNotFoundError(
        f"No model_best.keras or model.keras under {artifact_dir / 'exports'}"
    )


def _confound_labels_for_players(
    dataset_parquet: Path,
    player_entity_ids: list[str],
) -> dict[str, list[int]]:
    """Resolve era_decade and primary_league per player_id from the dataset parquet.

    Returns label codes: -1 = unknown. Era buckets: (season // 10) * 10.
    Primary league: most-frequent appearance league (AL, NL); others = -1.
    Avoids DuckDB attach so the sidecar runs while a long-running SQLMesh
    plan or fit holds the lock.
    """
    import duckdb

    parquet_str = str(dataset_parquet).replace("'", "''")
    sql = f"""
        WITH appearances AS (
            SELECT
                batter_id AS player_id,
                season,
                CASE WHEN league IN ('AL', 'NL') THEN league ELSE NULL END AS league_norm
            FROM read_parquet('{parquet_str}')
            WHERE batter_id IS NOT NULL
            UNION ALL
            SELECT
                pitcher_id AS player_id,
                season,
                CASE WHEN league IN ('AL', 'NL') THEN league ELSE NULL END AS league_norm
            FROM read_parquet('{parquet_str}')
            WHERE pitcher_id IS NOT NULL
        ),
        era_per_player AS (
            SELECT
                player_id,
                CAST(MEDIAN(season) AS INTEGER) AS median_season
            FROM appearances
            GROUP BY player_id
        ),
        league_per_player AS (
            SELECT player_id, league_norm, COUNT(*) AS n
            FROM appearances
            WHERE league_norm IS NOT NULL
            GROUP BY player_id, league_norm
            QUALIFY ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY n DESC) = 1
        )
        SELECT
            e.player_id,
            e.median_season,
            COALESCE(l.league_norm, '') AS primary_league
        FROM era_per_player AS e
        LEFT JOIN league_per_player AS l USING (player_id)
    """
    con = duckdb.connect(":memory:")
    try:
        rows = con.execute(sql).fetchall()
    finally:
        con.close()

    by_pid: dict[str, tuple[int, str]] = {
        str(r[0]): (int(r[1]), str(r[2])) for r in rows
    }
    decades: list[int] = []
    leagues: list[int] = []
    dh_regimes: list[int] = []
    league_code = {"AL": 0, "NL": 1}
    seen_decades: dict[int, int] = {}
    seen_dh: dict[str, int] = {}
    for pid in player_entity_ids:
        rec = by_pid.get(pid)
        if rec is None or rec[0] == 0:
            decades.append(-1)
            leagues.append(-1)
            dh_regimes.append(-1)
            continue
        season, league = rec
        decade_val = (season // 10) * 10
        if decade_val not in seen_decades:
            seen_decades[decade_val] = len(seen_decades)
        decades.append(seen_decades[decade_val])
        leagues.append(league_code.get(league, -1))
        if season <= 1972:
            dh_key = "pre_dh"
        elif season >= 2022:
            dh_key = "universal_dh"
        elif league == "AL":
            dh_key = "al_dh"
        elif league == "NL":
            dh_key = "nl_no_dh"
        else:
            dh_key = "other"
        if dh_key not in seen_dh:
            seen_dh[dh_key] = len(seen_dh)
        dh_regimes.append(seen_dh[dh_key])
    return {
        "era_decade": decades,
        "primary_league": leagues,
        "dh_regime": dh_regimes,
    }


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
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
    parser.add_argument(
        "--n-contexts", type=int, default=_DEFAULT_MC_CONTEXTS,
        help="Monte Carlo probe contexts (default 500)",
    )
    parser.add_argument(
        "--probe-train-rows", type=int, default=_DEFAULT_PROBE_TRAIN_ROWS,
        help="Linear-probe TRAIN sample size",
    )
    parser.add_argument(
        "--probe-val-rows", type=int, default=_DEFAULT_PROBE_VAL_ROWS,
        help="Linear-probe VALIDATE sample size",
    )
    parser.add_argument(
        "--skip-mc-probe", action="store_true",
        help="Skip MC slash probe (saves ~30s on huge artifacts)",
    )
    parser.add_argument(
        "--skip-linear-probe", action="store_true",
        help="Skip downstream linear-probe proxy",
    )
    parser.add_argument(
        "--skip-confound-probe", action="store_true",
        help="Skip confound-leak AUC probe",
    )
    args = parser.parse_args()
    if not args.db_path:
        sys.stderr.write("ERROR: --db-path or BC_DB_PATH required\n")
        return 1

    repo_root = Path(__file__).resolve().parent.parent
    artifact_dir, model_path = _resolve_artifact_paths(repo_root, args.artifact_id)
    eval_dir = artifact_dir / "eval"
    eval_dir.mkdir(parents=True, exist_ok=True)

    sys.path.insert(0, str(repo_root / "bc"))

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

    import keras  # noqa: F401  # ensure backend pins to torch
    import polars as pl

    from python_models.ml.features import Vocabulary  # noqa: F401  # register
    from python_models.ml.model_factory import (
        CrossLayer,
        PretrainModel,
        RoleBias,
    )
    from python_models.statistical.config import DATASETS_ROOT
    from python_models.statistical.deep.io import (
        load_dataset_parquet,
        partition_by_split,
    )
    from python_models.statistical.deep.leakage_probes import confound_probe
    from python_models.statistical.deep.pretrain.artifacts import (
        extract_role_bias_diagnostics,
    )
    from python_models.statistical.deep.pretrain.probes import (
        LinearProbeTaskSpec,
        build_linear_probe_callback,
        build_mc_probe_inputs,
        build_mc_slash_probe_callback,
        build_pa_flags,
        pad_offset_inputs,
        resolve_canonical_ids,
        sample_mc_context_indices,
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
        _load_pa_seed_rows,
    )

    layout = EVENT_UNIVERSE_LAYOUT
    spec = EVENT_UNIVERSE_SPEC

    _LOG.info("loading model %s", model_path)
    model = cast(Any, keras.models.load_model(
        str(model_path),
        custom_objects={
            "CrossLayer": CrossLayer,
            "PretrainModel": PretrainModel,
            "RoleBias": RoleBias,
        },
        compile=False,
    ))

    datasets_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_parquet = (
        datasets_root / spec.dataset_name / dataset_artifact / "dataset.parquet"
    )
    if not dataset_parquet.exists():
        sys.stderr.write(f"ERROR: dataset parquet missing at {dataset_parquet}\n")
        return 2

    _LOG.info("loading dataset %s", dataset_parquet)
    df = load_dataset_parquet(str(dataset_parquet))
    df = _apply_all_remaps(df, spec.head_specs)

    train_df, _val_df, _test_df = partition_by_split(
        df,
        split_column=spec.split_column,
        train_label=spec.train_label,
        validate_label=spec.validate_label,
        test_label=spec.test_label,
    )

    _LOG.info("building vocabularies (train rows=%d)", train_df.height)
    vocabularies, _means, _vars = _collect_input_stats(train_df, layout=layout)
    vocab_lookup: dict[str, dict[str, int]] = {
        name: {val: i + 1 for i, val in enumerate(vocab.values)}
        for name, vocab in vocabularies.items()
    }

    pa_flags = build_pa_flags(PA_RESULT_CLASS_LABELS, _load_pa_seed_rows())

    eval_report: dict[str, Any] = {
        "artifact_id": args.artifact_id,
        "model_path": str(model_path),
        "dataset_artifact": dataset_artifact,
        "primary_split_rows": {
            "train": train_df.height,
            "validate": _val_df.height,
            "test": _test_df.height,
        },
    }

    if not args.skip_mc_probe:
        _LOG.info("encoding train inputs for MC probe")
        train_x = _encode_inputs(train_df, layout=layout, vocabularies=vocabularies)
        probe_rows = resolve_canonical_ids(args.db_path)
        if not probe_rows:
            _LOG.warning("MC probe: zero probe rows resolved; skipping")
        else:
            ctx_idx = sample_mc_context_indices(
                train_df, n_contexts=args.n_contexts, seed=0
            )
            if ctx_idx is None:
                _LOG.warning("MC probe: no contexts sampled; skipping")
            else:
                probe_x = build_mc_probe_inputs(
                    layout=layout,
                    panel_rows=probe_rows,
                    encoded_train=train_x,
                    context_indices=ctx_idx,
                    vocab_lookup=vocab_lookup,
                )
                preds = model.predict(pad_offset_inputs(model, probe_x), verbose=0)
                pa_pred = preds.get("pa_result") if isinstance(preds, dict) else preds
                pa_pred = np.asarray(pa_pred, dtype=np.float64)
                if pa_pred.ndim == 3 and pa_pred.shape[1] == 1:
                    pa_pred = pa_pred[:, 0, :]
                n_ctx = int(ctx_idx.size)
                n_panel = len(probe_rows)
                pa_pred = pa_pred.reshape(n_ctx, n_panel, -1)
                marginal = pa_pred.mean(axis=0)
                eps = 1e-9
                hits = marginal @ pa_flags.is_hit.astype(np.float64)
                at_bats = marginal @ pa_flags.is_at_bat.astype(np.float64)
                on_base = marginal @ pa_flags.is_on_base_success.astype(np.float64)
                on_base_opp = marginal @ pa_flags.is_on_base_opportunity.astype(
                    np.float64
                )
                total_bases = marginal @ pa_flags.total_bases.astype(np.float64)
                avg = hits / np.maximum(at_bats, eps)
                obp = on_base / np.maximum(on_base_opp, eps)
                slg = total_bases / np.maximum(at_bats, eps)
                mc_rows: list[dict[str, Any]] = []
                for i, row in enumerate(probe_rows):
                    mc_rows.append(
                        {
                            "tier": row.tier,
                            "side": row.side,
                            "player_label": row.player_label,
                            "player_id": row.player_id,
                            "avg": float(avg[i]),
                            "obp": float(obp[i]),
                            "slg": float(slg[i]),
                        }
                    )
                    _LOG.info(
                        "mc_probe tier=%s side=%s player=%s AVG=%.3f OBP=%.3f SLG=%.3f",
                        row.tier,
                        row.side,
                        row.player_label,
                        float(avg[i]),
                        float(obp[i]),
                        float(slg[i]),
                    )
                eval_report["mc_marginal_slash"] = {
                    "n_contexts": n_ctx,
                    "rows": mc_rows,
                }

    if not args.skip_linear_probe:
        if "time_forward_fold" in df.columns:
            tf_train_df = df.filter(pl.col("time_forward_fold") == "TRAIN")
            tf_val_df = df.filter(pl.col("time_forward_fold") == "VALIDATE")
            _LOG.info("linear_probe: using time_forward_fold column")
        elif "season" in df.columns:
            tf_train_df = df.filter(pl.col("season") <= 2022)
            tf_val_df = df.filter(pl.col("season") == 2023)
            _LOG.info(
                "linear_probe: deriving fold from season (TRAIN<=2022, VALIDATE=2023)"
            )
        else:
            _LOG.warning(
                "linear_probe: no time_forward_fold or season column; skipping"
            )
            tf_train_df = None
            tf_val_df = None
        if tf_train_df is not None and tf_val_df is not None:
            rng = np.random.default_rng(seed=20260515)
            if tf_train_df.height < 1000 or tf_val_df.height < 200:
                _LOG.warning(
                    "linear_probe: time_forward_fold splits too small (tr=%d va=%d); skipping",
                    tf_train_df.height,
                    tf_val_df.height,
                )
            else:
                n_tr = min(args.probe_train_rows, tf_train_df.height)
                n_va = min(args.probe_val_rows, tf_val_df.height)
                idx_tr = rng.choice(tf_train_df.height, size=n_tr, replace=False)
                idx_va = rng.choice(tf_val_df.height, size=n_va, replace=False)
                tf_train_df = tf_train_df[sorted(int(i) for i in idx_tr)]
                tf_val_df = tf_val_df[sorted(int(i) for i in idx_va)]
                _LOG.info(
                    "linear_probe: encoding TRAIN=%d VALIDATE=%d", n_tr, n_va
                )
                tf_train_x = _encode_inputs(
                    tf_train_df, layout=layout, vocabularies=vocabularies
                )
                tf_val_x = _encode_inputs(
                    tf_val_df, layout=layout, vocabularies=vocabularies
                )
                layer_to_id_columns: dict[str, tuple[str, ...]] = {
                    f"embed_{unit}": tuple(
                        c for c in layout.high_card_columns
                        if layout.embedding_unit_for_column(c) == unit
                    )
                    for unit in layout.embedding_unit_names()
                }
                task_targets = ("pa_result", "trajectory_remapped", "outs_on_play_capped")
                tasks: list[LinearProbeTaskSpec] = []
                for tname in task_targets:
                    if tname not in tf_train_df.columns:
                        continue
                    labels_set = {
                        str(v) for v in tf_train_df[tname].drop_nulls().unique().to_list()
                    }
                    if not labels_set:
                        continue
                    sorted_labels = sorted(labels_set)
                    code_by_label = {l: i for i, l in enumerate(sorted_labels)}

                    def _codes(s: pl.Series) -> np.ndarray:
                        out: list[int] = []
                        for v in s.to_list():
                            if v is None:
                                out.append(-1)
                            else:
                                out.append(code_by_label.get(str(v), -1))
                        return np.asarray(out, dtype=np.int64)

                    tasks.append(
                        LinearProbeTaskSpec(
                            name=tname,
                            y_train=_codes(tf_train_df[tname]),
                            y_val=_codes(tf_val_df[tname]),
                        )
                    )
                if tasks:
                    callback = build_linear_probe_callback(
                        train_ids=tf_train_x,
                        val_ids=tf_val_x,
                        layer_to_id_columns=layer_to_id_columns,
                        tasks=tuple(tasks),
                        log_every_n_epochs=1,
                    )
                    callback.set_model(model)
                    callback.on_epoch_end(epoch=0)
                    # Re-read the proxy score from history if available; we
                    # re-run inline below for the report payload.
                    proxy_payload: list[dict[str, Any]] = []
                    scores: list[float] = []
                    X_train = callback._build_features(tf_train_x)  # type: ignore[attr-defined]
                    X_val = callback._build_features(tf_val_x)  # type: ignore[attr-defined]
                    if X_train.size > 0 and X_val.size > 0:
                        from sklearn.linear_model import LogisticRegression
                        from sklearn.metrics import f1_score, log_loss

                        for task in tasks:
                            mask_tr = task.y_train >= 0
                            mask_va = task.y_val >= 0
                            if mask_tr.sum() < 32 or mask_va.sum() < 32:
                                continue
                            clf = LogisticRegression(
                                max_iter=200, n_jobs=-1, solver="lbfgs"
                            )
                            clf.fit(X_train[mask_tr], task.y_train[mask_tr])
                            probs = clf.predict_proba(X_val[mask_va])
                            pred_labels = clf.classes_[probs.argmax(axis=1)]
                            f1 = float(
                                f1_score(
                                    task.y_val[mask_va],
                                    pred_labels,
                                    average="macro",
                                )
                            )
                            try:
                                ll = float(
                                    log_loss(
                                        task.y_val[mask_va],
                                        probs,
                                        labels=clf.classes_,
                                    )
                                )
                            except ValueError:
                                ll = float("nan")
                            proxy_payload.append(
                                {"task": task.name, "macro_f1": f1, "log_loss": ll}
                            )
                            scores.append(f1)
                    eval_report["downstream_proxy"] = {
                        "tasks": proxy_payload,
                        "downstream_proxy_score": (
                            float(np.mean(scores)) if scores else float("nan")
                        ),
                        "feature_dim": int(X_train.shape[1]) if X_train.size > 0 else 0,
                        "n_train": int(tf_train_df.height),
                        "n_val": int(tf_val_df.height),
                    }

    if not args.skip_confound_probe:
        embeddings_path = artifact_dir / "exports" / "embeddings.parquet"
        if not embeddings_path.exists():
            _LOG.warning("confound_probe: embeddings.parquet missing; skipping")
        else:
            emb_df = pl.read_parquet(embeddings_path)
            player_df = emb_df.filter(pl.col("entity_type") == "player")
            if player_df.height < 50:
                _LOG.warning(
                    "confound_probe: too few player rows (%d); skipping",
                    player_df.height,
                )
            else:
                vector_col = None
                for cand in ("embedding_value", "vector", "embedding", "values", "vec"):
                    if cand in player_df.columns:
                        vector_col = cand
                        break
                if vector_col is None:
                    _LOG.warning(
                        "confound_probe: no embedding column found in %s; cols=%s",
                        embeddings_path,
                        player_df.columns,
                    )
                    raise SystemExit(3)
                vectors = np.asarray(
                    player_df[vector_col].to_list(), dtype=np.float64
                )
                player_ids = player_df["entity_id"].cast(pl.Utf8).to_list()
                labels = _confound_labels_for_players(dataset_parquet, player_ids)
                results = confound_probe(vectors, labels)
                eval_report["confound_leak"] = [
                    {
                        "confound": r.confound,
                        "macro_auc": r.macro_auc,
                        "random_baseline_auc": r.random_baseline_auc,
                        "delta_vs_random": r.delta_vs_random,
                        "n_entities": r.n_entities,
                        "n_classes": r.n_classes,
                    }
                    for r in results
                ]
                for r in results:
                    _LOG.info(
                        "confound_leak confound=%s macro_auc=%.3f random=%.3f delta=%.3f n=%d k=%d",
                        r.confound,
                        r.macro_auc,
                        r.random_baseline_auc,
                        r.delta_vs_random,
                        r.n_entities,
                        r.n_classes,
                    )

    embed_health_payload: list[dict[str, Any]] = []
    for unit in layout.embedding_unit_names():
        name = f"embed_{unit}"
        try:
            layer = model.get_layer(name)
        except ValueError:
            continue
        weights = layer.get_weights()
        if not weights:
            continue
        matrix = np.asarray(weights[0], dtype=np.float32)
        if matrix.ndim != 2 or matrix.size == 0:
            continue
        n_rows, dim = matrix.shape
        try:
            s = np.linalg.svd(matrix, compute_uv=False)
        except np.linalg.LinAlgError:
            s = np.zeros(min(matrix.shape), dtype=np.float32)
        sum_s = float(np.sum(s))
        sum_s2 = float(np.sum(s * s))
        eff_rank = (sum_s * sum_s) / max(sum_s2, 1e-12)
        sample_size = min(2000, n_rows)
        rng = np.random.default_rng(seed=20260515)
        idx = rng.choice(n_rows, size=sample_size, replace=False)
        sample = matrix[idx].astype(np.float64)
        norms = np.linalg.norm(sample, axis=1, keepdims=True)
        normalized = sample / np.maximum(norms, 1e-9)
        cosines = normalized @ normalized.T
        upper = cosines[np.triu_indices_from(cosines, k=1)]
        mean_abs_cos = float(np.mean(np.abs(upper))) if upper.size > 0 else 0.0
        col_var = np.var(matrix, axis=0)
        embed_health_payload.append(
            {
                "layer": name,
                "n_rows": int(n_rows),
                "dim": int(dim),
                "eff_rank": float(eff_rank),
                "eff_rank_fraction": float(eff_rank / max(dim, 1)),
                "mean_abs_cosine": mean_abs_cos,
                "col_var_min": float(np.min(col_var)),
                "col_var_mean": float(np.mean(col_var)),
                "col_var_max": float(np.max(col_var)),
            }
        )
        _LOG.info(
            "embed_health layer=%s eff_rank=%.2f eff_rank_frac=%.3f mean_abs_cos=%.4f",
            name,
            eff_rank,
            eff_rank / max(dim, 1),
            mean_abs_cos,
        )
    eval_report["embed_health"] = embed_health_payload

    try:
        matrices = {
            unit: np.asarray(
                model.get_layer(f"embed_{unit}").get_weights()[0], dtype=np.float64
            )
            for unit in layout.embedding_unit_names()
        }
        eval_report["role_bias_diag"] = extract_role_bias_diagnostics(
            model, layout, matrices
        )
    except Exception as exc:
        _LOG.warning("role_bias_diag failed: %s", exc)

    out_path = eval_dir / "eval_report.json"
    _atomic_write_json(out_path, eval_report)
    _LOG.info("wrote %s", out_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
