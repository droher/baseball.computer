"""Statistical-package CLI.

Subcommands per
`notes/data-coverage-implementation/05-runtime-artifacts-and-library.md`.
``prepare-dataset`` is implemented; downstream commands stay stubbed
until each phase lands.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import cast

from python_models.statistical.dataset_registry import all_dataset_names, get_spec
from python_models.statistical.datasets import prepare_dataset
from python_models.statistical.duckdb_io import open_bc_db
from python_models.statistical.eda import run_eda
from python_models.statistical.leakage import (
    check_split_leakage,
    summarize_violations,
)
from python_models.statistical.model_config import ModelConfig
from python_models.statistical.publication import evaluate_publication_gate
from python_models.statistical.schemas import EdaReport
from python_models.statistical.logging import configure as configure_logging

_log = logging.getLogger(__name__)

_DEFAULT_LEDGER_SCHEMA: str = "main_models"
_ENV_LEDGER_SCHEMA: str = "BC_LEDGER_SCHEMA"


def _add_dataset_artifact_arg(p: argparse.ArgumentParser) -> None:
    _ = p.add_argument(
        "--dataset-artifact", required=True, help="Dataset artifact ID to consume."
    )


def _add_artifact_id_arg(p: argparse.ArgumentParser) -> None:
    _ = p.add_argument(
        "--artifact-id", required=True, help="Artifact ID for the output."
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bc-stats",
        description="baseball.computer statistical-package CLI.",
    )
    _ = parser.add_argument(
        "--log-level",
        default="INFO",
        help="stdlib logging level name (default: INFO).",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare = subparsers.add_parser(
        "prepare-dataset",
        help="Export Parquet dataset snapshot + metadata for a modeling dataset.",
    )
    _ = prepare.add_argument(
        "--dataset",
        required=True,
        choices=all_dataset_names(),
        help="Modeling dataset name (one of the registered model_input_* views).",
    )
    _add_artifact_id_arg(prepare)
    _ = prepare.add_argument(
        "--ledger-schema",
        default=None,
        help=(
            f"DuckDB schema holding the model_input_* views. Defaults to "
            f"${_ENV_LEDGER_SCHEMA} env var, then {_DEFAULT_LEDGER_SCHEMA!r}."
        ),
    )
    _ = prepare.add_argument(
        "--source-snapshot-id",
        default=None,
        help=(
            "Override the source_snapshot_id stamp instead of inferring it "
            "from the dataset rows. Use when the SQLMesh source_snapshot_id "
            "var is set to a versioned ID and you want to assert it matches."
        ),
    )
    _ = prepare.add_argument(
        "--db-path",
        default=None,
        help="Override the DuckDB path (defaults to BC_DB_PATH then bc_dev.db).",
    )
    _ = prepare.add_argument(
        "--output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )

    check_leakage = subparsers.add_parser(
        "check-split-leakage",
        help=(
            "Run split-registry leakage checks against a frozen dataset "
            "Parquet snapshot. Exits non-zero if any unit maps to multiple "
            "fold/holdout values."
        ),
    )
    _ = check_leakage.add_argument(
        "--dataset",
        required=True,
        choices=all_dataset_names(),
        help="Modeling dataset name (one of the registered model_input_* views).",
    )
    _add_dataset_artifact_arg(check_leakage)
    _ = check_leakage.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )

    gate_parser = subparsers.add_parser(
        "check-publication-gate",
        help=(
            "Compare an EDA report against a ModelConfig JSON and report "
            "whether the model is clear to publish."
        ),
    )
    _ = gate_parser.add_argument(
        "--model-config",
        required=True,
        help="Path to a ModelConfig JSON file.",
    )
    _ = gate_parser.add_argument(
        "--eda-report",
        required=True,
        help="Path to an EDA report.json file emitted by run-eda.",
    )

    run_eda_parser = subparsers.add_parser(
        "run-eda",
        help="Generate EDA tables, weak-identification flags, markdown summary.",
    )
    _ = run_eda_parser.add_argument(
        "--dataset",
        required=True,
        choices=all_dataset_names(),
        help="Modeling dataset name (one of the registered model_input_* views).",
    )
    _add_dataset_artifact_arg(run_eda_parser)
    _add_artifact_id_arg(run_eda_parser)
    _ = run_eda_parser.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )
    _ = run_eda_parser.add_argument(
        "--output-root",
        default=None,
        help="Override the EDA artifact root (defaults to artifacts/statistical/eda).",
    )

    fit_deep = subparsers.add_parser(
        "fit-deep",
        help="Train deep proposals/embeddings on a frozen dataset.",
    )
    _ = fit_deep.add_argument("--target", required=True, help="Deep target name.")
    _add_dataset_artifact_arg(fit_deep)
    _add_artifact_id_arg(fit_deep)
    _ = fit_deep.add_argument(
        "--source-snapshot-id",
        default=None,
        help=(
            "Override the source_snapshot_id stamped onto the deep manifest. "
            "Defaults to the dataset artifact's source_snapshot_id."
        ),
    )
    _ = fit_deep.add_argument(
        "--epochs",
        type=int,
        default=None,
        help="Override training epochs per fit (default: deep.training.DEFAULT_EPOCHS).",
    )
    _ = fit_deep.add_argument(
        "--keras-batch-size",
        type=int,
        default=None,
        help="Override Keras batch size (default: deep.training.DEFAULT_KERAS_BATCH_SIZE).",
    )
    _ = fit_deep.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )
    _ = fit_deep.add_argument(
        "--output-root",
        default=None,
        help="Override the deep artifact root (defaults to artifacts/statistical/deep).",
    )

    fit_bayes = subparsers.add_parser(
        "fit-bayes",
        help="Fit a hierarchical Bayesian model.",
    )
    _ = fit_bayes.add_argument("--model", required=True, help="Bayesian model name.")
    _add_dataset_artifact_arg(fit_bayes)
    _add_artifact_id_arg(fit_bayes)
    _ = fit_bayes.add_argument(
        "--smoke",
        action="store_true",
        help=(
            "Use SMOKE_CONFIG sampler (50 draws x 50 tune x 2 chains) "
            "and apply BC_STATS_SMOKE_LIMIT (default 100k rows)."
        ),
    )
    _ = fit_bayes.add_argument(
        "--prior-only",
        action="store_true",
        help="Short-circuit before pm.sample; emit prior predictive only.",
    )
    _ = fit_bayes.add_argument(
        "--flavor",
        choices=["gamma_dl_zero", "gamma_dl_shrunk"],
        default="gamma_dl_zero",
        help="DL covariate flavor for the geometry model. Ignored by non-geometry targets.",
    )
    _ = fit_bayes.add_argument(
        "--source-snapshot-id",
        default=None,
        help=(
            "Override the source_snapshot_id stamped onto the bayes manifest. "
            "Defaults to the dataset artifact's source_snapshot_id."
        ),
    )
    _ = fit_bayes.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )
    _ = fit_bayes.add_argument(
        "--output-root",
        default=None,
        help="Override the bayes artifact root (defaults to artifacts/statistical/bayes).",
    )

    export_sql = subparsers.add_parser(
        "export-sql",
        help="Write SQL-consumable Parquet exports from a posterior or deep artifact.",
    )
    _ = export_sql.add_argument("--model", required=True, help="Model name to export.")
    _add_artifact_id_arg(export_sql)

    validate = subparsers.add_parser(
        "validate",
        help="Run conservation/calibration/holdout/sensitivity checks on an artifact.",
    )
    _add_artifact_id_arg(validate)
    _ = validate.add_argument(
        "--model",
        default=None,
        help=(
            "Model name to disambiguate when multiple models share an "
            "artifact id (e.g. trajectory_observedness vs "
            "location_side_observedness both at 10k-v1)."
        ),
    )
    _ = validate.add_argument(
        "--output-path",
        default=None,
        help=(
            "Override where validation_report.json is written (defaults to "
            "<artifact_dir>/validation/validation_report.json)."
        ),
    )

    publish = subparsers.add_parser(
        "publish-manifest",
        help="Write a published-pointer JSON naming the approved artifact ID.",
    )
    _ = publish.add_argument("--model", required=True, help="Model name to publish.")
    _add_artifact_id_arg(publish)

    fit_pretrain = subparsers.add_parser(
        "fit-pretrain",
        help=(
            "Pretrain shared entity embeddings on a multi-head pretext "
            "dataset (default: event_universe)."
        ),
    )
    _ = fit_pretrain.add_argument(
        "--pretrain-target",
        default="event_universe",
        help="Pretrain target name (default: event_universe).",
    )
    _add_dataset_artifact_arg(fit_pretrain)
    _add_artifact_id_arg(fit_pretrain)
    _ = fit_pretrain.add_argument(
        "--source-snapshot-id",
        default=None,
        help=(
            "Override the source_snapshot_id stamped onto the pretrain manifest. "
            "Defaults to the dataset artifact's source_snapshot_id."
        ),
    )
    _ = fit_pretrain.add_argument(
        "--epochs",
        type=int,
        default=None,
        help="Override training epochs (default: pretrain.training.DEFAULT_EPOCHS).",
    )
    _ = fit_pretrain.add_argument(
        "--stage1-epochs",
        type=int,
        default=None,
        help=(
            "Stage-1 (freeze-trunk) epochs. Falls back to "
            "BC_PRETRAIN_STAGE1_EPOCHS env, then DEFAULT_STAGE1_EPOCHS (0 = single-stage). "
            "Stage-2 epochs = total --epochs minus this value."
        ),
    )
    _ = fit_pretrain.add_argument(
        "--keras-batch-size",
        type=int,
        default=None,
        help="Override Keras batch size (default: pretrain.training.DEFAULT_KERAS_BATCH_SIZE).",
    )
    _ = fit_pretrain.add_argument(
        "--dataset-output-root",
        default=None,
        help="Override the dataset artifact root (defaults to artifacts/statistical/datasets).",
    )
    _ = fit_pretrain.add_argument(
        "--output-root",
        default=None,
        help="Override the artifact root (defaults to artifacts/statistical/deep).",
    )

    publish_pretrain = subparsers.add_parser(
        "publish-pretrain",
        help=(
            "Write a published-pointer JSON under <root>/pretrain/<name>.json "
            "naming the approved pretrain artifact ID."
        ),
    )
    _add_artifact_id_arg(publish_pretrain)

    return parser


def _resolve_ledger_schema(cli_value: str | None) -> str:
    if cli_value is not None:
        return cli_value
    return os.environ.get(_ENV_LEDGER_SCHEMA, _DEFAULT_LEDGER_SCHEMA)


def _run_prepare_dataset(args: argparse.Namespace) -> int:
    spec = get_spec(args.dataset)
    ledger_schema = _resolve_ledger_schema(args.ledger_schema)
    db_path = Path(args.db_path) if args.db_path else None
    output_root = Path(args.output_root) if args.output_root else None
    with open_bc_db(db_path, read_only=True) as con:
        manifest = prepare_dataset(
            spec,
            artifact_id=args.artifact_id,
            con=con,
            ledger_schema=ledger_schema,
            output_root=output_root,
            source_snapshot_id_override=args.source_snapshot_id,
        )
    _log.info(
        "prepare-dataset completed artifact_id=%s output=%s",
        manifest.artifact_id,
        manifest.output_paths.get("dataset"),
    )
    return 0


def _run_check_split_leakage(args: argparse.Namespace) -> int:
    from python_models.statistical.config import DATASETS_ROOT

    spec = get_spec(args.dataset)
    root = Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    parquet_path = root / spec.name / args.dataset_artifact / "dataset.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {parquet_path}; run prepare-dataset first."
        )
    violations = check_split_leakage(parquet_path)
    if not violations:
        _log.info(
            "check-split-leakage clean dataset=%s artifact=%s",
            spec.name,
            args.dataset_artifact,
        )
        return 0
    summary = summarize_violations(violations)
    sample_size = min(len(violations), 10)
    _log.error(
        "check-split-leakage failed dataset=%s artifact=%s total=%d by_kind=%s",
        spec.name,
        args.dataset_artifact,
        len(violations),
        summary,
    )
    for v in violations[:sample_size]:
        _log.error(
            "leakage unit_kind=%s unit_id=%s distinct=%d values=%s rows=%d",
            v.unit_kind,
            v.unit_id,
            v.distinct_value_count,
            ",".join(v.distinct_values),
            v.row_count,
        )
    return 1


def _run_check_publication_gate(args: argparse.Namespace) -> int:
    config_path = Path(args.model_config)
    report_path = Path(args.eda_report)
    if not config_path.exists():
        raise FileNotFoundError(f"ModelConfig JSON not found at {config_path}")
    if not report_path.exists():
        raise FileNotFoundError(f"EDA report.json not found at {report_path}")

    config = ModelConfig.model_validate_json(config_path.read_text(encoding="utf-8"))
    report = EdaReport.model_validate_json(report_path.read_text(encoding="utf-8"))
    result = evaluate_publication_gate(config=config, eda_report=report)

    if result.warnings:
        for w in result.warnings:
            _log.info(
                "publication_gate_warning code=%s severity=%s effect=%s slice=%s message=%s",
                w.code,
                w.severity,
                w.effect,
                w.slice,
                w.message,
            )
    if result.blocking_violations:
        for v in result.blocking_violations:
            _log.error(
                "publication_gate_block code=%s effect=%s slice=%s message=%s",
                v.code,
                v.effect,
                v.slice,
                v.message,
            )
        _log.error(
            "publication_gate FAILED model=%s blocking=%d",
            config.model_name,
            len(result.blocking_violations),
        )
        return 1
    _log.info(
        "publication_gate PASSED model=%s warnings=%d",
        config.model_name,
        len(result.warnings),
    )
    return 0


def _run_run_eda(args: argparse.Namespace) -> int:
    spec = get_spec(args.dataset)
    output_root = Path(args.output_root) if args.output_root else None
    dataset_artifact_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else None
    )
    manifest = run_eda(
        spec,
        dataset_artifact_id=args.dataset_artifact,
        artifact_id=args.artifact_id,
        output_root=output_root,
        dataset_artifact_root=dataset_artifact_root,
    )
    if manifest.blocking_findings:
        for code in manifest.blocking_findings:
            _log.error(
                "run_eda_blocking_finding artifact_id=%s code=%s",
                manifest.artifact_id,
                code,
            )
        _log.error(
            "run-eda FAILED artifact_id=%s blocking_findings=%d",
            manifest.artifact_id,
            len(manifest.blocking_findings),
        )
        return 1
    _log.info(
        "run-eda completed artifact_id=%s blocking_findings=%d",
        manifest.artifact_id,
        len(manifest.blocking_findings),
    )
    return 0


def _run_fit_deep(args: argparse.Namespace) -> int:
    from python_models.statistical.config import DATASETS_ROOT, DEEP_ROOT
    from python_models.statistical.deep import targets as _targets  # noqa: F401
    from python_models.statistical.deep.feature_layout import coverage_layout_for
    from python_models.statistical.deep.registry import get_target
    from python_models.statistical.deep.training import (
        DEFAULT_EPOCHS,
        DEFAULT_KERAS_BATCH_SIZE,
        run_target,
    )
    from python_models.statistical.manifests import read_manifest

    spec = get_target(args.target)
    dataset_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_dir = dataset_root / spec.dataset_name / args.dataset_artifact
    parquet_path = dataset_dir / "dataset.parquet"
    manifest_path = dataset_dir / "manifest.json"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {parquet_path}; run prepare-dataset first."
        )
    if args.source_snapshot_id is not None:
        source_snapshot_id = str(args.source_snapshot_id)
    elif manifest_path.exists():
        source_snapshot_id = read_manifest(manifest_path).source_snapshot_id
    else:
        raise FileNotFoundError(
            f"dataset manifest missing at {manifest_path}; pass --source-snapshot-id to override."
        )

    output_root = Path(args.output_root) if args.output_root else DEEP_ROOT
    layout = coverage_layout_for(spec.dataset_name)
    result = run_target(
        spec,
        dataset_parquet=parquet_path,
        artifact_id=args.artifact_id,
        layout=layout,
        source_snapshot_id=source_snapshot_id,
        dataset_artifact_id=args.dataset_artifact,
        artifact_root=output_root,
        epochs=int(args.epochs) if args.epochs is not None else DEFAULT_EPOCHS,
        keras_batch_size=(
            int(args.keras_batch_size)
            if args.keras_batch_size is not None
            else DEFAULT_KERAS_BATCH_SIZE
        ),
    )
    _log.info(
        "fit-deep completed target=%s artifact_id=%s train=%d validate=%d test=%d",
        spec.name,
        args.artifact_id,
        result.train_rows,
        result.validate_rows,
        result.test_rows,
    )
    return 0


def _run_fit_bayes(args: argparse.Namespace) -> int:
    from python_models.statistical.bayes import targets as _bayes_targets  # noqa: F401
    from python_models.statistical.bayes.registry import get_target
    from python_models.statistical.bayes.specs import GammaDlFlavor
    from python_models.statistical.bayes.training import run_bayes_model
    from python_models.statistical.config import BAYES_ROOT, DATASETS_ROOT
    from python_models.statistical.dataset_registry import get_spec as _get_dataset_spec
    from python_models.statistical.manifests import read_manifest

    spec = get_target(str(args.model))
    dataset_spec = _get_dataset_spec(spec.dataset_name)
    dataset_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_dir = dataset_root / dataset_spec.name / args.dataset_artifact
    parquet_path = dataset_dir / "dataset.parquet"
    manifest_path = dataset_dir / "manifest.json"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {parquet_path}; run prepare-dataset first."
        )
    if args.source_snapshot_id is not None:
        source_snapshot_id = str(args.source_snapshot_id)
    elif manifest_path.exists():
        source_snapshot_id = read_manifest(manifest_path).source_snapshot_id
    else:
        raise FileNotFoundError(
            f"dataset manifest missing at {manifest_path}; pass --source-snapshot-id to override."
        )

    output_root = Path(args.output_root) if args.output_root else BAYES_ROOT
    manifest = run_bayes_model(
        model_name=str(args.model),
        dataset_artifact_id=str(args.dataset_artifact),
        artifact_id=str(args.artifact_id),
        source_snapshot_id=source_snapshot_id,
        smoke=bool(args.smoke),
        prior_only=bool(args.prior_only),
        gamma_dl_flavor=cast(GammaDlFlavor, args.flavor),
        artifact_root=output_root,
        dataset_root=dataset_root,
    )
    _log.info(
        "fit-bayes completed model=%s artifact_id=%s smoke=%s prior_only=%s",
        manifest.name,
        manifest.artifact_id,
        bool(args.smoke),
        bool(args.prior_only),
    )
    return 0


def _run_validate(args: argparse.Namespace) -> int:
    import os
    import tempfile

    from python_models.statistical.validate import (
        _find_manifest,  # type: ignore[reportPrivateUsage]
        validate_artifact,
    )

    artifact_id = str(args.artifact_id)
    model_name = getattr(args, "model", None)
    candidate_roots = _build_candidate_roots()
    report = validate_artifact(
        artifact_id, model_name=model_name, candidate_roots=candidate_roots
    )
    artifact_dir = _find_manifest(
        artifact_id, candidate_roots, model_name=model_name
    ).parent
    output_path = (
        Path(args.output_path)
        if args.output_path
        else artifact_dir / "validation" / "validation_report.json"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = report.model_dump_json(indent=2)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(output_path.parent), prefix=".validation.", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            _ = fh.write(payload)
        os.replace(tmp_name, output_path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise

    blocking = sum(1 for f in report.findings if f.severity == "block")
    _log.info(
        "validate artifact_id=%s kind=%s status=%s findings=%d blocking=%d output=%s",
        report.artifact_id,
        report.kind,
        report.status,
        len(report.findings),
        blocking,
        output_path,
    )
    return 1 if report.status == "failed" else 0


def _build_candidate_roots() -> tuple[Path, ...]:
    from python_models.statistical.config import (
        BAYES_ROOT,
        DATASETS_ROOT,
        DEEP_ROOT,
        EDA_ROOT,
    )

    return (DEEP_ROOT, BAYES_ROOT, DATASETS_ROOT, EDA_ROOT)


def _run_publish_manifest(args: argparse.Namespace) -> int:
    from datetime import datetime, timezone

    from python_models.statistical.config import (
        BAYES_ROOT,
        DATASETS_ROOT,
        DEEP_ROOT,
        EDA_ROOT,
    )
    from python_models.statistical.manifests import (
        read_manifest,
        write_published_pointer,
    )
    from python_models.statistical.schemas import PublishedPointer

    artifact_id = str(args.artifact_id)
    model_name = str(args.model)

    candidate_roots: list[Path] = [DEEP_ROOT, BAYES_ROOT, DATASETS_ROOT, EDA_ROOT]
    found: Path | None = None
    for root in candidate_roots:
        for candidate in root.rglob(f"{artifact_id}/manifest.json"):
            if candidate.parent.parent.name != model_name:
                continue
            found = candidate
            break
        if found is not None:
            break
    if found is None:
        raise FileNotFoundError(
            f"no manifest.json for model={model_name!r} artifact_id={artifact_id!r} under {[str(r) for r in candidate_roots]}"
        )

    manifest = read_manifest(found)
    if manifest.artifact_id != artifact_id:
        raise ValueError(
            f"manifest.json at {found} reports artifact_id={manifest.artifact_id!r}, expected {artifact_id!r}"
        )
    pointer = PublishedPointer(
        model_name=model_name,
        artifact_id=artifact_id,
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=found,
    )
    target = write_published_pointer(pointer)
    _log.info(
        "publish-manifest wrote pointer model=%s artifact_id=%s path=%s",
        model_name,
        artifact_id,
        target,
    )
    return 0


def _run_fit_pretrain(args: argparse.Namespace) -> int:
    from python_models.statistical.config import DATASETS_ROOT, DEEP_ROOT
    from python_models.statistical.deep.pretrain import run_pretrain
    from python_models.statistical.deep.pretrain.targets import (
        get_pretrain_layout,
        get_pretrain_spec,
    )
    from python_models.statistical.deep.pretrain.training import (
        DEFAULT_EPOCHS,
        DEFAULT_KERAS_BATCH_SIZE,
        DEFAULT_STAGE1_EPOCHS,
    )
    from python_models.statistical.manifests import read_manifest

    spec = get_pretrain_spec(args.pretrain_target)
    layout = get_pretrain_layout(args.pretrain_target)
    dataset_root = (
        Path(args.dataset_output_root) if args.dataset_output_root else DATASETS_ROOT
    )
    dataset_dir = dataset_root / spec.dataset_name / args.dataset_artifact
    parquet_path = dataset_dir / "dataset.parquet"
    manifest_path = dataset_dir / "manifest.json"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"dataset Parquet missing at {parquet_path}; run prepare-dataset first."
        )
    if args.source_snapshot_id is not None:
        source_snapshot_id = str(args.source_snapshot_id)
    elif manifest_path.exists():
        source_snapshot_id = read_manifest(manifest_path).source_snapshot_id
    else:
        raise FileNotFoundError(
            f"dataset manifest missing at {manifest_path}; pass --source-snapshot-id to override."
        )

    output_root = Path(args.output_root) if args.output_root else DEEP_ROOT
    if args.stage1_epochs is not None:
        stage1_epochs = int(args.stage1_epochs)
    else:
        env_stage1 = os.environ.get("BC_PRETRAIN_STAGE1_EPOCHS")
        stage1_epochs = int(env_stage1) if env_stage1 else DEFAULT_STAGE1_EPOCHS
    result = run_pretrain(
        spec,
        dataset_parquet=parquet_path,
        artifact_id=args.artifact_id,
        layout=layout,
        source_snapshot_id=source_snapshot_id,
        dataset_artifact_id=args.dataset_artifact,
        artifact_root=output_root,
        epochs=int(args.epochs) if args.epochs is not None else DEFAULT_EPOCHS,
        stage1_epochs=stage1_epochs,
        keras_batch_size=(
            int(args.keras_batch_size)
            if args.keras_batch_size is not None
            else DEFAULT_KERAS_BATCH_SIZE
        ),
    )
    _log.info(
        "fit-pretrain completed target=%s artifact_id=%s train=%d validate=%d test=%d",
        spec.name,
        args.artifact_id,
        result.train_rows,
        result.validate_rows,
        result.test_rows,
    )
    return 0


def _run_publish_pretrain(args: argparse.Namespace) -> int:
    from datetime import datetime, timezone

    from python_models.statistical.config import DEEP_ROOT, resolve_published_roots
    from python_models.statistical.manifests import (
        PRETRAIN_POINTER_SUBDIR,
        read_manifest,
        write_published_pointer,
    )
    from python_models.statistical.schemas import PublishedPointer

    artifact_id = str(args.artifact_id)
    found: Path | None = None
    for candidate in DEEP_ROOT.rglob(f"{artifact_id}/manifest.json"):
        found = candidate
        break
    if found is None:
        raise FileNotFoundError(
            f"no manifest.json for pretrain artifact_id={artifact_id!r} under {DEEP_ROOT}"
        )
    manifest = read_manifest(found)
    if manifest.kind != "pretrain":
        raise ValueError(
            f"manifest at {found} has kind={manifest.kind!r}, expected 'pretrain'"
        )
    if manifest.artifact_id != artifact_id:
        raise ValueError(
            f"manifest at {found} reports artifact_id={manifest.artifact_id!r}, expected {artifact_id!r}"
        )

    branch_root, _ = resolve_published_roots()
    pretrain_root = branch_root / PRETRAIN_POINTER_SUBDIR
    pointer = PublishedPointer(
        model_name=manifest.name,
        artifact_id=artifact_id,
        published_at=datetime.now(tz=timezone.utc),
        manifest_path=found,
    )
    target = write_published_pointer(pointer, root=pretrain_root)
    _log.info(
        "publish-pretrain wrote pointer name=%s artifact_id=%s path=%s",
        manifest.name,
        artifact_id,
        target,
    )
    return 0


def _dispatch(args: argparse.Namespace) -> int:
    match args.command:
        case "prepare-dataset":
            return _run_prepare_dataset(args)
        case "run-eda":
            return _run_run_eda(args)
        case "check-split-leakage":
            return _run_check_split_leakage(args)
        case "check-publication-gate":
            return _run_check_publication_gate(args)
        case "fit-deep":
            return _run_fit_deep(args)
        case "fit-bayes":
            return _run_fit_bayes(args)
        case "fit-pretrain":
            return _run_fit_pretrain(args)
        case "publish-manifest":
            return _run_publish_manifest(args)
        case "publish-pretrain":
            return _run_publish_pretrain(args)
        case "validate":
            return _run_validate(args)
        case other:
            raise NotImplementedError(
                f"command {other!r} is a scaffolding stub; real implementation lands per the data-coverage checklist."
            )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
