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
        help="Train deep proposals/embeddings/calibrators on a frozen dataset.",
    )
    _ = fit_deep.add_argument("--target", required=True, help="Deep target name.")
    _add_dataset_artifact_arg(fit_deep)

    fit_bayes = subparsers.add_parser(
        "fit-bayes",
        help="Fit a hierarchical Bayesian model.",
    )
    _ = fit_bayes.add_argument("--model", required=True, help="Bayesian model name.")
    _add_dataset_artifact_arg(fit_bayes)
    _ = fit_bayes.add_argument(
        "--gamma-dl",
        choices=("zero", "shrunk"),
        default="zero",
        help="DL covariate ablation flavor.",
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

    publish = subparsers.add_parser(
        "publish-manifest",
        help="Write a published-pointer JSON naming the approved artifact ID.",
    )
    _ = publish.add_argument("--model", required=True, help="Model name to publish.")
    _add_artifact_id_arg(publish)

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
    _log.info(
        "run-eda completed artifact_id=%s blocking_findings=%d",
        manifest.artifact_id,
        len(manifest.blocking_findings),
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
