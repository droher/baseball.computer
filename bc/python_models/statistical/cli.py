"""Statistical-package CLI.

Subcommands per
`notes/data-coverage-implementation/05-runtime-artifacts-and-library.md`.
Bodies stay stubbed until each phase lands.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence

from python_models.statistical.logging import configure as configure_logging

_log = logging.getLogger(__name__)


def _add_dataset_artifact_arg(p: argparse.ArgumentParser) -> None:
    _ = p.add_argument("--dataset-artifact", required=True, help="Dataset artifact ID to consume.")


def _add_artifact_id_arg(p: argparse.ArgumentParser) -> None:
    _ = p.add_argument("--artifact-id", required=True, help="Artifact ID for the output.")


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
    _ = prepare.add_argument("--dataset", required=True, help="SQLMesh dataset model name.")
    _add_artifact_id_arg(prepare)

    run_eda = subparsers.add_parser(
        "run-eda",
        help="Generate EDA tables, weak-identification flags, markdown summary.",
    )
    _add_dataset_artifact_arg(run_eda)

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


def _dispatch(args: argparse.Namespace) -> int:
    raise NotImplementedError(
        f"command {args.command!r} is a Phase 1 scaffolding stub; real implementation lands per the data-coverage checklist."
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    configure_logging(getattr(logging, args.log_level.upper(), logging.INFO))
    return _dispatch(args)


if __name__ == "__main__":
    sys.exit(main())
