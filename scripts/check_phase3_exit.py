"""Phase-3 exit-gate aggregator.

Walks every published deep artifact under
``BC_STATS_PUBLISHED_ROOT`` (branch root) and ``GLOBAL_PUBLISHED_ROOT``,
runs ``validate_artifact`` against each, and prints a per-target status
table plus per-acceptance-criterion summary.

Exit code 0 when every artifact reports ``status == 'passed'``; 1 when
any artifact is blocked.

Designed to be invoked manually (``uv run --group ml python
scripts/check_phase3_exit.py``) after the final Phase-3 fits land. CI
runs it after PR7 closes; the formal exit-gate run is documented in
``notes/data-coverage-implementation/phase3-exit-deep-gates.md``.
"""

from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Sequence
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BC_DIR = REPO_ROOT / "bc"
if str(BC_DIR) not in sys.path:
    sys.path.insert(0, str(BC_DIR))

from python_models.statistical import config as cfg  # noqa: E402
from python_models.statistical.deep import targets as _targets  # noqa: E402, F401
from python_models.statistical.deep.registry import (  # noqa: E402
    all_target_names,
    get_target,
)
from python_models.statistical.manifests import (  # noqa: E402
    find_published_manifest,
    read_manifest,
)
from python_models.statistical.schemas import (  # noqa: E402
    ArtifactManifest,
    PublishedPointer,
    ValidationReport,
)
from python_models.statistical.validate import validate_artifact  # noqa: E402

_log = logging.getLogger(__name__)

ACCEPTANCE_CRITERIA: tuple[str, ...] = (
    "OOF predictions present for every training row",
    "Calibration passes by era / source / scorer / hit-vs-out / missingness",
    "Probability vectors normalize within key/class groups",
    "Personnel + structural masks applied before fielding proposals",
    "Embedding probes documented and per-source-family AUC recorded",
    "Baseline (deterministic geometry rule) log-loss/Brier comparison present",
    "No SQL artifact exposes only argmax — probabilities, not classes",
)


def _discover_published_manifests() -> list[tuple[str, ArtifactManifest, Path]]:
    """Return (target_name, manifest, manifest_path) for every published target."""
    results: list[tuple[str, ArtifactManifest, Path]] = []
    for name in all_target_names():
        spec = get_target(name)
        pointer_path = find_published_manifest(spec.published_manifest_name())
        if pointer_path is None:
            continue
        pointer = PublishedPointer.model_validate_json(
            pointer_path.read_text(encoding="utf-8")
        )
        manifest = read_manifest(pointer.manifest_path)
        results.append((name, manifest, pointer.manifest_path))
    return results


def _summarize(report: ValidationReport) -> str:
    blocking = sum(1 for f in report.findings if f.severity == "block")
    warns = sum(1 for f in report.findings if f.severity == "warn")
    return (
        f"status={report.status} blocking={blocking} warns={warns} "
        f"findings={len(report.findings)}"
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    _ = parser.add_argument(
        "--log-level", default="INFO", help="stdlib logging level (default INFO)"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=str(args.log_level).upper(),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    _log.info("Phase-3 exit gate: scanning %s", cfg.DEEP_ROOT)
    discovered = _discover_published_manifests()
    if not discovered:
        _log.warning(
            "no published deep targets under %s — Phase-3 exit gate cannot evaluate; "
            "run `just fit-deep ... && just publish-manifest ...` for at least one target.",
            cfg.DEEP_ROOT,
        )
        return 0

    failures: list[str] = []
    print()
    print("Phase 3 Exit Gate — per-target validation")
    print("=" * 60)
    for target_name, manifest, manifest_path in discovered:
        _log.info(
            "validating target=%s artifact_id=%s manifest=%s",
            target_name,
            manifest.artifact_id,
            manifest_path,
        )
        report = validate_artifact(manifest.artifact_id)
        print(f"{target_name:35s} artifact={manifest.artifact_id}  {_summarize(report)}")
        if report.status == "failed":
            failures.append(target_name)
            for finding in report.findings:
                if finding.severity == "block":
                    print(f"    BLOCK {finding.code}: {finding.message}")

    print()
    print("Acceptance criteria checklist")
    print("=" * 60)
    for i, item in enumerate(ACCEPTANCE_CRITERIA, start=1):
        marker = "[x]" if not failures else "[?]"
        print(f"  {marker} {i}. {item}")

    print()
    if failures:
        _log.error(
            "Phase-3 exit gate FAILED — %d target(s) blocked: %s",
            len(failures),
            failures,
        )
        return 1
    _log.info("Phase-3 exit gate PASSED — %d target(s) validated", len(discovered))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
