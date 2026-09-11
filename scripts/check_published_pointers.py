"""Fail fast when the estimated-model artifact pointers are unreachable.

The coverage ``@model``s materialize typed empty frames whenever no
published pointer resolves, so a build on a checkout without
``artifacts/`` silently publishes empty estimated tables. Run this before
``sqlmesh plan`` on the CI runner: it requires ``BC_STATS_ARTIFACTS_ROOT``
to name the canonical artifacts directory, at least one pointer under its
``published/`` subdirectory, and every pointer's manifest to exist.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT / "bc"))

from python_models.statistical import config as cfg  # noqa: E402
from python_models.statistical.manifests import read_published_pointer  # noqa: E402

_log = logging.getLogger("check_published_pointers")


def artifacts_root_from_env(environ: dict[str, str]) -> Path:
    raw = environ.get(cfg.ENV_ARTIFACTS_ROOT, "").strip()
    if not raw:
        raise SystemExit(
            f"{cfg.ENV_ARTIFACTS_ROOT} is unset; point it at the canonical"
            + " artifacts/statistical directory so published pointers resolve."
        )
    root = Path(raw)
    if not root.is_absolute():
        raise SystemExit(f"{cfg.ENV_ARTIFACTS_ROOT} must be absolute, got {raw!r}")
    if not root.is_dir():
        raise SystemExit(f"{cfg.ENV_ARTIFACTS_ROOT}={raw!r} is not a directory")
    return root


def check_pointers(published_root: Path, *, verify_evidence: bool = True) -> list[Path]:
    """Return the manifest paths behind every pointer under ``published_root``.

    Raises ``SystemExit`` when there are no pointers or any manifest is missing.
    """
    pointer_paths = sorted(published_root.rglob("*.json"))
    if not pointer_paths:
        raise SystemExit(f"no published pointers under {published_root}")
    missing: list[str] = []
    manifests: list[Path] = []
    for pointer_path in pointer_paths:
        pointer = read_published_pointer(pointer_path)
        manifests.append(pointer.manifest_path)
        if pointer.manifest_path.is_file():
            if verify_evidence:
                from python_models.statistical.publication_evidence import (
                    verify_published_evidence,
                )

                if pointer.publication_mode is None:
                    raise SystemExit(
                        f"pointer has no publication evidence policy: {pointer_path}"
                    )
                if (
                    pointer.publication_mode == "exploratory"
                    and not (pointer.notes or "").strip()
                ):
                    raise SystemExit(
                        f"exploratory pointer has no reason: {pointer_path}"
                    )
                root = published_root.parent
                try:
                    verify_published_evidence(
                        pointer.manifest_path,
                        expected_binding=pointer.validation_binding,
                        expected_artifact_id=pointer.artifact_id,
                        candidate_roots=tuple(
                            root / name for name in ("deep", "bayes", "datasets", "eda")
                        ),
                        exploratory=pointer.publication_mode == "exploratory",
                    )
                except (ValueError, OSError) as exc:
                    raise SystemExit(
                        f"invalid publication evidence for {pointer_path}: {exc}"
                    ) from exc
            _log.info(
                "pointer %s -> %s (%s)",
                pointer.model_name,
                pointer.artifact_id,
                pointer.manifest_path,
            )
        else:
            missing.append(f"{pointer_path.name}: {pointer.manifest_path}")
    if missing:
        listing = "\n  ".join(missing)
        raise SystemExit(
            f"published pointers whose manifest does not exist:\n  {listing}"
        )
    return manifests


def main() -> int:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    root = artifacts_root_from_env(dict(os.environ))
    manifests = check_pointers(cfg.resolve_global_published_root())
    _log.info("%d published pointers resolve under %s", len(manifests), root)
    return 0


if __name__ == "__main__":
    sys.exit(main())
