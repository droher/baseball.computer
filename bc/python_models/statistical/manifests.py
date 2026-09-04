"""Artifact ID generation, manifest read/write, published pointer resolution."""

from __future__ import annotations

import logging
import os
import tempfile
import uuid
from datetime import datetime, timezone
from pathlib import Path

from python_models.statistical.config import (
    resolve_artifact_root,
    resolve_published_roots,
)
from python_models.statistical.schemas import (
    ArtifactManifest,
    PublishedPointer,
)

_log = logging.getLogger(__name__)


def new_artifact_id() -> str:
    return str(uuid.uuid4())


def utc_now() -> datetime:
    return datetime.now(tz=timezone.utc)


def write_manifest(manifest: ArtifactManifest, path: Path) -> None:
    """Atomic write of an ``ArtifactManifest`` as JSON.

    Renames a same-directory temp file so partial writes never appear
    on disk.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = manifest.model_dump_json(indent=2)
    fd, tmp_name = tempfile.mkstemp(
        dir=str(path.parent), prefix=".manifest.", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(payload)
        os.replace(tmp_name, path)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise


def read_manifest(path: Path) -> ArtifactManifest:
    return ArtifactManifest.model_validate_json(path.read_text(encoding="utf-8"))


def find_published_manifest(model_name: str) -> Path | None:
    branch_root, global_root = resolve_published_roots()
    branch_path = branch_root / f"{model_name}.json"
    if branch_path.exists():
        return branch_path
    global_path = global_root / f"{model_name}.json"
    if global_path.exists():
        return global_path
    return None


PRETRAIN_POINTER_SUBDIR: str = "pretrain"


def find_published_pretrain(name: str) -> Path | None:
    """Locate a published pretrain pointer JSON by name.

    Mirrors ``find_published_manifest`` but resolves under the
    ``pretrain/`` subdir of the published root.
    """
    branch_root, global_root = resolve_published_roots()
    branch_path = branch_root / PRETRAIN_POINTER_SUBDIR / f"{name}.json"
    if branch_path.exists():
        return branch_path
    global_path = global_root / PRETRAIN_POINTER_SUBDIR / f"{name}.json"
    if global_path.exists():
        return global_path
    return None


def resolve_pointer_manifest_path(manifest_path: Path) -> Path:
    """Absolute manifest path for a pointer's stored ``manifest_path``.

    Absolute values are returned unchanged. Relative values resolve
    against the artifacts root so a pointer written on one machine or
    checkout keeps working on another.
    """
    if manifest_path.is_absolute():
        return manifest_path
    return resolve_artifact_root() / manifest_path


def relative_pointer_manifest_path(manifest_path: Path) -> Path:
    """``manifest_path`` relative to the artifacts root.

    Raises ``ValueError`` when the manifest lives outside the root, since
    such a pointer could not be resolved back.
    """
    root = resolve_artifact_root().resolve()
    try:
        return manifest_path.resolve().relative_to(root)
    except ValueError as exc:
        raise ValueError(
            f"manifest {manifest_path} is outside the artifacts root {root};"
            + " cannot write a relative pointer"
        ) from exc


def write_published_pointer(
    pointer: PublishedPointer,
    *,
    root: Path | None = None,
    relative: bool = False,
) -> Path:
    if relative:
        pointer = pointer.model_copy(
            update={
                "manifest_path": relative_pointer_manifest_path(pointer.manifest_path)
            }
        )
    branch_root, _ = resolve_published_roots()
    target_root = root if root is not None else branch_root
    target_root.mkdir(parents=True, exist_ok=True)
    target = target_root / f"{pointer.model_name}.json"
    fd, tmp_name = tempfile.mkstemp(
        dir=str(target_root), prefix=".pointer.", suffix=".json"
    )
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(pointer.model_dump_json(indent=2))
        os.replace(tmp_name, target)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except FileNotFoundError:
            pass
        raise
    return target


def read_published_pointer(path: Path) -> PublishedPointer:
    pointer = PublishedPointer.model_validate_json(path.read_text(encoding="utf-8"))
    return pointer.model_copy(
        update={"manifest_path": resolve_pointer_manifest_path(pointer.manifest_path)}
    )


def package_versions() -> dict[str, str]:
    """Snapshot installed versions for manifest provenance."""
    import importlib.metadata as md

    names = (
        "pymc",
        "arviz",
        "xarray",
        "zarr",
        "scikit-learn",
        "duckdb",
        "polars",
        "pydantic",
    )
    out: dict[str, str] = {}
    for name in names:
        try:
            out[name] = md.version(name)
        except md.PackageNotFoundError:
            continue
    return out


def query_hash(query_text: str) -> str:
    import hashlib

    return hashlib.sha256(query_text.encode("utf-8")).hexdigest()


def stable_model_version(major: int, minor: int, patch: int) -> str:
    return f"{major}.{minor}.{patch}"


def new_run_id() -> str:
    return f"run-{utc_now().strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:8]}"
