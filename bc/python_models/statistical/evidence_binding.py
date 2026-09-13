from __future__ import annotations

import hashlib
import json
import logging
import re
from pathlib import Path
from typing import cast

from pydantic import BaseModel

from python_models.statistical.manifests import read_manifest
from python_models.statistical.dataset_provenance import hash_parquet_schema
from python_models.statistical.schemas import DatasetMetadata
from python_models.statistical.geometry_contract import (
    DATASET_VERSIONS,
    require_geometry_parquet,
)

_log = logging.getLogger(__name__)


class EvidenceBinding(BaseModel):
    digest: str
    manifests: tuple[Path, ...]
    missing_provenance: tuple[str, ...]


def file_digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _manifest_digest(path: Path) -> str:
    raw: object = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict):
        raise ValueError(f"manifest must be a JSON object: {path}")
    payload = cast(dict[str, object], raw)
    for key in (
        "validation_status",
        "validation_gate_version",
        "validated_at",
        "validation_evidence",
        "validation_binding",
        "blocking_findings",
        "publication_mode",
    ):
        payload.pop(key, None)
    extras = payload.get("bayes_extras")
    if isinstance(extras, dict):
        extras = dict(cast(dict[str, object], extras))
        extras.pop("diagnostics_summary", None)
        extras.pop("weak_identification_flag", None)
        payload["bayes_extras"] = extras
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def bind_artifact(
    manifest_path: Path,
    *,
    candidate_roots: tuple[Path, ...] = (),
) -> EvidenceBinding:
    fingerprints: dict[str, str] = {}
    visited: dict[Path, str] = {}
    active: set[Path] = set()
    missing: list[str] = []

    def visit(path: Path) -> None:
        path = path.resolve()
        if path in active:
            raise ValueError(f"artifact dependency cycle at {path}")
        if path in visited:
            return
        active.add(path)
        manifest = read_manifest(path)
        identity = f"{manifest.kind}/{manifest.name}/{manifest.artifact_id}"
        if identity in visited.values():
            raise ValueError(f"duplicate artifact identity: {identity}")
        visited[path] = identity
        fingerprints[f"{identity}/manifest.json"] = _manifest_digest(path)
        if manifest.kind == "dataset":
            required_version = DATASET_VERSIONS.get(manifest.name)
            if required_version is not None and manifest.version != required_version:
                missing.append(
                    f"{identity}: obsolete geometry target contract; "
                    f"requires dataset version {required_version}"
                )
            for field in (
                "content_hash",
                "schema_hash",
                "transformation_hash",
                "dependency_hash",
            ):
                if not getattr(manifest, field):
                    missing.append(f"{identity}: missing {field}")
                elif re.fullmatch(r"[0-9a-f]{64}", getattr(manifest, field)) is None:
                    raise ValueError(f"invalid SHA-256 {field}: {identity}")
        elif manifest.kind in {"deep", "bayes", "pretrain"}:
            if not manifest.dataset_artifact_id:
                missing.append(f"{identity}: no dataset identity")
            if not manifest.input_manifests:
                missing.append(f"{identity}: no explicit dependency manifests")
            if (
                manifest.kind in {"deep", "pretrain"}
                and "pretraining" not in manifest.metadata
            ):
                missing.append(f"{identity}: pretraining dependency disposition absent")

        files = {
            p.resolve()
            for p in path.parent.rglob("*")
            if p.is_file()
            and not any(
                part.startswith(".") for part in p.relative_to(path.parent).parts
            )
            and p != path
            and p != path.parent / "validation" / "validation_report.json"
        }
        for output in manifest.output_paths.values():
            resolved = output if output.is_absolute() else path.parent / output
            if not resolved.exists():
                missing.append(f"{identity}: artifact output missing: {resolved}")
                continue
            if resolved.is_file():
                files.add(resolved.resolve())
        for file in sorted(files):
            try:
                label = str(file.relative_to(path.parent))
            except ValueError:
                label = str(file)
            fingerprints[f"{identity}/{label}"] = file_digest(file)
        if manifest.kind == "dataset" and manifest.content_hash:
            dataset = manifest.output_paths.get("dataset")
            if dataset is None:
                raise ValueError(f"dataset output is undeclared: {identity}")
            dataset_path = (
                dataset if dataset.is_absolute() else path.parent / dataset
            ).resolve()
            try:
                dataset_label = str(dataset_path.relative_to(path.parent))
            except ValueError:
                dataset_label = str(dataset_path)
            if fingerprints[f"{identity}/{dataset_label}"] != manifest.content_hash:
                raise ValueError(
                    f"dataset content differs from its recorded hash: {identity}"
                )

            if (
                manifest.name in DATASET_VERSIONS
                and manifest.version == DATASET_VERSIONS[manifest.name]
            ):
                require_geometry_parquet(
                    dataset_path,
                    dimension_column=(
                        "geometry_dimension"
                        if manifest.name == "model_input_geometry"
                        else "dimension"
                    ),
                    check_classes=manifest.name == "model_input_geometry",
                )

            if manifest.schema_hash != hash_parquet_schema(dataset_path):
                raise ValueError(
                    f"dataset schema differs from its recorded hash: {identity}"
                )
            metadata_path = path.with_name("dataset_metadata.json")
            if not metadata_path.is_file():
                missing.append(f"{identity}: dataset metadata missing")
            else:
                metadata = DatasetMetadata.model_validate_json(
                    metadata_path.read_text()
                )
                for field in (
                    "content_hash",
                    "schema_hash",
                    "transformation_hash",
                    "dependency_hash",
                ):
                    if getattr(manifest, field) != getattr(metadata, field):
                        raise ValueError(
                            f"dataset manifest and metadata disagree on {field}: {identity}"
                        )

        dependencies: list[Path] = [
            p if p.is_absolute() else path.parent / p for p in manifest.input_manifests
        ]
        declared = set(manifest.input_artifact_ids)
        if manifest.dataset_artifact_id:
            declared.add(manifest.dataset_artifact_id)
        explicit_ids = {read_manifest(p).artifact_id for p in dependencies}
        extras = manifest.bayes_extras
        if (
            extras is not None
            and (
                (
                    extras.propensity_active
                    and extras.gamma_propensity_flavor != "gamma_propensity_zero"
                )
                or manifest.metadata.get("handler_active") is True
                or extras.gamma_dl_flavor == "gamma_dl_shrunk"
                or extras.model_name == "assist_credit_allocation"
            )
            and not (explicit_ids - {manifest.dataset_artifact_id})
        ):
            missing.append(
                f"{identity}: active fitted covariate or marginalization dependency is unrecorded"
            )
        for artifact_id in sorted(declared - explicit_ids):
            matches = {
                p.resolve()
                for root in candidate_roots
                for p in root.glob(f"*/{artifact_id}/manifest.json")
            }
            if len(matches) != 1:
                missing.append(
                    f"{identity}: dependency {artifact_id} has {len(matches)} matches"
                )
                continue
            dependencies.extend(matches)
        for dependency in dependencies:
            visit(dependency)
        active.remove(path)

    visit(manifest_path)
    digest = hashlib.sha256(
        json.dumps(fingerprints, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    _log.info(
        "bound %d artifacts and %d files; %d provenance gaps",
        len(visited),
        len(fingerprints),
        len(missing),
    )
    return EvidenceBinding(
        digest=digest, manifests=tuple(visited), missing_provenance=tuple(missing)
    )
