from __future__ import annotations

import importlib.util
import uuid
from pathlib import Path
from typing import Any

import yaml

from scripts.build_retrosheet_candidate import SOURCE_SCHEMAS

BC_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = BC_DIR.parent


def load(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location(
        f"source_schemas_{uuid.uuid4().hex}", path
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def external_schemas() -> set[str]:
    entries = yaml.safe_load((BC_DIR / "external_models.yaml").read_text())
    return {entry["name"].split(".", 1)[0] for entry in entries}


def test_every_external_schema_has_a_source_root_everywhere_roots_are_listed() -> None:
    schemas = external_schemas()

    assert set(load(BC_DIR / "config.py")._DEFAULT_SOURCE_ROOTS) == schemas
    assert (
        set(load(REPO_ROOT / "scripts" / "preload_sources.py").DEFAULT_SOURCE_ROOTS)
        == schemas
    )
    assert set(SOURCE_SCHEMAS) == schemas
