"""Phase 1.5 Step 3 — bulk-convert dbt models to SQLMesh-native syntax.

Walks `bc/models/**/*.sql`, replaces dbt jinja with SQLMesh-native equivalents,
and prepends a `MODEL(...)` header.

Mechanical rules:

- Strip `{{ config(...) }}` blocks (multi-line).
- Replace `{{ ref('X') }}` → `main_seeds.X` if X starts with `seed_`, else
  `main_models.X`.
- Replace `{{ source('schema', 'table') }}` → `schema.table`.
- Prepend a `MODEL(name main_models.<file_stem>, kind FULL)` block unless
  one already exists.

Idempotent: rerunning is a no-op on already-converted files (detected by the
leading `MODEL (` marker).

One-shot — commit the output, then archive this script under
`scripts/migration/`.
"""

from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path

logger = logging.getLogger("convert_models_to_sqlmesh")

REPO_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = REPO_ROOT / "bc" / "models"

CONFIG_BLOCK = re.compile(r"\{\{\s*config\([^}]*?\)\s*\}\}\s*\n?", re.DOTALL)
REF_PATTERN = re.compile(r"\{\{\s*ref\(\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}")
SOURCE_PATTERN = re.compile(
    r"\{\{\s*source\(\s*['\"]([^'\"]+)['\"]\s*,\s*['\"]([^'\"]+)['\"]\s*\)\s*\}\}"
)
MODEL_HEADER_MARKER = re.compile(r"^\s*MODEL\s*\(", re.MULTILINE)


def _replace_ref(match: re.Match[str]) -> str:
    name = match.group(1)
    schema = "main_seeds" if name.startswith("seed_") else "main_models"
    return f"{schema}.{name}"


def _replace_source(match: re.Match[str]) -> str:
    return f"{match.group(1)}.{match.group(2)}"


def _convert(text: str, model_name: str) -> str:
    if MODEL_HEADER_MARKER.search(text):
        return text  # already converted

    text = CONFIG_BLOCK.sub("", text)
    text = REF_PATTERN.sub(_replace_ref, text)
    text = SOURCE_PATTERN.sub(_replace_source, text)
    text = text.lstrip("\n")

    header = f"MODEL (\n  name main_models.{model_name},\n  kind FULL,\n);\n\n"
    return header + text


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Print changes without writing.",
    )
    ap.add_argument(
        "--only",
        nargs="*",
        help="Limit to model file stems (e.g. --only stg_events game_results).",
    )
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    paths = sorted(MODELS_DIR.rglob("*.sql"))
    converted = 0
    skipped = 0
    for path in paths:
        stem = path.stem
        if args.only and stem not in args.only:
            continue
        original = path.read_text()
        new = _convert(original, stem)
        if new == original:
            skipped += 1
            continue
        if args.dry_run:
            logger.info("[dry-run] would convert %s", path.relative_to(REPO_ROOT))
        else:
            path.write_text(new)
            logger.info("converted %s", path.relative_to(REPO_ROOT))
        converted += 1

    logger.info("converted=%d skipped=%d total=%d", converted, skipped, converted + skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
