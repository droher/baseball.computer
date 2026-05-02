"""Phase 1.5 Step 3 fixup — wrap jinja-driven model bodies in JINJA_QUERY_BEGIN/END.

After `convert_models_to_sqlmesh.py` runs, the metric wrapper models contain
nothing but a `{{ metric_table_body(...) }}` call where SQLMesh expects a SELECT.
Models with `{% set %}` setup blocks similarly need the body wrapped so SQLMesh
defers parsing until after jinja expansion.

Idempotent — skips files that already have a `JINJA_QUERY_BEGIN` marker.
"""

from __future__ import annotations

import logging
import re
from pathlib import Path

logger = logging.getLogger("wrap_jinja_models")

REPO_ROOT = Path(__file__).resolve().parent.parent
MODELS_DIR = REPO_ROOT / "bc" / "models"

MODEL_BLOCK = re.compile(r"^(MODEL\s*\([^)]*\)\s*;)\s*", re.DOTALL)
JINJA_BODY_MARKER = re.compile(r"\{\{\s*\w|\{%\s*\w")


def _process(text: str) -> str | None:
    if "JINJA_QUERY_BEGIN" in text:
        return None
    m = MODEL_BLOCK.match(text)
    if not m:
        return None
    body = text[m.end():].lstrip("\n")
    if not JINJA_BODY_MARKER.search(body):
        return None
    return f"{m.group(1)}\n\nJINJA_QUERY_BEGIN;\n{body.rstrip()}\nJINJA_END;\n"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    wrapped = 0
    for path in sorted(MODELS_DIR.rglob("*.sql")):
        text = path.read_text()
        new = _process(text)
        if new is None:
            continue
        path.write_text(new)
        logger.info("wrapped %s", path.relative_to(REPO_ROOT))
        wrapped += 1
    logger.info("wrapped=%d", wrapped)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
