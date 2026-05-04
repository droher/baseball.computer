"""Resolve shared docstrings from `bc/models/**/*.md` doc blocks.

Re-DRYs the 374 dbt-style `{{ doc('key') }}` references that the YAML
port inlined. Use as `@doc('key')` inside MODEL block fields:

    MODEL (
      ...
      column_descriptions (
        event_key = @doc('event_key'),
        game_id = @doc('game_id'),
      )
    )

SQLMesh's `render_meta_fields` (.../sqlmesh/core/model/definition.py)
parses any meta-field string containing `@` as a macro expression and
renders it before validation; the macro returns a SQL literal whose
`.name` becomes the description string.
"""

from __future__ import annotations

import functools
import logging
import re
from pathlib import Path
from typing import Any

from sqlglot import exp
from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator


def _logger() -> logging.Logger:
    return logging.getLogger(__name__)


@functools.cache
def _doc_dict() -> dict[str, str]:
    """Parse every {% docs key %}...{% enddocs %} block under bc/models.

    Compiled regexes and Path objects stay function-local — SQLMesh's
    `make_python_env` serializer captures module globals through closures,
    and `re.Pattern` / `Path` don't survive that round-trip.
    """
    block_re = re.compile(
        r"\{%\s*docs\s+(\w+)\s*%\}(.*?)\{%\s*enddocs\s*%\}", re.DOTALL
    )
    ws_re = re.compile(r"\s+")
    models_dir = Path(__file__).resolve().parent.parent / "models"
    out: dict[str, str] = {}
    for md_path in sorted(models_dir.rglob("*.md")):
        for m in block_re.finditer(md_path.read_text()):
            key = m.group(1)
            body = m.group(2)
            cleaned = " ".join(line.strip() for line in body.strip().splitlines())
            text = ws_re.sub(" ", cleaned).strip()
            if key in out and out[key] != text:
                raise RuntimeError(
                    f"doc key {key!r} redefined with conflicting body in {md_path}"
                )
            out[key] = text
    return out


@macro()
def doc(evaluator: MacroEvaluator, key: Any) -> exp.Literal:  # noqa: ARG001
    """Return the `key` doc block as a SQL string literal."""
    name = key.name if isinstance(key, exp.Expr) else str(key)
    docs = _doc_dict()
    if name not in docs:
        raise KeyError(f"unknown doc key {name!r} (have {len(docs)} keys)")
    return exp.Literal.string(docs[name])
