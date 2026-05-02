"""Custom jinja builtins module for SQLMesh's `JinjaMacroRegistry`.

SQLMesh's native jinja env (`sqlmesh.utils.jinja.create_builtin_globals`)
intentionally does *not* include dbt's `return()` builtin — but our existing
macros (`stat_lists.sql`, `metric_col_lists.sql`, `metric_calcs.sql`) all use
`{{ return([...]) }}` to surface Python lists/dicts to caller templates.

This module wraps SQLMesh's default factory and adds `return` (along with
the small set of dbt builtins our macros actually use). It is wired in via
`bc.loader.BcSqlMeshLoader`, which sets `JinjaMacroRegistry.create_builtins_module`
to `"jinja_globals"` after the loader builds the registry.

If we ever delete the `return()` macros (e.g. by porting `stat_lists` to a
Python-based registry), this module can go away too.
"""

from __future__ import annotations

from typing import Any

from sqlmesh.utils.jinja import (
    JinjaMacroRegistry,
    MacroReturnVal,
    create_builtin_globals as sqlmesh_create_builtin_globals,
)


def return_val(val: Any) -> None:
    """Local copy of `sqlmesh.dbt.builtin.return_val`.

    Reimplemented here because `sqlmesh.dbt.builtin` imports `agate` at module
    load — a heavyweight dbt-only dependency we don't want to keep in the
    Phase 1.5 native runtime. The exception class itself lives in
    `sqlmesh.utils.jinja` (no agate dependency) and is what `_MacroWrapper`
    catches, so re-raising the same class preserves dbt-`return()` semantics.
    """
    raise MacroReturnVal(val)


def _ref(name: str) -> str:
    """Emit `main_seeds.<name>` for seeds, `main_models.<name>` for everything else.

    Mirrors the textual substitution that `scripts/convert_models_to_sqlmesh.py`
    applies inline; macros that still call `{{ ref(...) }}` (currently the
    metric/stat helper macros under `bc/macros/`) need the same behavior at
    render time.
    """
    schema = "main_seeds" if name.startswith("seed_") else "main_models"
    return f"{schema}.{name}"


def _source(schema: str, table: str) -> str:
    return f"{schema}.{table}"


class _Exceptions:
    """dbt-style `exceptions.raise_compiler_error` shim."""

    @staticmethod
    def raise_compiler_error(msg: str) -> None:
        raise RuntimeError(f"Compiler error: {msg}")


def create_builtin_globals(
    jinja_macros: JinjaMacroRegistry,
    global_vars: dict[str, Any],
    *args: Any,
    **kwargs: Any,
) -> dict[str, Any]:
    """Augment SQLMesh's jinja globals with the dbt-isms our macros use."""
    base = sqlmesh_create_builtin_globals(jinja_macros, global_vars, *args, **kwargs)
    base.setdefault("return", return_val)
    base.setdefault("ref", _ref)
    base.setdefault("source", _source)
    base.setdefault("exceptions", _Exceptions())
    return base
