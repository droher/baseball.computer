"""Plain-Python re-export of bc/macros/_docs.py::_doc_dict.

The MODEL-block @doc('key') macro resolves at parse time. Python @model
decorators take plain strings, so we expose the same dict-of-strings
without going through SQLMesh's MacroEvaluator. Same source-of-truth
files (bc/models/**/*.md), same normalization, byte-identical output.
"""

from __future__ import annotations

from macros._docs import _doc_dict


def doc(key: str) -> str:
    """Resolve a doc-block key to its description string.

    Raises KeyError on unknown key — matches @doc()'s behavior.
    """
    docs = _doc_dict()
    if key not in docs:
        raise KeyError(f"unknown doc key {key!r} (have {len(docs)} keys)")
    return docs[key]


def doc_dict() -> dict[str, str]:
    """All resolved doc blocks. Cached upstream."""
    return _doc_dict()
