"""Unit tests for the LSF-1 generator's pure pieces.

Covers the lambda → SQL-text renderer and the LSF-1 validator. Both
run without booting SQLMesh or DuckDB so the tests stay fast.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT_PATH = REPO_ROOT / "scripts" / "generate_llm_context.py"


def _load_module():
    spec = importlib.util.spec_from_file_location("_llm_ctx_gen", SCRIPT_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["_llm_ctx_gen"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture(scope="module")
def gen():
    return _load_module()


def test_render_lambda_simple_aggregate(gen):
    fn = lambda t: t.hits.sum()  # noqa: E731
    assert gen._render_lambda(fn, scope_var="t") == "sum(hits)"


def test_render_lambda_arithmetic_inside_sum(gen):
    fn = lambda t: (t.total_bases - t.hits).sum()  # noqa: E731
    assert gen._render_lambda(fn, scope_var="t") == "sum(total_bases - hits)"


def test_render_lambda_derived_uses_measure_names(gen):
    fn = lambda m: m.on_base_percentage + m.slugging_percentage  # noqa: E731
    assert (
        gen._render_lambda(fn, scope_var="m")
        == "(on_base_percentage + slugging_percentage)"
    )


def test_render_lambda_subscript_with_default_arg(gen):
    a = "left"
    fn = lambda t, _a=a: (t[f"batted_angle_{_a}"] * t.at_bats).sum()  # noqa: E731
    assert gen._render_lambda(fn, scope_var="t") == "sum(batted_angle_left * at_bats)"


def test_render_lambda_constant_string(gen):
    fn = lambda t: t["hits"].sum()  # noqa: E731
    assert gen._render_lambda(fn, scope_var="t") == "sum(hits)"


def test_render_lambda_unsupported_node_raises(gen):
    fn = lambda t: [c for c in t.cols]  # noqa: E731
    with pytest.raises(gen._MetricExprUnrenderable):
        gen._render_lambda(fn, scope_var="t")


def test_split_record_passes_through_escaped_semicolon(gen):
    line = r"COL|table.x|name|string|DIM|-|a desc with \; semis|-|-"
    fields = gen._split_record(line)
    # The desc field MUST still contain the literal "\;" so the list-decoder
    # downstream can treat it correctly. Per spec §6.8.
    assert fields[6] == r"a desc with \; semis"


def test_list_items_unescapes_semicolon(gen):
    items = gen._list_items(r"a;b\;still-b;c")
    assert items == ["a", "b;still-b", "c"]


def test_validator_clean_minimal_packet(gen):
    packet = (
        '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
        "DOMAIN|domain.t|test\n"
        "TABLES\n"
        "TABLE|table.a|s.a|one row per id|test|-\n"
        "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
        "COL|table.a|id|int|PK|-|-|-|-\n"
        "RELATIONSHIPS\n"
        "</DB_CONTEXT>\n"
    )
    report = gen.validate(packet)
    assert report.violations == []
    assert report.counts["tables"] == 1
    assert report.counts["cols"] == 1


def test_validator_catches_table_with_no_columns(gen):
    packet = (
        '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
        "DOMAIN|domain.t|test\n"
        "TABLES\n"
        "TABLE|table.a|s.a|one row per id|test|-\n"
        "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
        "RELATIONSHIPS\n"
        "</DB_CONTEXT>\n"
    )
    report = gen.validate(packet)
    assert any("v11" in v and "table.a" in v for v in report.violations), (
        f"expected v11 violation, got {report.violations}"
    )


def test_validator_catches_dangling_fk_ref(gen):
    packet = (
        '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
        "DOMAIN|domain.t|test\n"
        "TABLES\n"
        "TABLE|table.a|s.a|one row per id|test|-\n"
        "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
        "COL|table.a|id|int|PK|-|-|-|-\n"
        "COL|table.a|other_id|int|FK|table.missing.id|-|-|-\n"
        "RELATIONSHIPS\n"
        "</DB_CONTEXT>\n"
    )
    report = gen.validate(packet)
    assert any("v13" in v for v in report.violations)


def test_validator_catches_orphan_sql_block(gen):
    packet = (
        '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
        "DOMAIN|domain.t|test\n"
        "TABLES\n"
        "TABLE|table.a|s.a|one row per id|test|-\n"
        "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
        "COL|table.a|id|int|PK|-|-|-|-\n"
        "RELATIONSHIPS\n"
        "VERIFIED_QUERIES\n"
        "SQL|vq.no_matching_vq<<\n"
        "select 1\n"
        ">>SQL\n"
        "</DB_CONTEXT>\n"
    )
    report = gen.validate(packet)
    assert any("v20" in v for v in report.violations)


def test_validator_skips_records_inside_sql_block(gen):
    """A line starting with WARN| inside a SQL block must not be counted."""
    packet = (
        '<DB_CONTEXT version="LSF-1" dialect="duckdb">\n'
        "DOMAIN|domain.t|test\n"
        "TABLES\n"
        "TABLE|table.a|s.a|one row per id|test|-\n"
        "COLS|table.a|name|type|role|ref|desc|examples|tags\n"
        "COL|table.a|id|int|PK|-|-|-|-\n"
        "RELATIONSHIPS\n"
        "VERIFIED_QUERIES\n"
        "VQ|vq.t|q|-\n"
        "SQL|vq.t<<\n"
        "WARN|table.a|this is fake; should be ignored as a record\n"
        ">>SQL\n"
        "</DB_CONTEXT>\n"
    )
    report = gen.validate(packet)
    # That fake WARN line must not have been counted as a real WARN record
    assert report.counts["warns"] == 0
    assert report.counts["verified_queries"] == 1
