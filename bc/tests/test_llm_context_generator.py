"""Unit tests for the LSF-1 generator's pure pieces.

Covers the LSF-1 validator, the supplement loader, and the team-span
collapse behind the team enums. None of them boot SQLMesh or DuckDB.
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


def test_year_accepts_iso_and_us_dates(gen):
    assert gen._year("1915-04-14") == 1915
    assert gen._year("4/26/1995") == 1995
    assert gen._year("") is None
    assert gen._year("unknown") is None


def _franchise_row(**kw):
    base = {
        "franchise_id": "CLE",
        "team_id": "CLE",
        "league": "AL",
        "division": "",
        "location": "Cleveland",
        "nickname": "Indians",
        "alternative_nicknames": "",
        "date_start": "1915-04-14",
        "date_end": "1968-09-27",
        "city": "Cleveland",
        "state": "OH",
    }
    base.update(kw)
    return base


def test_team_spans_merge_division_splits_but_not_renames(gen):
    rows = [
        _franchise_row(nickname="Naps", date_start="1903-04-22", date_end="1914-10-04"),
        _franchise_row(),
        _franchise_row(division="E", date_start="1969-04-08", date_end="1993-10-03"),
        _franchise_row(division="C", date_start="1994-04-04", date_end=""),
    ]
    spans = gen._team_spans(rows)
    assert [(s.nickname, s.start, s.end) for s in spans] == [
        ("Naps", 1903, 1914),
        ("Indians", 1915, None),
    ]


def test_team_enums_carry_names_years_and_franchise(gen):
    rows = [
        _franchise_row(
            franchise_id="WAS",
            team_id="MON",
            league="NL",
            location="Montreal",
            nickname="Expos",
            alternative_nicknames="Nos Amours",
            date_start="1969-04-08",
            date_end="2004-10-03",
        )
    ]
    (e,) = gen._team_enums(rows)
    assert e.value == "MON"
    assert e.meaning == "Montreal Expos, NL, 1969-2004; franchise WAS"
    assert e.aliases == ["Expos", "Montreal Expos", "Nos Amours", "1969-2004"]


def test_verified_query_rejects_clarify_with_sql(gen):
    with pytest.raises(ValueError):
        gen._verified_query(
            {"id": "vq.x", "question": "q", "expect": "clarify", "sql": "select 1"}
        )
    with pytest.raises(ValueError):
        gen._verified_query({"id": "vq.y", "question": "q"})
    vq = gen._verified_query(
        {"id": "vq.z", "question": "q", "expect": "clarify", "notes": "n"}
    )
    assert vq.clarify and vq.sql is None


def test_emit_packet_skips_sql_block_for_clarify(gen):
    supplement = gen.Supplement(
        domain_id="domain.x",
        domain_description="d",
        tz="UTC",
        coverage=None,
        table_synonyms={},
        leagues={},
        ambiguities=[],
        rules=[],
        verified_queries=[
            gen.VerifiedQuery(id="vq.a", question="q", notes="n", sql="select 1"),
            gen.VerifiedQuery(
                id="vq.b", question="q2", notes="n2", sql=None, clarify=True
            ),
        ],
    )
    table = gen.Table(
        table_id="table.t",
        physical_name="s.t",
        grain="one row per id",
        description="d",
        synonyms=[],
        columns=[gen.Column(name="id", type="int", role="PK")],
    )
    packet = gen._emit_packet(
        supplement=supplement,
        tables=[table],
        relationships=[],
        metrics=[],
        enums=[],
        dialect="duckdb",
    )
    assert "SQL|vq.a<<" in packet
    assert "SQL|vq.b<<" not in packet
    assert "VQ|vq.b|q2|Expected response: clarify, not SQL. n2" in packet
    assert gen.validate(packet).violations == []


def test_league_enums_close_span_only_when_every_row_has_ended(gen):
    rows = [
        _franchise_row(league="AL", date_start="1901-04-24", date_end="1960-10-02"),
        _franchise_row(league="AL", date_start="1961-04-10", date_end=""),
        _franchise_row(league="AL", date_start="1969-04-08", date_end="1970-09-30"),
        _franchise_row(league="FL", date_start="1914-04-13", date_end="1914-10-08"),
        _franchise_row(league="FL", date_start="1915-04-10", date_end="1915-10-03"),
        _franchise_row(league="", date_start="1871-05-04", date_end="1871-10-30"),
    ]
    supplement = gen._load_supplement(gen.DEFAULT_SUPPLEMENT)
    by_value = {e.value: e for e in gen._league_enums(rows, supplement)}
    assert set(by_value) == {"AL", "FL", gen.NO_LEAGUE_VALUE}
    assert by_value["AL"].meaning == "American League, 1901-"
    assert by_value["FL"].meaning == "Federal League, 1914-1915"
    assert "Feds" in by_value["FL"].aliases
    assert all(e.column_ref.endswith(".league") for e in by_value.values())


def test_park_enums_carry_place_years_and_other_names(gen):
    rows = [
        ("DEN02", "Coors Field", "", "Denver", "CO", "1995-04-26", None),
        (
            "NYC14",
            "Polo Grounds IV",
            "Brush Stadium;Polo Grounds",
            "New York",
            "NY",
            "1911-06-28",
            "1963-09-18",
        ),
        ("XXX01", "Unknown Grounds", None, None, None, None, None),
    ]
    by_value = {e.value: e for e in gen._park_enums(rows)}
    assert by_value["DEN02"].meaning == "Coors Field, Denver, CO, 1995-"
    assert by_value["DEN02"].aliases == []
    assert by_value["NYC14"].meaning == "Polo Grounds IV, New York, NY, 1911-1963"
    assert by_value["NYC14"].aliases == ["Brush Stadium", "Polo Grounds"]
    assert by_value["XXX01"].meaning == "Unknown Grounds"
