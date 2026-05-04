"""sqlglot DataType helpers for project ENUMs.

SQLMesh's @model decorator's columns dict accepts SQL strings for known
types but rejects user-defined ENUMs ("No expression was parsed from
'PARK_ID'"). Build them as exp.DataType(udt=True) instances instead.
"""

from __future__ import annotations

from sqlglot import exp

_DIALECT = "duckdb"


def udt(name: str) -> exp.DataType:
    """User-defined type — used for project ENUMs registered via create_enums()."""
    return exp.DataType.build(name, udt=True, dialect=_DIALECT)


PARK_ID = udt("PARK_ID")
TEAM_ID = udt("TEAM_ID")
GAME_ID = udt("GAME_ID")
PLAYER_ID = udt("PLAYER_ID")
HAND = udt("HAND")
