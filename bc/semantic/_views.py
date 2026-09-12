"""SQL for the six semantic views published into the DuckLake catalog.

Each view is the SQL twin of the ibis backing expression in
``_tables_common``: ``season_with_league`` with its regular-season
filter for the season grain, ``event_with_game_info`` for the event
grain. Table references are schema-qualified without a catalog name so
the views bind under whatever alias a consumer attaches the catalog as.
"""

from __future__ import annotations

from python_models.metrics._constants import GAME_COLS

from ._tables_common import MetricGrain, MetricKind, backing_model_name

SEMANTIC_SCHEMA = "semantic"

SEMANTIC_VIEWS: tuple[tuple[str, MetricKind, MetricGrain], ...] = (
    ("offense_seasons", "offense", "season"),
    ("offense_events", "offense", "event"),
    ("pitching_seasons", "pitching", "season"),
    ("pitching_events", "pitching", "event"),
    ("fielding_seasons", "fielding", "season"),
    ("fielding_events", "fielding", "event"),
)


def semantic_view_name(kind: MetricKind, grain: MetricGrain) -> str:
    return f"{kind}_{grain}s"


def semantic_view_sql(kind: MetricKind, grain: MetricGrain) -> str:
    """SELECT body of one semantic view over the published main_models tables."""
    model = backing_model_name(kind, grain)
    if grain == "season":
        return (
            "SELECT s.*, coalesce(f.league, 'N/A') AS league\n"
            f"FROM {model} AS s\n"
            "LEFT JOIN main_seeds.seed_franchises AS f\n"
            "  ON s.team_id = f.team_id\n"
            " AND s.season >= year(f.date_start)\n"
            " AND s.season <= coalesce(year(f.date_end), 9999)\n"
            "WHERE s.game_type IN (\n"
            "  SELECT game_type FROM main_seeds.seed_game_types WHERE is_regular_season\n"
            ")"
        )
    game_cols = ",\n       ".join(f"g.{c}" for c in GAME_COLS)
    return (
        "SELECT e.*,\n"
        f"       {game_cols}\n"
        f"FROM {model} AS e\n"
        "LEFT JOIN main_models.team_game_start_info AS g\n"
        "  ON e.team_id = g.team_id\n"
        " AND e.game_id = g.game_id"
    )
