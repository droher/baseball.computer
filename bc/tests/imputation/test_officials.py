from __future__ import annotations

import duckdb

from python_models.imputation.context import ContextCompletionConfig
from python_models.imputation.officials import (
    OFFICIAL_ROLES,
    build_officials_completion_sql,
)


def test_candidates_preserve_identity_and_do_not_invent_historical_people() -> None:
    with duckdb.connect() as connection:
        connection.execute("CREATE SCHEMA main_models")
        fields = ", ".join(f"{role} VARCHAR" for role in OFFICIAL_ROLES)
        connection.execute(
            "CREATE TABLE main_models.game_start_info (game_id VARCHAR, season SMALLINT, "
            "home_league VARCHAR, away_league VARCHAR, game_type VARCHAR, source_type VARCHAR, "
            + fields
            + ")"
        )
        connection.execute(
            "INSERT INTO main_models.game_start_info VALUES "
            "('early',1903,'NL','NL','RegularSeason','PlayByPlay',NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
            "('a',1950,'NL','NL','RegularSeason','PlayByPlay','scorer','ump',NULL,NULL,NULL,NULL,NULL),"
            "('b',1950,'NL','NL','RegularSeason','PlayByPlay',NULL,NULL,NULL,NULL,NULL,NULL,NULL),"
            "('box',1903,'NL','NL','RegularSeason','BoxScore','wrong','wrong',NULL,NULL,NULL,NULL,NULL)"
        )
        result = connection.execute(
            build_officials_completion_sql(ContextCompletionConfig())
        ).fetchdf()
        assert set(result.game_id) == {"early", "a", "b"}
        assert not result.duplicated(["game_id", "role"]).any()
        early = result[result.game_id == "early"]
        assert all(len(values) == 0 for values in early.candidate_identities)
        observed = result[
            (result.game_id == "a") & (result.role == "umpire_home_id")
        ].iloc[0]
        assert observed.recorded_identity == "ump"
        assert list(observed.candidate_identities) == ["ump"]
        estimated = result[
            (result.game_id == "b") & (result.role == "umpire_home_id")
        ].iloc[0]
        assert list(estimated.candidate_identities) == ["ump"]
        assert sum(estimated.candidate_probabilities) == 1
        assert estimated.identity_status == "unresolved_participant_slot"
        absent = result[
            (result.game_id == "b") & (result.role == "umpire_left_id")
        ].iloc[0]
        assert absent.role_presence_status == "absence_or_unrecorded_presence_unknown"
