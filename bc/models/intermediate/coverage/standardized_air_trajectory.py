"""Per-event standardized launch-angle band probabilities for every directly
recorded airborne trajectory label in a season the translation covers."""

from __future__ import annotations

from sqlglot import exp
from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models._doc_lookup import doc
from python_models.statistical.air_trajectory_translation import (
    build_standardized_sql,
)

_GRAIN = ("event_key", "standardized_air_subtype")

_AUDITS: list[tuple[str, dict[str, object]]] = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    *(exp.column(name) for name in _GRAIN),
                    exp.column("season"),
                    exp.column("recorded_air_subtype"),
                    exp.column("result_family"),
                    exp.column("expected_share"),
                    exp.column("partially_identified"),
                ]
            ),
        },
    ),
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=[exp.column(name) for name in _GRAIN])},
    ),
    ("estimated_contract_complete", {}),
    ("min_row_count", {"threshold": 1000000}),
]


@model(
    "main_models.standardized_air_trajectory",
    is_sql=True,
    kind="FULL",
    columns={
        "event_key": "UINTEGER",
        "season": "SMALLINT",
        "recorded_air_subtype": "VARCHAR",
        "result_family": "VARCHAR",
        "standardized_air_subtype": "VARCHAR",
        "expected_share": "DOUBLE",
        "share_lower_95": "DOUBLE",
        "share_upper_95": "DOUBLE",
        "basis": "VARCHAR",
        "cell_status": "VARCHAR",
        "partially_identified": "BOOLEAN",
        "artifact_id": "VARCHAR",
        "model_name": "VARCHAR",
        "model_version": "VARCHAR",
        "source_snapshot_id": "VARCHAR",
        "method": "VARCHAR",
        "observed_status": "VARCHAR",
        "confidence_status": "VARCHAR",
        "weak_identification_flag": "BOOLEAN",
    },
    column_descriptions={
        "event_key": doc("event_key"),
        "season": doc("season"),
        "recorded_air_subtype": "The directly recorded airborne label (Fly, LineDrive, PopUp) from event_observation_geometry.",
        "result_family": "Result family from event_observation_context, one of the five the translation conditions on.",
        "standardized_air_subtype": "Standardized launch-angle band: Fly, LineDrive, or PopUp.",
        "expected_share": "Probability that the event falls in this band given its recorded label, result family, and season. The three rows of an event sum to 1.",
        "share_lower_95": "Lower end of the nominal 95 percent interval of the translation cell.",
        "share_upper_95": "Upper end of the nominal 95 percent interval of the translation cell.",
        "basis": "Basis of the season's translation; see air_trajectory_translation.",
        "cell_status": "posterior_cell or prior_only_cell; see air_trajectory_translation.",
        "partially_identified": "TRUE for seasons before 2009.",
        "weak_identification_flag": "Same as partially_identified.",
    },
    grain=list(_GRAIN),
    audits=_AUDITS,
    description=(
        "Per-event probabilities over the standardized launch-angle bands for "
        "every directly recorded Fly, LineDrive, or PopUp label in 1989 to 2025, "
        "from air_trajectory_translation joined on season, recorded label, and "
        "result family, for every game type although the translation was fit on "
        "regular-season games. Ground balls keep their recorded label and are not "
        "included; bunts are not included. Events whose result family is outside "
        "the five translated families, or whose season is before 1989, have no rows."
    ),
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    del evaluator
    return build_standardized_sql()
