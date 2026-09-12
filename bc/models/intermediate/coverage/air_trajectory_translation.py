"""Season-by-season translation from recorded airborne trajectory labels to
standardized launch-angle bands, with the estimated-metadata contract."""

from __future__ import annotations

from sqlglot import exp
from sqlmesh import model
from sqlmesh.core.macros import MacroEvaluator

from python_models._doc_lookup import doc
from python_models.statistical.air_trajectory_translation import (
    BASES,
    CELL_STATUSES,
    RECORDED_AIR_SUBTYPES,
    RESULT_FAMILIES,
    STANDARDIZED_AIR_SUBTYPES,
    build_translation_sql,
)

_GRAIN = (
    "season",
    "recorded_air_subtype",
    "result_family",
    "standardized_air_subtype",
)


def _values(values: tuple[str, ...]) -> exp.Tuple:
    return exp.Tuple(expressions=[exp.Literal.string(value) for value in values])


_AUDITS: list[tuple[str, dict[str, object]]] = [
    (
        "not_null",
        {
            "columns": exp.Tuple(
                expressions=[
                    *(exp.column(name) for name in _GRAIN),
                    exp.column("probability_mean"),
                    exp.column("probability_lower_95"),
                    exp.column("probability_upper_95"),
                    exp.column("basis"),
                    exp.column("cell_status"),
                    exp.column("partially_identified"),
                ]
            ),
        },
    ),
    (
        "unique_grain",
        {"columns": exp.Tuple(expressions=[exp.column(name) for name in _GRAIN])},
    ),
    (
        "accepted_values",
        {
            "column": exp.column("recorded_air_subtype"),
            "is_in": _values(RECORDED_AIR_SUBTYPES),
        },
    ),
    (
        "accepted_values",
        {
            "column": exp.column("standardized_air_subtype"),
            "is_in": _values(STANDARDIZED_AIR_SUBTYPES),
        },
    ),
    (
        "accepted_values",
        {"column": exp.column("result_family"), "is_in": _values(RESULT_FAMILIES)},
    ),
    ("accepted_values", {"column": exp.column("basis"), "is_in": _values(BASES)}),
    (
        "accepted_values",
        {"column": exp.column("cell_status"), "is_in": _values(CELL_STATUSES)},
    ),
    ("estimated_contract_complete", {}),
    ("min_row_count", {"threshold": 1000}),
]


@model(
    "main_models.air_trajectory_translation",
    is_sql=True,
    kind="FULL",
    columns={
        "season": "SMALLINT",
        "recorded_air_subtype": "VARCHAR",
        "result_family": "VARCHAR",
        "standardized_air_subtype": "VARCHAR",
        "probability_mean": "DOUBLE",
        "probability_lower_95": "DOUBLE",
        "probability_upper_95": "DOUBLE",
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
        "season": doc("season"),
        "recorded_air_subtype": "Recorded airborne trajectory label: Fly, LineDrive, or PopUp.",
        "result_family": "Plate appearance result family the translation is conditioned on.",
        "standardized_air_subtype": "Standardized launch-angle band: Fly (25 to 50 degrees), LineDrive (10 to 25), or PopUp (above 50).",
        "probability_mean": "Posterior mean probability that an event with this recorded label and result family falls in this band. The three bands of a cell sum to 1.",
        "probability_lower_95": "Lower end of the nominal 95 percent interval. Excludes the error in the pre-2009 assumptions.",
        "probability_upper_95": "Upper end of the nominal 95 percent interval.",
        "basis": "How the season was estimated: referenced_season_posterior (a season with Statcast reference angles), pipeline_new_season_predictive (an unreferenced season of a referenced recording pipeline), or raked_from_pipeline_c_with_modern_mix (a season before 2009, raked from the 2020 onward translation under the modern band-mix assumption). Seasons per basis: docs/geometry-air-translation-estimate-2026-09-11.md.",
        "cell_status": "posterior_cell when the reference pipeline observed the cell; prior_only_cell when the interval is the prior alone.",
        "partially_identified": "TRUE before 2009: the translation assumes the modern true band mix and cannot be checked against same-vocabulary references.",
        "method": "Same as basis.",
        "weak_identification_flag": "Same as partially_identified.",
    },
    grain=list(_GRAIN),
    audits=_AUDITS,
    description=(
        "Translation from the recorded airborne trajectory label (Fly, LineDrive, "
        "PopUp) and result family to the standardized launch-angle band, one row "
        "per season, recorded label, result family, and band, for 1989 to 2025. "
        "An estimate from the airborne pipeline translation run, not a recorded "
        "value; fit on regular-season games and applied to every game type. "
        "Ground balls and bunts are not translated."
    ),
)
def entrypoint(evaluator: MacroEvaluator) -> str:
    del evaluator
    return build_translation_sql()
