"""Convergence diagnostics over every posterior variable, plus the group-level pair."""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportAny=false

from __future__ import annotations

import math
from pathlib import Path

import arviz as az
import numpy as np
import pytest

from python_models.statistical.validate import (
    DIAGNOSTICS_BY_VARIABLE_FILENAME,
    GROUP_LEVEL_MAX_ELEMENTS,
    compute_posterior_diagnostics,
    diagnostics_indicate_weak_identification,
    read_diagnostics_by_variable,
    write_diagnostics_by_variable,
)

N_CHAINS = 2
N_DRAWS = 400


def _synthetic_idata(
    *,
    seed: int = 0,
    drift_large: float = 0.0,
    divergences: int = 0,
) -> az.InferenceData:
    rng = np.random.default_rng(seed)
    large = rng.normal(size=(N_CHAINS, N_DRAWS, GROUP_LEVEL_MAX_ELEMENTS + 1))
    large[1, :, :] += drift_large
    posterior = {
        "scalar": rng.normal(size=(N_CHAINS, N_DRAWS)),
        "small": rng.normal(size=(N_CHAINS, N_DRAWS, 8)),
        "boundary": rng.normal(size=(N_CHAINS, N_DRAWS, GROUP_LEVEL_MAX_ELEMENTS)),
        "large": large,
        "grid": rng.normal(size=(N_CHAINS, N_DRAWS, 3, 4)),
    }
    diverging = np.zeros((N_CHAINS, N_DRAWS), dtype=bool)
    diverging.ravel()[:divergences] = True
    return az.from_dict(posterior=posterior, sample_stats={"diverging": diverging})


def _element_counts(idata: az.InferenceData) -> dict[str, int]:
    posterior = idata["posterior"]
    return {
        str(name): int(
            np.prod(
                [
                    s
                    for d, s in posterior[name].sizes.items()
                    if d not in ("chain", "draw")
                ]
            )
        )
        for name in posterior.data_vars
    }


def test_every_posterior_variable_is_diagnosed_by_default() -> None:
    idata = _synthetic_idata()
    diagnostics = compute_posterior_diagnostics(idata)
    counts = _element_counts(idata)
    assert {row.name for row in diagnostics.by_variable} == set(counts)
    for row in diagnostics.by_variable:
        assert row.n_elements == counts[row.name]
        assert row.diagnosed
        assert row.rhat_max is not None and math.isfinite(row.rhat_max)
        assert row.ess_bulk_min is not None and row.ess_bulk_min > 0
        assert row.ess_tail_min is not None and row.ess_tail_min > 0
    assert diagnostics.total_draws == N_CHAINS * N_DRAWS
    assert diagnostics.divergences == 0


def test_summary_pairs_are_extremes_of_the_per_variable_table() -> None:
    diagnostics = compute_posterior_diagnostics(_synthetic_idata(drift_large=4.0))
    diagnosed = [row for row in diagnostics.by_variable if row.diagnosed]
    assert diagnostics.rhat_max == max(row.rhat_max or -math.inf for row in diagnosed)
    assert diagnostics.ess_bulk_min == min(
        row.ess_bulk_min or math.inf for row in diagnosed
    )
    assert diagnostics.ess_tail_min == min(
        row.ess_tail_min or math.inf for row in diagnosed
    )
    group = [row for row in diagnosed if row.n_elements <= GROUP_LEVEL_MAX_ELEMENTS]
    assert {row.name for row in group} == {"scalar", "small", "boundary", "grid"}
    assert set(diagnostics.group_level_variables) == {row.name for row in group}
    assert diagnostics.group_level_rhat_max == max(
        row.rhat_max or -math.inf for row in group
    )
    assert diagnostics.group_level_ess_bulk_min == min(
        row.ess_bulk_min or math.inf for row in group
    )


def test_badly_mixed_large_variable_moves_gate_pair_but_not_group_pair() -> None:
    idata = _synthetic_idata(drift_large=4.0)
    diagnostics = compute_posterior_diagnostics(idata)
    large = next(row for row in diagnostics.by_variable if row.name == "large")
    assert (
        large.rhat_max is not None and large.rhat_max > diagnostics.group_level_rhat_max
    )
    assert diagnostics.rhat_max == large.rhat_max
    assert diagnostics.ess_bulk_min < diagnostics.group_level_ess_bulk_min
    assert diagnostics_indicate_weak_identification(
        rhat_max=diagnostics.rhat_max,
        ess_bulk_min=diagnostics.ess_bulk_min,
        divergences=diagnostics.divergences,
    )
    assert not diagnostics_indicate_weak_identification(
        rhat_max=diagnostics.group_level_rhat_max,
        ess_bulk_min=diagnostics.group_level_ess_bulk_min,
        divergences=diagnostics.divergences,
    )


def test_oversized_variables_are_excluded_and_recorded(
    caplog: pytest.LogCaptureFixture,
) -> None:
    idata = _synthetic_idata(drift_large=4.0)
    cap = GROUP_LEVEL_MAX_ELEMENTS
    with caplog.at_level("WARNING", logger="python_models.statistical.validate"):
        diagnostics = compute_posterior_diagnostics(
            idata, max_elements_per_variable=cap
        )
    assert diagnostics.excluded_variables == ("large",)
    excluded = next(row for row in diagnostics.by_variable if row.name == "large")
    assert not excluded.diagnosed
    assert excluded.rhat_max is None
    assert excluded.ess_bulk_min is None
    assert excluded.ess_tail_min is None
    assert excluded.n_elements == cap + 1
    assert any("large" in record.getMessage() for record in caplog.records)
    assert diagnostics.rhat_max == diagnostics.group_level_rhat_max
    assert diagnostics.ess_bulk_min == diagnostics.group_level_ess_bulk_min


def test_divergences_are_read_from_sample_stats() -> None:
    diagnostics = compute_posterior_diagnostics(_synthetic_idata(divergences=7))
    assert diagnostics.divergences == 7
    assert diagnostics_indicate_weak_identification(
        rhat_max=diagnostics.group_level_rhat_max,
        ess_bulk_min=diagnostics.group_level_ess_bulk_min,
        divergences=diagnostics.divergences,
    )


def test_group_level_rule_is_a_single_threshold() -> None:
    idata = _synthetic_idata()
    lowered = compute_posterior_diagnostics(idata, group_level_max_elements=8)
    assert set(lowered.group_level_variables) == {"scalar", "small"}
    raised = compute_posterior_diagnostics(
        idata, group_level_max_elements=GROUP_LEVEL_MAX_ELEMENTS + 1
    )
    assert set(raised.group_level_variables) == {row.name for row in raised.by_variable}
    assert raised.group_level_rhat_max == raised.rhat_max
    assert raised.group_level_ess_bulk_min == raised.ess_bulk_min


def test_empty_group_level_set_yields_nan_and_a_weak_flag() -> None:
    diagnostics = compute_posterior_diagnostics(
        _synthetic_idata(), group_level_max_elements=0
    )
    assert diagnostics.group_level_variables == ()
    assert math.isnan(diagnostics.group_level_rhat_max)
    assert math.isnan(diagnostics.group_level_ess_bulk_min)
    assert diagnostics_indicate_weak_identification(
        rhat_max=diagnostics.group_level_rhat_max,
        ess_bulk_min=diagnostics.group_level_ess_bulk_min,
        divergences=0,
    )


def test_per_variable_table_round_trips_through_json(tmp_path: Path) -> None:
    diagnostics = compute_posterior_diagnostics(
        _synthetic_idata(), max_elements_per_variable=GROUP_LEVEL_MAX_ELEMENTS
    )
    validation_dir = tmp_path / "validation"
    written = write_diagnostics_by_variable(validation_dir, diagnostics.by_variable)
    assert written == validation_dir / DIAGNOSTICS_BY_VARIABLE_FILENAME
    assert read_diagnostics_by_variable(written) == diagnostics.by_variable
