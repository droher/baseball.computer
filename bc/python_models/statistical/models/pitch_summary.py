"""Cell-grain Multinomial pitch-summary builder.

Models the count vector over the plate appearance's 12 final ball-strike
classes (balls 0-3 x strikes 0-2) per ``(result_family, season, league)`` cell
as ``Multinomial(n=cell_total, p=softmax(eta))``.

Final counts obey a hard structural constraint per result family: strikeouts
end with two strikes and walks end with three balls (``reachable_classes`` in
the prep module is the single definition). Structurally unreachable
``(result_family, class)`` entries are pinned to a large negative logit so their
softmax share is ~0 and carry no free parameter. Each result family pins its own
reference — the modal reachable class, guaranteed reachable and well-observed —
to a zero logit, which identifies the per-family softmax without a zero-sum
constraint. A universal reference is impossible because ``b0_s0`` is unreachable
for both constrained families.

The remaining reachable non-reference logits carry a centered two-level
hierarchy stored flat over the reachable non-reference pairs: a per-``(result
family, class)`` ``result_logodds`` against that family's reference, and a
per-cell ``cell_logodds`` centered on its family's value with a shared
``sigma_cell``. There is no corpus level across families: their distributions
live in different reachable spaces against different references, so pooling
them is meaningless. ``cell_class_prob = softmax(eta)`` is the published
final-count distribution and the only cell-sized Deterministic;
``result_class_prob`` is the per-family marginal used as the held-out baseline.
"""

# pyright: reportMissingTypeStubs=false, reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportOperatorIssue=false, reportCallIssue=false, reportArgumentType=false, reportPrivateImportUsage=false, reportIndexIssue=false, reportAttributeAccessIssue=false

from __future__ import annotations

import logging

import numpy as np
import pymc as pm
import pytensor.tensor as pt

from python_models.statistical.models._pitch_summary_data import PitchSummaryInputs
from python_models.statistical.schemas import BayesPriorConfig

_log = logging.getLogger(__name__)

RESULT_LOGODDS_SCALE: float = 3.0
MASK_LOGIT: float = -30.0


def build_pitch_summary_model(
    inputs: PitchSummaryInputs,
    *,
    priors: BayesPriorConfig | None = None,
) -> pm.Model:
    """Construct the reachability-masked cell-grain final-count model."""
    cfg = priors or BayesPriorConfig()
    counts = inputs.counts.astype(np.int64)
    cell_total = counts.sum(axis=1)
    n_cells = inputs.n_cells
    n_class = inputs.n_classes
    n_result = inputs.n_result_families

    reachable = inputs.reachable_mask
    ref_by_result = inputs.ref_class_by_result
    cell_result = inputs.cell_result_idx

    observed_unreachable = int(counts[~reachable[cell_result]].sum())
    if observed_unreachable:
        raise AssertionError(
            f"{observed_unreachable} observed events land in structurally "
            "unreachable (result_family, class) entries"
        )

    result_pairs = [
        (r, k)
        for r in range(n_result)
        for k in range(n_class)
        if reachable[r, k] and k != ref_by_result[r]
    ]
    pair_lookup = {pair: i for i, pair in enumerate(result_pairs)}
    result_pair_rows = np.array([r for r, _ in result_pairs], dtype=np.int64)
    result_pair_cols = np.array([k for _, k in result_pairs], dtype=np.int64)
    n_result_pairs = len(result_pairs)

    cell_rows: list[int] = []
    cell_cols: list[int] = []
    cell_pairs: list[int] = []
    for c in range(n_cells):
        r = int(cell_result[c])
        for k in range(n_class):
            if reachable[r, k] and k != ref_by_result[r]:
                cell_rows.append(c)
                cell_cols.append(k)
                cell_pairs.append(pair_lookup[(r, k)])
    cell_row_idx = np.array(cell_rows, dtype=np.int64)
    cell_col_idx = np.array(cell_cols, dtype=np.int64)
    cell_pair_idx = np.array(cell_pairs, dtype=np.int64)
    n_cell_pairs = len(cell_rows)

    result_rows = np.arange(n_result)
    cell_ref_rows = np.arange(n_cells)
    cell_ref_cols = ref_by_result[cell_result]

    with pm.Model(coords=inputs.coords) as model:
        result_logodds = pm.Normal(
            "result_logodds", mu=0.0, sigma=RESULT_LOGODDS_SCALE, shape=n_result_pairs
        )
        sigma_cell = pm.HalfNormal("sigma_cell", sigma=cfg.sigma_cell_scale)
        cell_logodds = pm.Normal(
            "cell_logodds",
            mu=result_logodds[cell_pair_idx],
            sigma=sigma_cell,
            shape=n_cell_pairs,
        )

        eta_result = pt.full((n_result, n_class), MASK_LOGIT, dtype="float64")
        eta_result = pt.set_subtensor(eta_result[result_rows, ref_by_result], 0.0)
        eta_result = pt.set_subtensor(
            eta_result[result_pair_rows, result_pair_cols],
            pt.cast(result_logodds, "float64"),
        )
        pm.Deterministic(
            "result_class_prob",
            pm.math.softmax(eta_result, axis=1),
            dims=("result_family", "class"),
        )

        eta = pt.full((n_cells, n_class), MASK_LOGIT, dtype="float64")
        eta = pt.set_subtensor(eta[cell_ref_rows, cell_ref_cols], 0.0)
        eta = pt.set_subtensor(
            eta[cell_row_idx, cell_col_idx], pt.cast(cell_logodds, "float64")
        )
        cell_class_prob = pm.Deterministic(
            "cell_class_prob", pm.math.softmax(eta, axis=1), dims=("cell", "class")
        )

        _ = pm.Multinomial(
            "final_count_obs",
            n=cell_total,
            p=cell_class_prob,
            observed=counts,
        )

    _log.info(
        "build_pitch_summary_model cells=%d results=%d classes=%d "
        "result_pairs=%d cell_pairs=%d events=%d",
        inputs.n_cells,
        n_result,
        n_class,
        n_result_pairs,
        n_cell_pairs,
        inputs.n_events,
    )
    return model
