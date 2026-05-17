"""Diagnostic probes for the shared-embedding pretrainer.

Three probes are available, each gated on its own env var:

- ``SlashLineProbe`` (BC_PRETRAIN_SLASH_PROBE=1): implied AVG/OBP/SLG
  from the ``pa_result`` softmax for 5 batter + 5 pitcher tiers.
- ``FieldingProbe`` (BC_PRETRAIN_FIELDING_PROBE=1): implied P(out) from
  the ``hit_or_out`` head for 5 shortstop tiers, evaluated on a sampled
  ``trajectory_remapped='GroundBall'`` context with the ball routed to
  position 6.
- ``OutfieldArmProbe`` (BC_PRETRAIN_OF_ARM_PROBE=1): implied
  P(OutAdvancing) from the ``r1_advancement`` softmax for 5 right
  fielder tiers, evaluated on a sampled OF-fly-with-runner context.

All probes require ``BC_DB_PATH`` set at fit time so roster IDs resolve
from ``main_models.stg_people``. Probes broadcast a real sampled
training-row context to every probe row and only override the slot
under test, so the model sees in-distribution inputs.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any

import numpy as np
import polars as pl
from numpy.typing import NDArray

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.pretrain.spec import PretrainSpec

_log = logging.getLogger(__name__)


def pad_offset_inputs(
    model: Any, probe_x: dict[str, NDArray[Any]]
) -> dict[str, NDArray[Any]]:
    """Return ``probe_x`` augmented with zero arrays for any ``offset_*`` input.

    Phase-3 v7 stage-2 models declare additive ``offset_<head>`` keras Inputs.
    Probes feed canonical contexts and want the embedding/residual signal in
    isolation; supplying zeros leaves the softmax acting on Δ-logits only,
    so probe semantics stay comparable to v6 (no offsets).
    """
    out = dict(probe_x)
    batch = None
    for v in probe_x.values():
        batch = int(np.asarray(v).shape[0])
        break
    if batch is None:
        return out
    try:
        model_inputs = list(model.inputs)
    except AttributeError:
        return out
    for inp in model_inputs:
        name = getattr(inp, "name", None)
        if not isinstance(name, str) or not name.startswith("offset_"):
            continue
        if name in out:
            continue
        shape = tuple(int(d) if d is not None else 1 for d in inp.shape[1:])
        out[name] = np.zeros((batch, *shape), dtype=np.float32)
    return out


PROBE_TIERS_BATTERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "HOF",
        "batter",
        ("Babe Ruth", "Ruth, Babe", "ruthba01"),
    ),
    (
        "AllStar",
        "batter",
        ("Mike Trout", "Trout, Mike", "troutmi01"),
    ),
    (
        "Average",
        "batter",
        ("Justin Turner", "Turner, Justin", "turnejt01"),
    ),
    (
        "BelowAvg",
        "batter",
        ("Andrelton Simmons", "Simmons, Andrelton", "simmoan01"),
    ),
    (
        "Replacement",
        "batter",
        ("Pat Kelly", "Kelly, Pat", "kellypa01"),
    ),
)

PROBE_TIERS_PITCHERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "HOF",
        "pitcher",
        ("Pedro Martinez", "Martinez, Pedro", "martipe02"),
    ),
    (
        "AllStar",
        "pitcher",
        ("Clayton Kershaw", "Kershaw, Clayton", "kershcl01"),
    ),
    (
        "Average",
        "pitcher",
        ("Mark Buehrle", "Buehrle, Mark", "buehrma01"),
    ),
    (
        "BelowAvg",
        "pitcher",
        ("Edwin Jackson", "Jackson, Edwin", "jacksed01"),
    ),
    (
        "Replacement",
        "pitcher",
        ("Bronson Arroyo", "Arroyo, Bronson", "arroybr01"),
    ),
)


PROBE_TIERS_SHORTSTOPS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "HOF",
        "ss",
        ("Ozzie Smith", "Smith, Ozzie", "smitoz01"),
    ),
    (
        "DefElite",
        "ss",
        ("Andrelton Simmons", "Simmons, Andrelton", "simmoan01"),
    ),
    (
        "AllStar",
        "ss",
        ("Francisco Lindor", "Lindor, Francisco", "lindofr01"),
    ),
    (
        "BelowAvg",
        "ss",
        ("Derek Jeter", "Jeter, Derek", "jeterde01"),
    ),
    (
        "Replacement",
        "ss",
        ("Tim Anderson", "Anderson, Tim", "anderti01"),
    ),
)


PROBE_TIERS_RIGHT_FIELDERS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    (
        "CannonArm",
        "rf",
        ("Roberto Clemente", "Clemente, Roberto", "clemero01"),
    ),
    (
        "StrongArm",
        "rf",
        ("Vladimir Guerrero", "Guerrero, Vladimir", "guerrvl01"),
    ),
    (
        "Average",
        "rf",
        ("Aaron Judge", "Judge, Aaron", "judgeaa01"),
    ),
    (
        "BelowAvg",
        "rf",
        ("Manny Ramirez", "Ramirez, Manny", "ramirma02"),
    ),
    (
        "Replacement",
        "rf",
        ("Adam Dunn", "Dunn, Adam", "dunnad01"),
    ),
)


@dataclass(frozen=True)
class ProbeRow:
    tier: str
    side: str
    player_label: str
    player_id: str


@dataclass(frozen=True)
class PaResultFlags:
    is_hit: NDArray[np.float32]
    is_at_bat: NDArray[np.float32]
    is_on_base_success: NDArray[np.float32]
    is_on_base_opportunity: NDArray[np.float32]
    total_bases: NDArray[np.float32]


def build_pa_flags(
    pa_class_labels: tuple[str, ...],
    seed_rows: list[dict[str, Any]],
) -> PaResultFlags:
    by_name: dict[str, dict[str, Any]] = {}
    for row in seed_rows:
        by_name[str(row["plate_appearance_result"])] = row
    is_hit = np.zeros(len(pa_class_labels), dtype=np.float32)
    is_at_bat = np.zeros(len(pa_class_labels), dtype=np.float32)
    is_on_base_success = np.zeros(len(pa_class_labels), dtype=np.float32)
    is_on_base_opportunity = np.zeros(len(pa_class_labels), dtype=np.float32)
    total_bases = np.zeros(len(pa_class_labels), dtype=np.float32)
    for i, label in enumerate(pa_class_labels):
        row = by_name.get(label)
        if row is None:
            continue
        is_hit[i] = 1.0 if _truthy(row.get("is_hit")) else 0.0
        is_at_bat[i] = 1.0 if _truthy(row.get("is_at_bat")) else 0.0
        is_on_base_success[i] = (
            1.0 if _truthy(row.get("is_on_base_success")) else 0.0
        )
        is_on_base_opportunity[i] = (
            1.0 if _truthy(row.get("is_on_base_opportunity")) else 0.0
        )
        total_bases[i] = float(row.get("total_bases", 0) or 0)
    return PaResultFlags(
        is_hit=is_hit,
        is_at_bat=is_at_bat,
        is_on_base_success=is_on_base_success,
        is_on_base_opportunity=is_on_base_opportunity,
        total_bases=total_bases,
    )


def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v != 0
    if isinstance(v, str):
        return v.strip().lower() in ("true", "t", "yes", "y", "1")
    return False


def _discover_stg_people_schema(con: Any) -> str | None:
    """Return a schema name that contains an stg_people table, preferring main_models.*."""
    try:
        rows = con.execute(
            "SELECT table_schema FROM information_schema.tables "
            "WHERE table_name = 'stg_people' "
            "ORDER BY CASE WHEN table_schema = 'main_models' THEN 0 "
            "             WHEN table_schema LIKE 'main_models__%' THEN 1 "
            "             ELSE 2 END, "
            "         table_schema"
        ).fetchall()
    except Exception as exc:  # pragma: no cover — defensive
        _log.warning("slash_probe stg_people schema lookup failed err=%s", exc)
        return None
    return str(rows[0][0]) if rows else None


def _resolve_player_id(
    con: Any, schema: str, name_candidates: tuple[str, ...]
) -> str | None:
    """Look up a retrosheet_player_id from name aliases."""
    qident = f'"{schema}".stg_people'
    for cand in name_candidates:
        if not cand:
            continue
        try:
            row = con.execute(
                f"SELECT retrosheet_player_id FROM {qident} "
                "WHERE LOWER(retrosheet_player_id) = LOWER(?) "
                "   OR LOWER(first_name || ' ' || last_name) = LOWER(?) "
                "   OR LOWER(last_name || ', ' || first_name) = LOWER(?) "
                "LIMIT 1",
                [cand, cand, cand],
            ).fetchone()
        except Exception as exc:  # pragma: no cover — defensive
            _log.warning("slash_probe player lookup failed cand=%r err=%s", cand, exc)
            return None
        if row is not None and row[0] is not None:
            return str(row[0])
    return None


def _resolve_tiers(
    db_path: str,
    tiers: tuple[tuple[str, str, tuple[str, ...]], ...],
    *,
    probe_label: str,
) -> tuple[ProbeRow, ...]:
    import duckdb

    con = duckdb.connect(":memory:")
    con.execute(f"ATTACH '{db_path}' AS bc (READ_ONLY)")
    con.execute("USE bc")
    try:
        schema = _discover_stg_people_schema(con)
        if schema is None:
            _log.warning("%s: no stg_people table found in %s", probe_label, db_path)
            return ()
        _log.info("%s resolving roster from schema=%s", probe_label, schema)
        rows: list[ProbeRow] = []
        for tier, side, cands in tiers:
            pid = _resolve_player_id(con, schema, cands)
            if pid is None:
                _log.warning(
                    "%s could not resolve tier=%s side=%s candidates=%s",
                    probe_label,
                    tier,
                    side,
                    cands,
                )
                continue
            rows.append(
                ProbeRow(tier=tier, side=side, player_label=cands[0], player_id=pid)
            )
        return tuple(rows)
    finally:
        con.close()


def resolve_canonical_ids(db_path: str) -> tuple[ProbeRow, ...]:
    return _resolve_tiers(
        db_path,
        PROBE_TIERS_BATTERS + PROBE_TIERS_PITCHERS,
        probe_label="slash_probe",
    )


def resolve_shortstop_ids(db_path: str) -> tuple[ProbeRow, ...]:
    return _resolve_tiers(
        db_path, PROBE_TIERS_SHORTSTOPS, probe_label="fielding_probe"
    )


def resolve_right_fielder_ids(db_path: str) -> tuple[ProbeRow, ...]:
    return _resolve_tiers(
        db_path, PROBE_TIERS_RIGHT_FIELDERS, probe_label="of_arm_probe"
    )


def _training_modes(stats: dict[str, Any], col: str, default: int = 0) -> int:
    mode = stats.get(col)
    if mode is None:
        return default
    return int(mode)


def build_probe_inputs(
    *,
    layout: FeatureLayout,
    spec: PretrainSpec,
    probe_rows: tuple[ProbeRow, ...],
    encoded_train: dict[str, NDArray[Any]],
    batter_id_col: str = "batter_id",
    pitcher_id_col: str = "pitcher_id",
    vocab_lookup: dict[str, dict[str, int]] | None = None,
    context_row_index: int | None = None,
) -> dict[str, NDArray[Any]]:
    """Build an ``n_rows`` × ``n_features`` batch from a list of probe rows.

    A real training-row context is sampled (deterministic via
    ``context_row_index``; default = fixed seed 0 picks row 0) and
    broadcast to all probe rows. Only the player slot under test
    (``batter_id`` for batter probes, ``pitcher_id`` for pitcher probes)
    is overridden per row. All other slots (fielders, runners, opposing
    player, low-card categoricals, numerics) carry the sampled row's
    real values, so the model sees in-distribution context.
    """
    _ = spec
    n = len(probe_rows)
    probe_x: dict[str, NDArray[Any]] = {}

    n_train = 0
    for arr in encoded_train.values():
        if arr is not None and arr.size > 0:
            n_train = int(arr.shape[0])
            break
    if n_train == 0:
        idx = 0
    else:
        rng = np.random.default_rng(seed=42)
        idx = int(context_row_index) if context_row_index is not None else int(
            rng.integers(0, n_train)
        )
        idx = max(0, min(idx, n_train - 1))

    def _broadcast(col: str, dtype: Any) -> NDArray[Any]:
        train_arr = encoded_train.get(col)
        if train_arr is None or train_arr.size == 0:
            fallback = 0 if np.issubdtype(np.dtype(dtype), np.integer) else 0.0
            return np.full((n, 1), fallback, dtype=dtype)
        val = np.asarray(train_arr, dtype=dtype)[idx, 0]
        return np.full((n, 1), val, dtype=dtype)

    for col in layout.high_card_columns:
        probe_x[col] = _broadcast(col, np.int64)

    for col in layout.low_card_columns:
        probe_x[col] = _broadcast(col, np.int64)

    for col in layout.numeric_columns:
        probe_x[col] = _broadcast(col, np.float32)

    if vocab_lookup is None:
        return probe_x
    bvocab = vocab_lookup.get(layout.embedding_unit_for_column(batter_id_col), {})
    pvocab = vocab_lookup.get(layout.embedding_unit_for_column(pitcher_id_col), {})
    for i, row in enumerate(probe_rows):
        if row.side == "batter":
            probe_x[batter_id_col][i, 0] = int(bvocab.get(row.player_id, 0))
        else:
            probe_x[pitcher_id_col][i, 0] = int(pvocab.get(row.player_id, 0))
    return probe_x


def sample_context_row_index(
    train_df: pl.DataFrame,
    *,
    predicate: pl.Expr,
    label: str,
    seed: int = 0,
) -> int | None:
    """Sample an index into the training frame whose row satisfies ``predicate``.

    Returns ``None`` if no row matches. Uses ``with_row_index`` to preserve
    the original row position (which is what ``encoded_train`` was indexed
    with), so downstream callers can broadcast that row's encoded values.
    """
    try:
        matches = (
            train_df.with_row_index(name="__bc_probe_idx")
            .filter(predicate)
            .select("__bc_probe_idx")
        )
    except Exception as exc:  # pragma: no cover — defensive
        _log.warning("%s context sample failed err=%s", label, exc)
        return None
    if matches.height == 0:
        _log.warning("%s context sample matched 0 rows", label)
        return None
    rng = np.random.default_rng(seed=seed)
    pick = int(rng.integers(0, matches.height))
    idx = int(matches.row(pick)[0])
    _log.info(
        "%s context sample matched=%d idx=%d", label, matches.height, idx
    )
    return idx


def neutral_pa_context_predicate() -> pl.Expr:
    """Filter to a run-environment-neutral PA context.

    Goal: probe rows should not be biased by extreme era, park, count, or
    base/out state. Window:
      season 1980-1992 (post-collusion, post-DH for AL, pre-juiced-ball,
        pre-PED-era; AL BA ~.260, SLG ~.395 — the most neutral run
        environment in modern history),
      AL league (DH on; no pitcher-batting confound),
      empty bases, 1 out, 1-1 count, tied score, mid-game (inning 4-5).

    Modern players (Trout, Kershaw, ...) will see ``season=1980-1992``
    in the numeric input — an OOD combo for their embedding. That is
    intentional: probing player skill *stripped of era* is what we want.
    """
    return (
        pl.col("season").is_between(1980, 1992)
        & (pl.col("league") == "AL")
        & (pl.col("count_balls") == 1)
        & (pl.col("count_strikes") == 1)
        & (pl.col("outs_start") == 1)
        & (pl.col("base_state_start") == 0)
        & (pl.col("score_margin") == 0)
        & pl.col("inning_start").is_between(4, 5)
    )


def grounder_to_ss_predicate() -> pl.Expr:
    """Neutral PA context restricted to GroundBall→SS events."""
    return neutral_pa_context_predicate() & (
        pl.col("trajectory_remapped") == "GroundBall"
    ) & (pl.col("batted_to_fielder_class").cast(pl.Utf8) == "6")


def of_fly_with_runner_predicate() -> pl.Expr:
    """Neutral context restricted to OF flies / liners with r1 only on base.

    Diverges from ``neutral_pa_context_predicate`` on ``base_state_start``:
    requires runner on 1B only (state code 1) so ``r1_advancement`` is
    non-null and ``r2_advancement`` / ``r3_advancement`` don't pollute the
    arm signal.
    """
    return (
        pl.col("season").is_between(1980, 1992)
        & (pl.col("league") == "AL")
        & (pl.col("count_balls") == 1)
        & (pl.col("count_strikes") == 1)
        & (pl.col("outs_start") == 1)
        & (pl.col("base_state_start") == 1)
        & (pl.col("score_margin") == 0)
        & pl.col("inning_start").is_between(4, 5)
        & pl.col("trajectory_remapped").is_in(["Fly", "LineDrive"])
        & pl.col("batted_to_fielder_class").cast(pl.Utf8).is_in(["7", "8", "9"])
        & pl.col("runner_on_1b_id").is_not_null()
    )


def build_fielder_slot_probe_inputs(
    *,
    layout: FeatureLayout,
    probe_rows: tuple[ProbeRow, ...],
    encoded_train: dict[str, NDArray[Any]],
    slot_column: str,
    vocab_lookup: dict[str, dict[str, int]],
    context_row_index: int,
) -> dict[str, NDArray[Any]]:
    """Build a ``len(probe_rows)`` × ``n_features`` batch sweeping a single
    fielder slot through canonical IDs.

    Broadcasts ``context_row_index`` from ``encoded_train`` to every probe
    row, then overrides only ``slot_column`` with the per-row player code
    (OOV → 0 when the canonical ID is missing from the vocab).
    """
    n = len(probe_rows)
    probe_x: dict[str, NDArray[Any]] = {}

    n_train = 0
    for arr in encoded_train.values():
        if arr is not None and arr.size > 0:
            n_train = int(arr.shape[0])
            break
    if n_train == 0:
        idx = 0
    else:
        idx = max(0, min(int(context_row_index), n_train - 1))

    def _broadcast(col: str, dtype: Any) -> NDArray[Any]:
        train_arr = encoded_train.get(col)
        if train_arr is None or train_arr.size == 0:
            fallback = 0 if np.issubdtype(np.dtype(dtype), np.integer) else 0.0
            return np.full((n, 1), fallback, dtype=dtype)
        val = np.asarray(train_arr, dtype=dtype)[idx, 0]
        return np.full((n, 1), val, dtype=dtype)

    for col in layout.high_card_columns:
        probe_x[col] = _broadcast(col, np.int64)
    for col in layout.low_card_columns:
        probe_x[col] = _broadcast(col, np.int64)
    for col in layout.numeric_columns:
        probe_x[col] = _broadcast(col, np.float32)

    slot_unit = layout.embedding_unit_for_column(slot_column)
    slot_vocab = vocab_lookup.get(slot_unit, {})
    for i, row in enumerate(probe_rows):
        probe_x[slot_column][i, 0] = int(slot_vocab.get(row.player_id, 0))
    return probe_x


def build_fielding_probe_callback(
    *,
    probe_rows: tuple[ProbeRow, ...],
    probe_x: dict[str, NDArray[Any]],
    head_name: str = "hit_or_out",
) -> Any:
    """Sweep SS through canonical roster; log P(out) on a GroundBall→SS context.

    The ``hit_or_out`` head is binary: 1 = hit, 0 = out. We log P(out) =
    1 - sigmoid(logit). Higher P(out) → better defender on grounders.
    """
    import keras

    class FieldingProbe(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.probe_rows = probe_rows
            self.probe_x = probe_x
            self.head_name = head_name

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            try:
                preds = self.model.predict(
                    pad_offset_inputs(self.model, self.probe_x), verbose=0
                )
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning(
                    "fielding_probe predict failed epoch=%d err=%s", epoch, exc
                )
                return
            head_pred = preds.get(self.head_name) if isinstance(preds, dict) else None
            if head_pred is None:
                _log.warning(
                    "fielding_probe: head %s missing from preds", self.head_name
                )
                return
            arr = np.asarray(head_pred, dtype=np.float64).reshape(len(self.probe_rows), -1)
            p_hit = arr[:, 0] if arr.shape[1] >= 1 else np.zeros(len(self.probe_rows))
            p_out = 1.0 - p_hit
            for i, row in enumerate(self.probe_rows):
                _log.info(
                    "fielding_probe epoch=%d tier=%s slot=ss player=%s P(out)=%.3f P(hit)=%.3f",
                    epoch,
                    row.tier,
                    row.player_label,
                    float(p_out[i]),
                    float(p_hit[i]),
                )

    return FieldingProbe()


def build_outfield_arm_probe_callback(
    *,
    probe_rows: tuple[ProbeRow, ...],
    probe_x: dict[str, NDArray[Any]],
    advancement_class_labels: tuple[str, ...],
    head_name: str = "r1_advancement",
) -> Any:
    """Sweep RF through canonical roster; log P(OutAdvancing | r1 advancement).

    Higher P(OutAdvancing) on an OF-fly-with-runner-on-1st context indicates
    a stronger / more accurate arm able to throw out the runner.
    """
    import keras

    try:
        out_idx = advancement_class_labels.index("OutAdvancing")
    except ValueError:
        out_idx = -1

    class OutfieldArmProbe(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.probe_rows = probe_rows
            self.probe_x = probe_x
            self.head_name = head_name

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            if out_idx < 0:
                _log.warning(
                    "of_arm_probe: OutAdvancing class label not in head %s",
                    self.head_name,
                )
                return
            try:
                preds = self.model.predict(
                    pad_offset_inputs(self.model, self.probe_x), verbose=0
                )
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning(
                    "of_arm_probe predict failed epoch=%d err=%s", epoch, exc
                )
                return
            head_pred = preds.get(self.head_name) if isinstance(preds, dict) else None
            if head_pred is None:
                _log.warning(
                    "of_arm_probe: head %s missing from preds", self.head_name
                )
                return
            arr = np.asarray(head_pred, dtype=np.float64)
            if arr.ndim == 3 and arr.shape[1] == 1:
                arr = arr[:, 0, :]
            for i, row in enumerate(self.probe_rows):
                probs = arr[i]
                p_out_adv = float(probs[out_idx])
                p_scored = float(
                    probs[advancement_class_labels.index("Scored")]
                ) if "Scored" in advancement_class_labels else float("nan")
                p_advanced1 = float(
                    probs[advancement_class_labels.index("Advanced1")]
                ) if "Advanced1" in advancement_class_labels else float("nan")
                _log.info(
                    "of_arm_probe epoch=%d tier=%s slot=rf player=%s P(OutAdvancing)=%.3f P(Scored)=%.3f P(Advanced1)=%.3f",
                    epoch,
                    row.tier,
                    row.player_label,
                    p_out_adv,
                    p_scored,
                    p_advanced1,
                )

    return OutfieldArmProbe()


def fielding_probe_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_FIELDING_PROBE", "0") in ("1", "true", "TRUE")


def of_arm_probe_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_OF_ARM_PROBE", "0") in ("1", "true", "TRUE")


def build_slash_probe_callback(
    *,
    layout: FeatureLayout,
    spec: PretrainSpec,
    pa_class_labels: tuple[str, ...],
    pa_flags: PaResultFlags,
    probe_rows: tuple[ProbeRow, ...],
    probe_x: dict[str, NDArray[Any]],
) -> Any:
    import keras

    class SlashLineProbe(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.pa_class_labels = pa_class_labels
            self.pa_flags = pa_flags
            self.probe_rows = probe_rows
            self.probe_x = probe_x
            self.layout = layout
            self.spec = spec

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            try:
                preds = self.model.predict(
                    pad_offset_inputs(self.model, self.probe_x), verbose=0
                )
            except Exception as exc:  # pragma: no cover — defensive
                _log.warning("slash_probe predict failed epoch=%d err=%s", epoch, exc)
                return
            if isinstance(preds, dict):
                pa_pred = preds.get("pa_result")
            else:
                pa_pred = None
            if pa_pred is None:
                _log.warning("slash_probe: pa_result head missing from preds")
                return
            pa_pred = np.asarray(pa_pred, dtype=np.float64)
            if pa_pred.ndim == 3 and pa_pred.shape[1] == 1:
                pa_pred = pa_pred[:, 0, :]
            eps = 1e-9
            hits = pa_pred @ self.pa_flags.is_hit.astype(np.float64)
            at_bats = pa_pred @ self.pa_flags.is_at_bat.astype(np.float64)
            on_base = pa_pred @ self.pa_flags.is_on_base_success.astype(np.float64)
            on_base_opp = pa_pred @ self.pa_flags.is_on_base_opportunity.astype(np.float64)
            total_bases = pa_pred @ self.pa_flags.total_bases.astype(np.float64)
            avg_arr = hits / np.maximum(at_bats, eps)
            obp_arr = on_base / np.maximum(on_base_opp, eps)
            slg_arr = total_bases / np.maximum(at_bats, eps)
            for i, row in enumerate(self.probe_rows):
                _log.info(
                    "slash_probe epoch=%d tier=%s side=%s player=%s AVG=%.3f OBP=%.3f SLG=%.3f",
                    epoch,
                    row.tier,
                    row.side,
                    row.player_label,
                    float(avg_arr[i]),
                    float(obp_arr[i]),
                    float(slg_arr[i]),
                )

    return SlashLineProbe()


def slash_probe_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_SLASH_PROBE", "0") in ("1", "true", "TRUE")


def db_path_for_probe() -> str | None:
    return os.environ.get("BC_DB_PATH")


def mc_probe_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_MC_PROBE", "0") in ("1", "true", "TRUE")


def downstream_proxy_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_DOWNSTREAM_PROXY", "0") in ("1", "true", "TRUE")


def confound_leak_probe_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_CONFOUND_LEAK", "0") in ("1", "true", "TRUE")


def embed_health_enabled() -> bool:
    return os.environ.get("BC_PRETRAIN_EMBED_HEALTH", "0") in ("1", "true", "TRUE")


def build_embed_health_callback(
    *,
    layer_names: tuple[str, ...],
    cosine_sample_size: int = 2000,
    log_every_n_epochs: int = 1,
    out_jsonl_path: Any | None = None,
) -> Any:
    """Per-epoch embedding-health diagnostics for the named layers.

    Reports per layer: effective rank = (sum(s))^2 / sum(s^2) over the SVD
    singular values; mean pairwise cosine on a random row sample; min /
    max / mean per-dim variance across rows. JSON-lines artifact under
    ``out_jsonl_path`` accumulates one record per epoch per layer.
    """
    import keras

    class EmbeddingHealthCallback(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.layer_names = layer_names
            self.cosine_sample_size = max(2, int(cosine_sample_size))
            self.log_every_n_epochs = max(1, int(log_every_n_epochs))
            self.out_jsonl_path = out_jsonl_path

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            if (epoch + 1) % self.log_every_n_epochs != 0:
                return
            records: list[dict[str, Any]] = []
            for name in self.layer_names:
                try:
                    layer = self.model.get_layer(name)
                except ValueError:
                    _log.warning("embed_health: layer %s missing", name)
                    continue
                weights = layer.get_weights()
                if not weights:
                    continue
                matrix = np.asarray(weights[0], dtype=np.float32)
                if matrix.ndim != 2 or matrix.size == 0:
                    continue
                n_rows, dim = matrix.shape
                try:
                    s = np.linalg.svd(matrix, compute_uv=False)
                except np.linalg.LinAlgError:
                    s = np.zeros(min(matrix.shape), dtype=np.float32)
                sum_s = float(np.sum(s))
                sum_s2 = float(np.sum(s * s))
                eff_rank = (sum_s * sum_s) / max(sum_s2, 1e-12)
                rng = np.random.default_rng(seed=epoch * 7919 + hash(name) % (2**31))
                sample_size = min(self.cosine_sample_size, n_rows)
                idx = rng.choice(n_rows, size=sample_size, replace=False)
                sample = matrix[idx].astype(np.float64)
                norms = np.linalg.norm(sample, axis=1, keepdims=True)
                normalized = sample / np.maximum(norms, 1e-9)
                cosines = normalized @ normalized.T
                upper = cosines[np.triu_indices_from(cosines, k=1)]
                mean_abs_cos = float(np.mean(np.abs(upper))) if upper.size > 0 else 0.0
                col_var = np.var(matrix, axis=0)
                rec = {
                    "epoch": int(epoch),
                    "layer": name,
                    "n_rows": int(n_rows),
                    "dim": int(dim),
                    "eff_rank": float(eff_rank),
                    "eff_rank_fraction": float(eff_rank / max(dim, 1)),
                    "mean_abs_cosine": mean_abs_cos,
                    "col_var_min": float(np.min(col_var)),
                    "col_var_mean": float(np.mean(col_var)),
                    "col_var_max": float(np.max(col_var)),
                }
                _log.info(
                    "embed_health epoch=%d layer=%s eff_rank=%.2f eff_rank_frac=%.3f mean_abs_cos=%.4f col_var_mean=%.4f",
                    epoch,
                    name,
                    rec["eff_rank"],
                    rec["eff_rank_fraction"],
                    rec["mean_abs_cosine"],
                    rec["col_var_mean"],
                )
                records.append(rec)
            if self.out_jsonl_path is not None and records:
                import json
                from pathlib import Path

                Path(self.out_jsonl_path).parent.mkdir(parents=True, exist_ok=True)
                with open(self.out_jsonl_path, "a", encoding="utf-8") as fh:
                    for rec in records:
                        fh.write(json.dumps(rec) + "\n")

    return EmbeddingHealthCallback()


@dataclass(frozen=True)
class LinearProbeTaskSpec:
    """Single task in the downstream linear-probe proxy.

    ``y_train`` / ``y_val`` are 1-D arrays of integer class codes. Rows with
    code ``-1`` are masked out (e.g. NULL-target rows for sparse heads).
    """

    name: str
    y_train: NDArray[np.int64]
    y_val: NDArray[np.int64]


def build_linear_probe_callback(
    *,
    train_ids: dict[str, NDArray[np.int64]],
    val_ids: dict[str, NDArray[np.int64]],
    layer_to_id_columns: dict[str, tuple[str, ...]],
    tasks: tuple[LinearProbeTaskSpec, ...],
    log_every_n_epochs: int = 2,
    out_jsonl_path: Any | None = None,
) -> Any:
    """Per-epoch linear-probe downstream proxy on frozen embeddings.

    Mechanism: directly looks up the rows of each embedding matrix
    referenced by ``layer_to_id_columns``, concatenates per-row, fits
    sklearn ``LogisticRegression`` on ``train`` slice, scores on ``val``
    slice. Reports per-task macro-F1 + log-loss and an aggregate
    ``downstream_proxy_score`` (mean macro-F1 across tasks). One scalar
    suitable for cross-epoch comparison and cross-artifact comparison.
    """
    import keras

    class LinearProbeDownstreamCallback(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.train_ids = train_ids
            self.val_ids = val_ids
            self.layer_to_id_columns = layer_to_id_columns
            self.tasks = tasks
            self.log_every_n_epochs = max(1, int(log_every_n_epochs))
            self.out_jsonl_path = out_jsonl_path

        def _build_features(self, ids: dict[str, NDArray[np.int64]]) -> NDArray[np.float32]:
            blocks: list[NDArray[np.float32]] = []
            for layer_name, cols in self.layer_to_id_columns.items():
                try:
                    layer = self.model.get_layer(layer_name)
                except ValueError:
                    _log.warning(
                        "downstream_proxy: layer %s missing from model", layer_name
                    )
                    continue
                weights = layer.get_weights()
                if not weights:
                    continue
                emb = np.asarray(weights[0], dtype=np.float32)
                for col in cols:
                    codes = ids.get(col)
                    if codes is None:
                        continue
                    flat = codes.reshape(-1).astype(np.int64, copy=False)
                    flat = np.clip(flat, 0, emb.shape[0] - 1)
                    blocks.append(emb[flat])
            if not blocks:
                return np.zeros((0, 0), dtype=np.float32)
            return np.concatenate(blocks, axis=1)

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            if (epoch + 1) % self.log_every_n_epochs != 0:
                return
            try:
                from sklearn.linear_model import LogisticRegression
                from sklearn.metrics import f1_score, log_loss
            except ImportError as exc:
                _log.warning("downstream_proxy: sklearn unavailable: %s", exc)
                return
            X_train = self._build_features(self.train_ids)
            X_val = self._build_features(self.val_ids)
            if X_train.size == 0 or X_val.size == 0:
                _log.warning("downstream_proxy: empty feature matrix")
                return
            per_task: dict[str, dict[str, float]] = {}
            scores: list[float] = []
            for task in self.tasks:
                mask_tr = task.y_train >= 0
                mask_va = task.y_val >= 0
                if mask_tr.sum() < 32 or mask_va.sum() < 32:
                    _log.warning(
                        "downstream_proxy: task=%s insufficient labeled rows (tr=%d va=%d)",
                        task.name,
                        int(mask_tr.sum()),
                        int(mask_va.sum()),
                    )
                    continue
                y_tr = task.y_train[mask_tr]
                y_va = task.y_val[mask_va]
                clf = LogisticRegression(
                    max_iter=200, n_jobs=-1, solver="lbfgs"
                )
                clf.fit(X_train[mask_tr], y_tr)
                probs = clf.predict_proba(X_val[mask_va])
                preds = probs.argmax(axis=1)
                pred_labels = clf.classes_[preds]
                f1 = float(f1_score(y_va, pred_labels, average="macro"))
                try:
                    ll = float(log_loss(y_va, probs, labels=clf.classes_))
                except ValueError:
                    ll = float("nan")
                per_task[task.name] = {"macro_f1": f1, "log_loss": ll}
                scores.append(f1)
                _log.info(
                    "downstream_proxy epoch=%d task=%s macro_f1=%.4f log_loss=%.4f",
                    epoch,
                    task.name,
                    f1,
                    ll,
                )
            agg = float(np.mean(scores)) if scores else float("nan")
            _log.info(
                "downstream_proxy epoch=%d downstream_proxy_score=%.4f n_tasks=%d",
                epoch,
                agg,
                len(scores),
            )
            if self.out_jsonl_path is not None and scores:
                import json
                from pathlib import Path

                rec = {
                    "epoch": int(epoch),
                    "downstream_proxy_score": agg,
                    "per_task": per_task,
                }
                Path(self.out_jsonl_path).parent.mkdir(parents=True, exist_ok=True)
                with open(self.out_jsonl_path, "a", encoding="utf-8") as fh:
                    fh.write(json.dumps(rec) + "\n")

    return LinearProbeDownstreamCallback()


def _league_dh_regime_expr() -> pl.Expr:
    return (
        pl.when(pl.col("season") <= 1972).then(pl.lit("pre_dh"))
        .when((pl.col("league") == "AL") & (pl.col("season") <= 2021)).then(pl.lit("al_dh"))
        .when((pl.col("league") == "NL") & (pl.col("season") <= 2021)).then(pl.lit("nl_no_dh"))
        .otherwise(pl.lit("universal_dh"))
    )


def sample_mc_context_indices(
    train_df: pl.DataFrame,
    *,
    n_contexts: int,
    seed: int = 0,
) -> NDArray[np.int64] | None:
    """Sample ``n_contexts`` train-row indices stratified by (era_decade × league_DH_regime).

    Cells are capped at the global average per cell to avoid modern-era dominance.
    Returns None if train_df is empty.
    """
    if train_df.height == 0:
        _log.warning("mc_probe: empty train_df, cannot sample contexts")
        return None
    rng = np.random.default_rng(seed=seed)
    enriched = (
        train_df.with_row_index(name="__bc_mc_idx")
        .with_columns(
            ((pl.col("season") // 10) * 10).alias("__bc_era"),
            _league_dh_regime_expr().alias("__bc_regime"),
        )
        .select("__bc_mc_idx", "__bc_era", "__bc_regime")
    )
    cell_groups = enriched.group_by(["__bc_era", "__bc_regime"])
    cell_counts = cell_groups.agg(pl.len().alias("n")).sort(["__bc_era", "__bc_regime"])
    n_cells = cell_counts.height
    if n_cells == 0:
        return None
    per_cell = max(1, n_contexts // n_cells)
    picks: list[int] = []
    for era_val, regime_val, _n in cell_counts.iter_rows():
        cell = enriched.filter(
            (pl.col("__bc_era") == era_val) & (pl.col("__bc_regime") == regime_val)
        ).select("__bc_mc_idx")
        if cell.height == 0:
            continue
        take = min(per_cell, cell.height)
        chosen = rng.choice(cell.height, size=take, replace=False)
        picks.extend(int(cell.row(int(i))[0]) for i in chosen)
    if len(picks) < n_contexts and enriched.height >= n_contexts:
        remaining = n_contexts - len(picks)
        already = set(picks)
        candidates = [
            int(r[0])
            for r in enriched.select("__bc_mc_idx").iter_rows()
            if int(r[0]) not in already
        ]
        if len(candidates) >= remaining:
            extras = rng.choice(len(candidates), size=remaining, replace=False)
            picks.extend(candidates[int(i)] for i in extras)
    out = np.asarray(picks[:n_contexts], dtype=np.int64)
    _log.info(
        "mc_probe sampled %d contexts across %d cells (target=%d)",
        out.size,
        n_cells,
        n_contexts,
    )
    return out


def build_mc_probe_inputs(
    *,
    layout: FeatureLayout,
    panel_rows: tuple[ProbeRow, ...],
    encoded_train: dict[str, NDArray[Any]],
    context_indices: NDArray[np.int64],
    vocab_lookup: dict[str, dict[str, int]],
    batter_id_col: str = "batter_id",
    pitcher_id_col: str = "pitcher_id",
) -> dict[str, NDArray[Any]]:
    """Build ``(n_contexts * n_panel, 1)`` batch tensors.

    Row order: ``[(ctx_0, panel_0), (ctx_0, panel_1), ..., (ctx_1, panel_0), ...]``.
    For each ``(context, panel_player)`` pair, all encoded features are taken
    from ``encoded_train[context_indices[ctx]]`` and only the slot under test
    (batter or pitcher per panel_row.side) is overridden with the panel
    player's vocab code.
    """
    n_ctx = int(context_indices.size)
    n_panel = len(panel_rows)
    if n_ctx == 0 or n_panel == 0:
        return {}
    total = n_ctx * n_panel
    probe_x: dict[str, NDArray[Any]] = {}

    def _gather(col: str, dtype: Any) -> NDArray[Any]:
        train_arr = encoded_train.get(col)
        if train_arr is None or train_arr.size == 0:
            fallback = 0 if np.issubdtype(np.dtype(dtype), np.integer) else 0.0
            return np.full((total, 1), fallback, dtype=dtype)
        gathered = np.asarray(train_arr, dtype=dtype)[context_indices, 0]
        return np.repeat(gathered, n_panel).reshape(total, 1).astype(dtype, copy=False)

    for col in layout.high_card_columns:
        probe_x[col] = _gather(col, np.int64)
    for col in layout.low_card_columns:
        probe_x[col] = _gather(col, np.int64)
    for col in layout.numeric_columns:
        probe_x[col] = _gather(col, np.float32)

    bvocab = vocab_lookup.get(layout.embedding_unit_for_column(batter_id_col), {})
    pvocab = vocab_lookup.get(layout.embedding_unit_for_column(pitcher_id_col), {})
    for ctx_i in range(n_ctx):
        base = ctx_i * n_panel
        for p_i, row in enumerate(panel_rows):
            offset = base + p_i
            if row.side == "batter":
                probe_x[batter_id_col][offset, 0] = int(bvocab.get(row.player_id, 0))
            elif row.side == "pitcher":
                probe_x[pitcher_id_col][offset, 0] = int(pvocab.get(row.player_id, 0))
    return probe_x


def build_mc_slash_probe_callback(
    *,
    panel_rows: tuple[ProbeRow, ...],
    probe_x: dict[str, NDArray[Any]],
    n_contexts: int,
    pa_flags: PaResultFlags,
    head_name: str = "pa_result",
    log_every_n_epochs: int = 1,
) -> Any:
    """Average ``pa_result`` softmax across ``n_contexts`` Monte-Carlo contexts per panel player.

    Logs marginal AVG/OBP/SLG plus a per-pair pairwise-dominance Spearman ρ
    among batter-side rows and pitcher-side rows separately. External-truth
    rank correlation is not computed here (it lives in the sidecar evaluator
    where the panel CSV's truth columns are available).
    """
    import keras

    class MonteCarloSlashProbe(keras.callbacks.Callback):
        def __init__(self) -> None:
            super().__init__()
            self.panel_rows = panel_rows
            self.probe_x = probe_x
            self.n_contexts = n_contexts
            self.pa_flags = pa_flags
            self.head_name = head_name
            self.log_every_n_epochs = max(1, int(log_every_n_epochs))

        def on_epoch_end(self, epoch: int, logs: dict[str, Any] | None = None) -> None:
            if (epoch + 1) % self.log_every_n_epochs != 0:
                return
            try:
                preds = self.model.predict(
                    pad_offset_inputs(self.model, self.probe_x), verbose=0
                )
            except Exception as exc:
                _log.warning("mc_probe predict failed epoch=%d err=%s", epoch, exc)
                return
            head_pred = preds.get(self.head_name) if isinstance(preds, dict) else preds
            if head_pred is None:
                _log.warning("mc_probe: head %s missing from preds", self.head_name)
                return
            arr = np.asarray(head_pred, dtype=np.float64)
            if arr.ndim == 3 and arr.shape[1] == 1:
                arr = arr[:, 0, :]
            n_panel = len(self.panel_rows)
            arr = arr.reshape(self.n_contexts, n_panel, -1)
            marginal = arr.mean(axis=0)
            eps = 1e-9
            is_hit = self.pa_flags.is_hit.astype(np.float64)
            is_at_bat = self.pa_flags.is_at_bat.astype(np.float64)
            is_on_base_success = self.pa_flags.is_on_base_success.astype(np.float64)
            is_on_base_opportunity = self.pa_flags.is_on_base_opportunity.astype(np.float64)
            total_bases = self.pa_flags.total_bases.astype(np.float64)
            hits = marginal @ is_hit
            at_bats = marginal @ is_at_bat
            on_base = marginal @ is_on_base_success
            on_base_opp = marginal @ is_on_base_opportunity
            tb = marginal @ total_bases
            avg_arr = hits / np.maximum(at_bats, eps)
            obp_arr = on_base / np.maximum(on_base_opp, eps)
            slg_arr = tb / np.maximum(at_bats, eps)
            for i, row in enumerate(self.panel_rows):
                _log.info(
                    "mc_probe epoch=%d ctx=%d tier=%s side=%s player=%s AVG=%.3f OBP=%.3f SLG=%.3f",
                    epoch,
                    self.n_contexts,
                    row.tier,
                    row.side,
                    row.player_label,
                    float(avg_arr[i]),
                    float(obp_arr[i]),
                    float(slg_arr[i]),
                )

    return MonteCarloSlashProbe()
