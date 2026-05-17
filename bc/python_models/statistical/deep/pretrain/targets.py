"""Registered pretrain targets + their FeatureLayouts."""

from __future__ import annotations

from python_models.ml.features import FeatureLayout
from python_models.statistical.deep.pretrain.spec import HeadSpec, PretrainSpec

EVENT_UNIVERSE_DATASET: str = "model_input_event_universe"

PLAYER_GROUP_COLS: tuple[str, ...] = (
    "batter_id",
    "pitcher_id",
    "fielder_pos_2",
    "fielder_pos_3",
    "fielder_pos_4",
    "fielder_pos_5",
    "fielder_pos_6",
    "fielder_pos_7",
    "fielder_pos_8",
    "fielder_pos_9",
    "runner_on_1b_id",
    "runner_on_2b_id",
    "runner_on_3b_id",
)

EVENT_UNIVERSE_HIGH_CARD: tuple[str, ...] = (
    *PLAYER_GROUP_COLS,
    "park_id",
    "scorer",
)

EVENT_UNIVERSE_LOW_CARD: tuple[str, ...] = (
    "league",
    "game_type",
    "frame_start",
    "base_state_start",
    "outs_start",
    "count_balls",
    "count_strikes",
    "alignment_regime",
    "personnel_confidence",
    "context_confidence",
    "source_family",
    "time_of_day",
    "doubleheader_status",
    "precipitation",
    "sky",
    "wind_direction",
    "field_condition",
)

EVENT_UNIVERSE_NUMERIC: tuple[str, ...] = (
    "season",
    "inning_start",
    "score_margin",
    "temperature_fahrenheit",
    "wind_speed_mph",
    "day_of_year",
)

EVENT_UNIVERSE_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=EVENT_UNIVERSE_HIGH_CARD,
    low_card_columns=EVENT_UNIVERSE_LOW_CARD,
    numeric_columns=EVENT_UNIVERSE_NUMERIC,
    grain_column="event_key",
    split_column="primary_fold",
    embedding_groups=(("player", PLAYER_GROUP_COLS),),
)

TRAJECTORY_CLASS_LABELS: tuple[str, ...] = (
    "Fly",
    "GroundBall",
    "LineDrive",
    "PopUp",
    "Bunt",
)

PA_RESULT_CLASS_LABELS: tuple[str, ...] = (
    "Double",
    "FieldersChoice",
    "GroundRuleDouble",
    "HitByPitch",
    "HomeRun",
    "InPlayOut",
    "InsideTheParkHomeRun",
    "IntentionalWalk",
    "Interference",
    "ReachedOnError",
    "SacrificeFly",
    "SacrificeHit",
    "Single",
    "StrikeOut",
    "Triple",
    "Walk",
)

RESULT_FAMILY_CLASS_LABELS: tuple[str, ...] = (
    "fielders_choice",
    "hbp",
    "hit",
    "interference",
    "out_in_play",
    "reached_on_error",
    "sacrifice",
    "strikeout",
    "walk",
)

OUTS_ON_PLAY_LABELS: tuple[str, ...] = ("0", "1", "2", "3")
RUNS_ON_PLAY_LABELS: tuple[str, ...] = ("0", "1", "2", "3", "4")

ADVANCEMENT_CLASS_LABELS: tuple[str, ...] = (
    "Stayed",
    "Advanced1",
    "Advanced2",
    "Scored",
    "OutAdvancing",
    "OutCaughtStealing",
    "OutPickoff",
)

EVENT_UNIVERSE_HEADS_V4: tuple[HeadSpec, ...] = (
    HeadSpec(
        name="pa_result",
        target_column="pa_result",
        kind="multiclass",
        configured_class_labels=PA_RESULT_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="outs_on_play_capped",
        target_column="outs_on_play_capped",
        kind="multiclass",
        configured_class_labels=OUTS_ON_PLAY_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="runs_on_play_capped",
        target_column="runs_on_play_capped",
        kind="multiclass",
        configured_class_labels=RUNS_ON_PLAY_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="trajectory_remapped",
        target_column="trajectory_remapped",
        kind="multiclass",
        configured_class_labels=TRAJECTORY_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="r1_advancement",
        target_column="r1_advancement",
        kind="multiclass",
        configured_class_labels=ADVANCEMENT_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="r2_advancement",
        target_column="r2_advancement",
        kind="multiclass",
        configured_class_labels=ADVANCEMENT_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="r3_advancement",
        target_column="r3_advancement",
        kind="multiclass",
        configured_class_labels=ADVANCEMENT_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_general",
        target_column="batted_location_general",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_depth",
        target_column="batted_location_depth",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_edge",
        target_column="batted_location_edge",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_to_fielder_class",
        target_column="batted_to_fielder_class",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
)

EVENT_UNIVERSE_HEADS_V6: tuple[HeadSpec, ...] = EVENT_UNIVERSE_HEADS_V4

EVENT_UNIVERSE_SPEC: PretrainSpec = PretrainSpec(
    name="event_universe",
    dataset_name=EVENT_UNIVERSE_DATASET,
    head_specs=EVENT_UNIVERSE_HEADS_V6,
)

EVENT_UNIVERSE_CONTEXT_HIGH_CARD: tuple[str, ...] = ()
EVENT_UNIVERSE_CONTEXT_LOW_CARD: tuple[str, ...] = EVENT_UNIVERSE_LOW_CARD
EVENT_UNIVERSE_CONTEXT_NUMERIC: tuple[str, ...] = EVENT_UNIVERSE_NUMERIC

EVENT_UNIVERSE_CONTEXT_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=EVENT_UNIVERSE_CONTEXT_HIGH_CARD,
    low_card_columns=EVENT_UNIVERSE_CONTEXT_LOW_CARD,
    numeric_columns=EVENT_UNIVERSE_CONTEXT_NUMERIC,
    grain_column="event_key",
    split_column="primary_fold",
    embedding_groups=(),
)

EVENT_UNIVERSE_CONTEXT_SPEC: PretrainSpec = PretrainSpec(
    name="event_universe_context",
    dataset_name=EVENT_UNIVERSE_DATASET,
    head_specs=EVENT_UNIVERSE_HEADS_V6,
)

V8_PLAYER_GROUP_COLS: tuple[str, ...] = ("batter_id", "pitcher_id")

V8_OBSERVED_LOW_CARD: tuple[str, ...] = (
    "pa_result",
    "r1_advancement",
    "r2_advancement",
    "r3_advancement",
)

V8_OBSERVED_NUMERIC: tuple[str, ...] = (
    "outs_on_play_capped",
    "runs_on_play_capped",
)

EVENT_UNIVERSE_V8_HIGH_CARD: tuple[str, ...] = (
    *V8_PLAYER_GROUP_COLS,
    "park_id",
    "scorer",
)

EVENT_UNIVERSE_V8_LOW_CARD: tuple[str, ...] = (
    *EVENT_UNIVERSE_LOW_CARD,
    *V8_OBSERVED_LOW_CARD,
)

EVENT_UNIVERSE_V8_NUMERIC: tuple[str, ...] = (
    *EVENT_UNIVERSE_NUMERIC,
    *V8_OBSERVED_NUMERIC,
)

EVENT_UNIVERSE_V8_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=EVENT_UNIVERSE_V8_HIGH_CARD,
    low_card_columns=EVENT_UNIVERSE_V8_LOW_CARD,
    numeric_columns=EVENT_UNIVERSE_V8_NUMERIC,
    grain_column="event_key",
    split_column="primary_fold",
    embedding_groups=(("player", V8_PLAYER_GROUP_COLS),),
)

EVENT_UNIVERSE_V8_HEADS: tuple[HeadSpec, ...] = (
    HeadSpec(
        name="trajectory_remapped",
        target_column="trajectory_remapped",
        kind="multiclass",
        configured_class_labels=TRAJECTORY_CLASS_LABELS,
        class_universe_source="configured",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_general",
        target_column="batted_location_general",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_depth",
        target_column="batted_location_depth",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_location_edge",
        target_column="batted_location_edge",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
    HeadSpec(
        name="batted_to_fielder_class",
        target_column="batted_to_fielder_class",
        kind="multiclass",
        class_universe_source="train_distinct",
        loss_weight=1.0,
    ),
)

V8_BATTED_BALL_PA_RESULTS: tuple[str, ...] = (
    "Single",
    "Double",
    "GroundRuleDouble",
    "Triple",
    "HomeRun",
    "InsideTheParkHomeRun",
    "InPlayOut",
    "FieldersChoice",
    "ReachedOnError",
    "SacrificeFly",
    "SacrificeHit",
)

V8_ROW_FILTER_PREDICATE: str = (
    "pa_result IN ("
    + ", ".join(f"'{v}'" for v in V8_BATTED_BALL_PA_RESULTS)
    + ")"
)

EVENT_UNIVERSE_V8_SPEC: PretrainSpec = PretrainSpec(
    name="event_universe_v8",
    dataset_name=EVENT_UNIVERSE_DATASET,
    head_specs=EVENT_UNIVERSE_V8_HEADS,
    row_filter_predicate=V8_ROW_FILTER_PREDICATE,
)

EVENT_UNIVERSE_V8_CONTEXT_LAYOUT: FeatureLayout = FeatureLayout(
    high_card_columns=(),
    low_card_columns=EVENT_UNIVERSE_LOW_CARD,
    numeric_columns=EVENT_UNIVERSE_NUMERIC,
    grain_column="event_key",
    split_column="primary_fold",
    embedding_groups=(),
)

EVENT_UNIVERSE_V8_CONTEXT_SPEC: PretrainSpec = PretrainSpec(
    name="event_universe_v8_context",
    dataset_name=EVENT_UNIVERSE_DATASET,
    head_specs=EVENT_UNIVERSE_V8_HEADS,
    row_filter_predicate=V8_ROW_FILTER_PREDICATE,
)


_PRETRAIN_SPECS: dict[str, PretrainSpec] = {
    EVENT_UNIVERSE_SPEC.name: EVENT_UNIVERSE_SPEC,
    EVENT_UNIVERSE_CONTEXT_SPEC.name: EVENT_UNIVERSE_CONTEXT_SPEC,
    EVENT_UNIVERSE_V8_SPEC.name: EVENT_UNIVERSE_V8_SPEC,
    EVENT_UNIVERSE_V8_CONTEXT_SPEC.name: EVENT_UNIVERSE_V8_CONTEXT_SPEC,
}

_PRETRAIN_LAYOUTS: dict[str, FeatureLayout] = {
    EVENT_UNIVERSE_SPEC.name: EVENT_UNIVERSE_LAYOUT,
    EVENT_UNIVERSE_CONTEXT_SPEC.name: EVENT_UNIVERSE_CONTEXT_LAYOUT,
    EVENT_UNIVERSE_V8_SPEC.name: EVENT_UNIVERSE_V8_LAYOUT,
    EVENT_UNIVERSE_V8_CONTEXT_SPEC.name: EVENT_UNIVERSE_V8_CONTEXT_LAYOUT,
}


def get_pretrain_spec(name: str) -> PretrainSpec:
    try:
        return _PRETRAIN_SPECS[name]
    except KeyError as exc:
        known = ", ".join(sorted(_PRETRAIN_SPECS))
        raise KeyError(f"unknown pretrain target {name!r}. known: {known}") from exc


def get_pretrain_layout(name: str) -> FeatureLayout:
    try:
        return _PRETRAIN_LAYOUTS[name]
    except KeyError as exc:
        known = ", ".join(sorted(_PRETRAIN_LAYOUTS))
        raise KeyError(
            f"no FeatureLayout registered for pretrain target {name!r}. registered: {known}"
        ) from exc


def all_pretrain_names() -> tuple[str, ...]:
    return tuple(sorted(_PRETRAIN_SPECS))
