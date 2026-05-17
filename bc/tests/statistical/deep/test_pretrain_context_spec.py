"""Context-only PretrainSpec invariants for v7 stage-1."""

from __future__ import annotations

from python_models.statistical.deep.pretrain.targets import (
    EVENT_UNIVERSE_CONTEXT_LAYOUT,
    EVENT_UNIVERSE_CONTEXT_SPEC,
    EVENT_UNIVERSE_HEADS_V6,
    EVENT_UNIVERSE_LAYOUT,
    EVENT_UNIVERSE_SPEC,
    get_pretrain_layout,
    get_pretrain_spec,
)


def test_context_layout_has_no_high_card() -> None:
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.high_card_columns == ()
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.embedding_groups == ()
    assert EVENT_UNIVERSE_CONTEXT_LAYOUT.embedding_unit_names() == ()


def test_context_layout_reuses_full_low_card_and_numeric() -> None:
    assert (
        EVENT_UNIVERSE_CONTEXT_LAYOUT.low_card_columns
        == EVENT_UNIVERSE_LAYOUT.low_card_columns
    )
    assert (
        EVENT_UNIVERSE_CONTEXT_LAYOUT.numeric_columns
        == EVENT_UNIVERSE_LAYOUT.numeric_columns
    )


def test_context_spec_shares_heads_with_full_spec() -> None:
    assert EVENT_UNIVERSE_CONTEXT_SPEC.head_specs == EVENT_UNIVERSE_HEADS_V6


def test_context_spec_uses_same_dataset() -> None:
    assert EVENT_UNIVERSE_CONTEXT_SPEC.dataset_name == EVENT_UNIVERSE_SPEC.dataset_name


def test_context_spec_registered() -> None:
    assert get_pretrain_spec("event_universe_context") is EVENT_UNIVERSE_CONTEXT_SPEC
    assert get_pretrain_layout("event_universe_context") is EVENT_UNIVERSE_CONTEXT_LAYOUT


def test_context_layout_passes_pre_event_validator() -> None:
    from python_models.statistical.deep.feature_layout import validate_pre_event

    validate_pre_event(EVENT_UNIVERSE_CONTEXT_LAYOUT)
