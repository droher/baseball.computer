"""Binary DeepTargetSpec guard rejects a non-binary label space."""

from __future__ import annotations

import polars as pl
import pytest

from python_models.statistical.deep.training import _assert_binary_target_values


def test_binary_values_pass() -> None:
    frame = pl.DataFrame({"y": [0, 1, 1, 0, None]})
    _assert_binary_target_values(frame, target_column="y")


def test_boolean_values_pass() -> None:
    frame = pl.DataFrame({"y": [True, False, True]})
    _assert_binary_target_values(frame, target_column="y")


def test_multiclass_values_raise() -> None:
    frame = pl.DataFrame({"y": [0, 1, 2, 3]})
    with pytest.raises(ValueError, match="non-binary label values"):
        _assert_binary_target_values(frame, target_column="y")


def test_error_lists_the_offending_values() -> None:
    frame = pl.DataFrame({"y": [0, 1, 5, 7]})
    with pytest.raises(ValueError) as excinfo:
        _assert_binary_target_values(frame, target_column="y")
    message = str(excinfo.value)
    assert "5" in message
    assert "7" in message
