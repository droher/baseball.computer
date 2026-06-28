"""Justfile smoke checks for the Phase-3 Pattern-C cleanup.

Pattern C named-positional recipes interpolate ``{{ ARG }}`` AND forward
``"$@"``. Without ``shift N`` the positional args double-pass through the
underlying command. These tests catch regressions in the rendered shell.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]


def _just(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["just", "--dry-run", *args],
        cwd=REPO_ROOT,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


@pytest.mark.parametrize(
    "recipe,positional,expected_substring",
    [
        (
            "prepare-dataset",
            ["model_input_geometry", "aid-1"],
            "--dataset model_input_geometry --artifact-id aid-1",
        ),
        (
            "run-eda",
            ["model_input_geometry", "ds-aid", "eda-aid"],
            "--dataset model_input_geometry --dataset-artifact ds-aid --artifact-id eda-aid",
        ),
        (
            "check-split-leakage",
            ["model_input_geometry", "ds-aid"],
            "--dataset model_input_geometry --dataset-artifact ds-aid",
        ),
        (
            "check-publication-gate",
            ["config.json", "report.json"],
            "--model-config config.json --eda-report report.json",
        ),
        (
            "fit-deep",
            ["geometry_trajectory", "ds-aid", "deep-aid"],
            "--target geometry_trajectory --dataset-artifact ds-aid --artifact-id deep-aid",
        ),
        (
            "publish-manifest",
            ["dl_proposal_trajectory", "deep-aid"],
            "--model dl_proposal_trajectory --artifact-id deep-aid",
        ),
        (
            "validate-artifact",
            ["deep-aid"],
            "--artifact-id deep-aid",
        ),
    ],
)
def test_recipe_does_not_double_pass_positional_args(
    recipe: str, positional: list[str], expected_substring: str
) -> None:
    result = _just(recipe, *positional)
    assert result.returncode == 0, result.stderr
    output = result.stdout
    assert expected_substring in output, (
        f"recipe {recipe!r} rendered without expected interpolation; "
        f"output={output!r}"
    )
    # Pattern C: the named args should appear exactly once each.
    for arg in positional:
        assert output.count(arg) == 1, (
            f"recipe {recipe!r} positional arg {arg!r} appeared "
            f"{output.count(arg)} times (Pattern C requires shift + exactly one occurrence)."
        )
