import json
import math
from pathlib import Path

import pytest

from python_models.statistical.backtests.geometry_air_regime_validation import (
    run_recovery,
)


def test_recovery_replays_and_reports_intervals(tmp_path: Path) -> None:
    first, second = tmp_path / "first", tmp_path / "second"
    for output in (first, second):
        run_recovery(output, repetitions=2, draws=256)
    for name in ("config.json", "recovery.jsonl", "summary.json"):
        assert (first / name).read_bytes() == (second / name).read_bytes()
    records = [
        json.loads(row) for row in (first / "recovery.jsonl").read_text().splitlines()
    ]
    assert records
    for record in records:
        assert record["lower"] <= record["upper"]
        assert math.isclose(record["width"], record["upper"] - record["lower"])
        assert record["covered"] == (
            record["lower"] <= record["truth"] <= record["upper"]
        )
        upper_bound = 1 if record["kind"] == "parameter" else 100
        assert 0 <= record["lower"] <= record["upper"] <= upper_bound
    summary = json.loads((first / "summary.json").read_text())
    assert summary["status"] == "complete"
    assert summary["operational_smoke"] is True
    for row in summary["summaries"]:
        assert 0 <= row["coverage"] <= 1
        assert row["coverage_wilson_95"][0] <= row["coverage"] + 1e-14
        assert row["coverage"] <= row["coverage_wilson_95"][1] + 1e-14
    with pytest.raises(FileExistsError):
        run_recovery(first, repetitions=2, draws=256)


@pytest.mark.parametrize(("repetitions", "draws"), [(0, 1), (1, 0)])
def test_recovery_rejects_empty_runs(
    tmp_path: Path, repetitions: int, draws: int
) -> None:
    output = tmp_path / "invalid"
    with pytest.raises(ValueError, match="positive"):
        run_recovery(output, repetitions=repetitions, draws=draws)
    assert not output.exists()
