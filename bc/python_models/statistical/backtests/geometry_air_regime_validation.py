from __future__ import annotations

import argparse
import hashlib
import json
import logging
from importlib.metadata import version
from pathlib import Path
from typing import cast

import numpy as np

from python_models.statistical.backtests.geometry_air_dirichlet import (
    QuadratureConvergenceError,
    sample_posterior,
    validate_quadrature_convergence,
)

LOGGER = logging.getLogger(__name__)


def converged_order(counts: list[list[int]], concentration: float) -> int:
    for lower, higher in ((64, 128), (128, 256), (256, 512)):
        try:
            validate_quadrature_convergence(counts, concentration, lower, higher)
        except QuadratureConvergenceError:
            if higher == 512:
                raise
        else:
            return higher
    raise RuntimeError("quadrature refinement exhausted")


def run_recovery(output: Path, *, repetitions: int, draws: int = 4096) -> None:
    if repetitions < 1 or draws < 1:
        raise ValueError("repetitions and draws must be positive")
    output.mkdir(parents=True, exist_ok=False)
    seed = 20260911
    config = {
        "repetitions": repetitions,
        "draws": draws,
        "seed": seed,
        "training_events_by_regime": [20, 200],
        "future_events_per_regime": 100,
        "concentrations": [3.0, 30.0, 300.0],
        "operational_smoke": repetitions != 200 or draws != 4096,
        "source_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "kernel_sha256": hashlib.sha256(
            Path(__file__).with_name("geometry_air_dirichlet.py").read_bytes()
        ).hexdigest(),
        "protocol_sha256": hashlib.sha256(
            (
                Path(__file__).resolve().parents[4]
                / "docs/geometry-air-regime-posterior-protocol.md"
            ).read_bytes()
        ).hexdigest(),
        "package_versions": {name: version(name) for name in ("numpy", "scipy")},
    }
    (output / "config.json").write_text(json.dumps(config, indent=2) + "\n")
    rng = np.random.default_rng(seed)
    records: list[dict[str, object]] = []
    with (output / "recovery.jsonl").open("w") as stream:
        for concentration in (3.0, 30.0, 300.0):
            for replicate in range(repetitions):
                q = rng.dirichlet(np.ones(3))
                truth = rng.dirichlet(concentration * q, size=2)
                counts = [
                    rng.multinomial(n, truth[g]).tolist()
                    for g, n in enumerate((20, 200))
                ]
                order = converged_order(counts, concentration)
                posterior_seed = int(cast(np.int64, rng.integers(0, 2**63 - 1)))
                samples = sample_posterior(
                    counts, concentration, order, draws, posterior_seed
                )
                future = np.stack([rng.multinomial(100, p) for p in truth])
                replicated = np.empty(samples.shape, dtype=np.int64)
                for g in range(2):
                    for d in range(draws):
                        replicated[d, g] = rng.multinomial(100, samples[d, g])
                if not np.all(replicated.sum(axis=2) == 100):
                    raise RuntimeError("replicated counts do not conserve events")
                for kind, distribution, reference in (
                    ("parameter", samples, truth),
                    ("future_count", replicated, future),
                ):
                    lower, upper = np.quantile(distribution, [0.025, 0.975], axis=0)
                    for g in range(2):
                        for c in range(3):
                            record: dict[str, object] = {
                                "concentration": concentration,
                                "replicate": replicate,
                                "kind": kind,
                                "regime_index": g,
                                "class_index": c,
                                "truth": float(reference[g, c]),
                                "lower": float(lower[g, c]),
                                "upper": float(upper[g, c]),
                                "covered": bool(
                                    lower[g, c] <= reference[g, c] <= upper[g, c]
                                ),
                                "width": float(upper[g, c] - lower[g, c]),
                                "quadrature_order": order,
                            }
                            records.append(record)
                            stream.write(json.dumps(record) + "\n")
                stream.flush()
                if (replicate + 1) % 10 == 0 or replicate + 1 == repetitions:
                    LOGGER.info(
                        "concentration=%g completed=%d/%d",
                        concentration,
                        replicate + 1,
                        repetitions,
                    )
    summaries: list[dict[str, object]] = []
    for concentration in (3.0, 30.0, 300.0):
        for kind in ("parameter", "future_count"):
            for g in range(2):
                for c in range(3):
                    group = [
                        r
                        for r in records
                        if r["concentration"] == concentration
                        and r["kind"] == kind
                        and r["regime_index"] == g
                        and r["class_index"] == c
                    ]
                    covered = sum(r["covered"] is True for r in group)
                    rate = covered / len(group)
                    z = 1.959963984540054
                    denominator = 1 + z**2 / len(group)
                    center = (rate + z**2 / (2 * len(group))) / denominator
                    half = (
                        z
                        * np.sqrt(
                            rate * (1 - rate) / len(group)
                            + z**2 / (4 * len(group) ** 2)
                        )
                        / denominator
                    )
                    summaries.append(
                        {
                            "concentration": concentration,
                            "kind": kind,
                            "regime_index": g,
                            "class_index": c,
                            "replicates": len(group),
                            "coverage": rate,
                            "coverage_wilson_95": [
                                float(center - half),
                                float(center + half),
                            ],
                            "mean_width": float(
                                np.mean([cast(float, r["width"]) for r in group])
                            ),
                            "necessary_screen_pass": rate >= 0.90,
                        }
                    )
    result = {
        "status": "complete",
        "operational_smoke": config["operational_smoke"],
        "all_necessary_screens_pass": all(
            r["necessary_screen_pass"] for r in summaries
        ),
        "summaries": summaries,
    }
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    LOGGER.info("recovery complete: %s", output)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    run_recovery(args.output, repetitions=5 if args.smoke else 200)


if __name__ == "__main__":
    main()
