"""Importing deep modules must not pull MLflow into sys.modules.

Phase 3 ships without MLflow on purpose — the artifact-id directory +
manifest.json + structured logs replace the file backend.
"""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


BC_DIR = Path(__file__).resolve().parents[3]

SNIPPET = """
import sys
import python_models.statistical.deep as _d
import python_models.statistical.deep.training as _t
import python_models.statistical.deep.feature_layout as _fl
import python_models.statistical.deep.io as _io
import python_models.statistical.deep.artifacts as _a
import python_models.statistical.deep.registry as _r
import python_models.statistical.deep.target_spec as _ts
import python_models.statistical.deep.leakage_probes as _lp
loaded = sorted(k for k in sys.modules if k.startswith('mlflow'))
assert not loaded, f'mlflow loaded: {loaded}'
print('OK')
"""


def test_deep_imports_do_not_load_mlflow() -> None:
    env = os.environ.copy()
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{BC_DIR}:{existing}" if existing else str(BC_DIR)
    )
    _ = env.setdefault("KERAS_BACKEND", "torch")
    result = subprocess.run(
        [sys.executable, "-c", SNIPPET],
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    assert result.returncode == 0, (
        f"stdout={result.stdout!r} stderr={result.stderr!r}"
    )
    assert "OK" in result.stdout
