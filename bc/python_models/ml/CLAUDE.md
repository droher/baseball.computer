# ML pipeline (Keras 3 + PyTorch + MLflow)

- `KERAS_BACKEND=torch` is set once in `ml/__init__.py`. Don't re-set it elsewhere.
- `TargetSpec` (`features.py`) carries `kind ∈ {multiclass, binary, regression}`. `model_factory._make_outputs_layer` dispatches softmax / sigmoid / linear heads off `kind`. Per-target wrappers stay thin: `model_<target>.py`, `scripts/train_<target>.py`.
- Training is offline. Run `scripts/train_<name>.py` to produce the artifact JSON at `bc/python_models/ml/artifacts/<name>.json`.
- No SQLMesh model scores these targets, so the build never imports Keras or torch. `prediction.Scorer` and `artifact_exists` remain for offline scoring.
- ML deps live in the `ml` uv group (`apache-hamilton`, `mlflow`, `keras`, `torch`, `scikit-learn`).
