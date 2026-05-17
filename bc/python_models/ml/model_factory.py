"""Generic Keras model factory for ML targets.

One trunk (per-categorical embeddings, one-hot for low-card, normalized
numerics, two dense ReLU blocks) terminates in a target-specific output
layer chosen from the `TargetSpec.kind`. The same trunk powers two
caller entry points:

* `build_model(...)` — single-head per-target Keras model.
* `build_pretrain_model(...)` — multi-head pretext model used to warm
  start shared entity embeddings before per-target fine-tuning.

`set_pretrained_embeddings(...)` reads a pretrain artifact directory
and copies aligned rows into a per-target model's Embedding layers
without overwriting unseen entities.
"""

from __future__ import annotations

import python_models.ml  # noqa: F401  # set KERAS_BACKEND before keras import

import json
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal

import keras
import numpy as np
import polars as pl
from keras import layers
from numpy.typing import NDArray

from python_models.ml.features import (
    FeatureLayout,
    TargetSpec,
)


EMBEDDING_DIM_CAP: int = 128
EMBEDDING_L2: float = 1e-5
DENSE_L2: float = 1e-5
SPATIAL_DROPOUT_RATE: float = 0.05
DEEP_WIDTHS: tuple[int, ...] = (512, 256)
DEEP_DROPOUT: tuple[float, ...] = (0.2, 0.1)
CROSS_DEPTH: int = 2
CROSS_LOW_RANK: int = 128
ROLE_EMBED_DIM: int = 8
INITIAL_LEARNING_RATE: float = 1e-3
COSINE_FLOOR_ALPHA: float = 0.01

VICREG_VARIANCE_WEIGHT: float = 1.0
VICREG_COVARIANCE_WEIGHT: float = 0.04
VICREG_VARIANCE_GAMMA: float = 1.0

SIGLIP_INIT_LOG_TEMP: float = 0.6931472  # log(2)
SIGLIP_INIT_BIAS: float = -1.0

_log = logging.getLogger(__name__)


def _embedding_dim(vocab_size: int) -> int:
    override = os.environ.get("BC_DEEP_FORCE_EMBED_DIM")
    if override is not None:
        return int(override)
    return min(EMBEDDING_DIM_CAP, int(round(vocab_size**0.5)) + 1)


@keras.saving.register_keras_serializable(package="bc.deep")
class RoleBias(layers.Layer):
    """Adds a learnable per-slot bias to the last dim of the input.

    Used to differentiate the 13 player-column slices that share one
    Embedding table (batter, pitcher, 8 fielders, 3 runners). Without
    this, downstream cross / Hadamard layers cannot distinguish which
    slot a given 96-D embedding vector came from.
    """

    def __init__(self, dim: int, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._dim: int = int(dim)
        self._bias: keras.Variable | None = None

    def build(self, input_shape: Any) -> None:
        self._bias = self.add_weight(
            shape=(self._dim,),
            initializer="zeros",
            name="bias",
        )
        super().build(input_shape)

    def call(self, inputs: Any) -> Any:
        return inputs + self._bias

    def compute_output_shape(self, input_shape: Any) -> Any:
        return input_shape

    def get_config(self) -> dict[str, Any]:
        config = super().get_config()
        config.update({"dim": self._dim})
        return config


@keras.saving.register_keras_serializable(package="bc.deep")
class CrossLayer(layers.Layer):
    """DCN-V2 low-rank cross layer: x_{l+1} = x0 ⊙ (W xl + b) + xl, W = U V^T.

    Replaces the DCN-v1 vector form ``x0 * (xl · w_scalar)`` which collapsed
    xl to one scalar; the low-rank ``W = U V^T`` (effective rank ``k``)
    captures pair-specific interactions across dims. ReLU activates the
    projected xl before the Hadamard product with x0 (DCN-V2 §3.2).
    """

    def __init__(self, low_rank: int = CROSS_LOW_RANK, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._k: int = int(low_rank)
        self._u: keras.Variable | None = None
        self._v: keras.Variable | None = None
        self._b: keras.Variable | None = None

    def build(self, input_shape: Any) -> None:
        x0_shape, xl_shape = input_shape
        d = int(x0_shape[-1])
        if int(xl_shape[-1]) != d:
            raise ValueError(
                f"CrossLayer expects x0 and xl with matching last dim; got {x0_shape} vs {xl_shape}"
            )
        self._v = self.add_weight(
            shape=(d, self._k),
            initializer="glorot_uniform",
            regularizer=keras.regularizers.l2(DENSE_L2),
            name="v",
        )
        self._u = self.add_weight(
            shape=(self._k, d),
            initializer="glorot_uniform",
            regularizer=keras.regularizers.l2(DENSE_L2),
            name="u",
        )
        self._b = self.add_weight(
            shape=(d,),
            initializer="zeros",
            name="b",
        )
        super().build(input_shape)

    def call(self, inputs: Any) -> Any:
        x0, xl = inputs
        proj = keras.ops.matmul(xl, self._v)
        proj = keras.activations.relu(proj)
        wx = keras.ops.matmul(proj, self._u)
        return x0 * (wx + self._b) + xl

    def compute_output_shape(self, input_shape: Any) -> Any:
        return input_shape[1]

    def get_config(self) -> dict[str, Any]:
        config = super().get_config()
        config.update({"low_rank": self._k})
        return config


@keras.saving.register_keras_serializable(package="bc.deep")
def sparse_categorical_focal_loss(gamma: float = 2.0) -> keras.losses.Loss:
    def _loss(y_true: Any, y_pred: Any) -> Any:
        eps = 1e-7
        y_pred_clipped = keras.ops.clip(y_pred, eps, 1.0 - eps)
        y_true_int = keras.ops.cast(y_true, "int32")
        y_true_int = keras.ops.reshape(y_true_int, (-1,))
        idx = keras.ops.expand_dims(y_true_int, axis=-1)
        p_t = keras.ops.take_along_axis(y_pred_clipped, idx, axis=-1)
        p_t = keras.ops.reshape(p_t, (-1,))
        focal_weight = keras.ops.power(1.0 - p_t, gamma)
        return -focal_weight * keras.ops.log(p_t)

    _loss.__name__ = f"sparse_categorical_focal_loss_g{gamma:.2f}".replace(".", "_")
    return _loss


@dataclass(frozen=True)
class _Head:
    output: keras.KerasTensor
    loss: keras.losses.Loss | Any
    metrics: tuple[keras.metrics.Metric, ...]


def _make_outputs_layer(
    trunk: keras.KerasTensor,
    target_spec: TargetSpec,
    num_classes: int,
    *,
    loss_type: str = "cross_entropy",
    focal_gamma: float = 2.0,
) -> _Head:
    if target_spec.kind == "multiclass":
        out = layers.Dense(
            num_classes, activation="softmax", name=target_spec.name
        )(trunk)
        if loss_type == "focal":
            loss = sparse_categorical_focal_loss(gamma=focal_gamma)
        else:
            loss = keras.losses.SparseCategoricalCrossentropy()
        return _Head(
            output=out,
            loss=loss,
            metrics=(keras.metrics.SparseCategoricalAccuracy(),),
        )
    if target_spec.kind == "binary":
        out = layers.Dense(1, activation="sigmoid", name=target_spec.name)(trunk)
        return _Head(
            output=out,
            loss=keras.losses.BinaryCrossentropy(),
            metrics=(keras.metrics.BinaryAccuracy(),),
        )
    if target_spec.kind == "regression":
        out = layers.Dense(1, activation="linear", name=target_spec.name)(trunk)
        return _Head(
            output=out,
            loss=keras.losses.MeanSquaredError(),
            metrics=(
                keras.metrics.MeanSquaredError(name="mse"),
                keras.metrics.MeanAbsoluteError(name="mae"),
            ),
        )
    raise ValueError(f"unsupported target kind {target_spec.kind!r}")


def _make_optimizer(
    *, steps_per_epoch: int | None, total_epochs: int | None
) -> keras.optimizers.Optimizer:
    if steps_per_epoch is not None and total_epochs is not None and steps_per_epoch > 0 and total_epochs > 0:
        schedule = keras.optimizers.schedules.CosineDecay(
            initial_learning_rate=INITIAL_LEARNING_RATE,
            decay_steps=steps_per_epoch * total_epochs,
            alpha=COSINE_FLOOR_ALPHA,
        )
        return keras.optimizers.Adam(learning_rate=schedule)
    return keras.optimizers.Adam(learning_rate=INITIAL_LEARNING_RATE)


@dataclass(frozen=True)
class _Backbone:
    inputs: dict[str, keras.KerasTensor]
    trunk: keras.KerasTensor
    embeddings: dict[str, layers.Embedding]


def _build_backbone(
    *,
    layout: FeatureLayout,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
) -> _Backbone:
    inputs: dict[str, keras.KerasTensor] = {}
    branches: list[keras.KerasTensor] = []
    embeddings: dict[str, layers.Embedding] = {}

    embedding_reg = keras.regularizers.l2(EMBEDDING_L2)
    dense_reg = keras.regularizers.l2(DENSE_L2)

    group_layers: dict[str, layers.Embedding] = {}
    for group_name, _cols in layout.embedding_groups:
        size = vocab_sizes[group_name]
        group_layers[group_name] = layers.Embedding(
            input_dim=size,
            output_dim=_embedding_dim(size),
            embeddings_regularizer=embedding_reg,
            name=f"embed_{group_name}",
        )

    slot_embeddings: dict[str, keras.KerasTensor] = {}
    for col in layout.high_card_columns:
        group_name = layout.group_for_column(col)
        if group_name is not None:
            emb_layer = group_layers[group_name]
        else:
            size = vocab_sizes[col]
            emb_layer = layers.Embedding(
                input_dim=size,
                output_dim=_embedding_dim(size),
                embeddings_regularizer=embedding_reg,
                name=f"embed_{col}",
            )
        embeddings[col] = emb_layer
        inp = keras.Input(shape=(1,), dtype="int64", name=col)
        emb = emb_layer(inp)
        emb = layers.SpatialDropout1D(
            SPATIAL_DROPOUT_RATE, name=f"sdrop_{col}"
        )(emb)
        if group_name is not None:
            emb = RoleBias(int(emb.shape[-1]), name=f"role_{col}")(emb)
        slot_embeddings[col] = emb
        branches.append(layers.Flatten(name=f"flatten_{col}")(emb))
        inputs[col] = inp

    for col in layout.low_card_columns:
        size = vocab_sizes[col]
        inp = keras.Input(shape=(1,), dtype="int64", name=col)
        one_hot = layers.CategoryEncoding(
            num_tokens=size,
            output_mode="one_hot",
            name=f"onehot_{col}",
        )(inp)
        branches.append(layers.Flatten(name=f"flatten_{col}")(one_hot))
        inputs[col] = inp

    for col in layout.numeric_columns:
        inp = keras.Input(shape=(1,), dtype="float32", name=col)
        norm = layers.Normalization(
            mean=numeric_means[col],
            variance=numeric_variances[col],
            name=f"norm_{col}",
        )(inp)
        branches.append(norm)
        inputs[col] = inp

    concat = layers.Concatenate(name="concat")(branches)
    x0 = layers.LayerNormalization(name="trunk_layernorm")(concat)

    cross_depth_eff = int(os.environ.get("BC_PRETRAIN_CROSS_DEPTH", CROSS_DEPTH))
    cross = x0
    for i in range(cross_depth_eff):
        cross = CrossLayer(name=f"cross_{i + 1}")([x0, cross])

    deep = x0
    for idx, (width, dropout) in enumerate(
        zip(DEEP_WIDTHS, DEEP_DROPOUT, strict=True), start=1
    ):
        deep = layers.Dense(
            width,
            activation="relu",
            kernel_regularizer=dense_reg,
            name=f"deep_{idx}",
        )(deep)
        if dropout > 0:
            deep = layers.Dropout(dropout, name=f"deep_dropout_{idx}")(deep)

    merged = layers.Concatenate(name="cross_deep_concat")([cross, deep])
    return _Backbone(inputs=inputs, trunk=merged, embeddings=embeddings)


def build_model(
    *,
    target_spec: TargetSpec,
    layout: FeatureLayout,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
    num_classes: int,
    steps_per_epoch: int | None = None,
    total_epochs: int | None = None,
    loss_type: str = "cross_entropy",
    focal_gamma: float = 2.0,
) -> keras.Model:
    backbone = _build_backbone(
        layout=layout,
        vocab_sizes=vocab_sizes,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
    )
    head = _make_outputs_layer(
        backbone.trunk,
        target_spec,
        num_classes,
        loss_type=loss_type,
        focal_gamma=focal_gamma,
    )

    model = keras.Model(
        inputs=backbone.inputs, outputs=head.output, name=target_spec.name
    )
    optimizer = _make_optimizer(
        steps_per_epoch=steps_per_epoch, total_epochs=total_epochs
    )
    model.compile(
        optimizer=optimizer,
        loss=head.loss,
        weighted_metrics=list(head.metrics),
    )
    return model


PretrainHeadKind = Literal["multiclass", "binary"]


@dataclass(frozen=True)
class PretrainHeadBuild:
    name: str
    kind: PretrainHeadKind
    num_classes: int
    loss_weight: float = 1.0


def _make_pretrain_schedule(
    *,
    initial_lr: float,
    total_steps: int,
    warmup_frac: float = 0.05,
    alpha: float = 0.1,
) -> Any:
    if total_steps <= 0:
        return initial_lr
    warmup_steps = max(1, int(total_steps * warmup_frac))
    decay_steps = max(1, total_steps - warmup_steps)
    return keras.optimizers.schedules.CosineDecay(
        initial_learning_rate=0.0,
        decay_steps=decay_steps,
        alpha=alpha,
        warmup_target=initial_lr,
        warmup_steps=warmup_steps,
    )


def make_pretrain_optimizers(
    *,
    total_steps: int,
    trunk_lr: float = 1e-3,
    embed_lr: float = 5e-3,
) -> tuple[keras.optimizers.Optimizer, keras.optimizers.Optimizer]:
    trunk_schedule = _make_pretrain_schedule(
        initial_lr=trunk_lr, total_steps=total_steps
    )
    embed_schedule = _make_pretrain_schedule(
        initial_lr=embed_lr, total_steps=total_steps
    )
    trunk_opt = keras.optimizers.Adam(learning_rate=trunk_schedule, name="trunk_adam")
    embed_opt = keras.optimizers.Adam(learning_rate=embed_schedule, name="embed_adam")
    return trunk_opt, embed_opt


def _is_embedding_variable(var: Any) -> bool:
    name = getattr(var, "path", None) or getattr(var, "name", "")
    if not isinstance(name, str):
        name = str(name)
    return "embed_" in name or "log_sigma_" in name


@keras.saving.register_keras_serializable(package="bc.deep")
class PretrainModel(keras.Model):
    """Multi-head pretext model with Kendall-Gal uncertainty weighting + split optimizers.

    Per-head trainable ``log_sigma`` weights weight each head's NULL-masked loss as
    ``0.5 * exp(-log_sigma) * head_loss + 0.5 * log_sigma`` (Kendall & Gal 2018).

    ``train_step`` splits gradients by variable path: ``embed_*`` / ``log_sigma_*``
    go to ``embed_optimizer``; everything else goes to ``trunk_optimizer``.
    """

    def __init__(
        self,
        *,
        inputs: Any,
        outputs: Any,
        head_kinds: dict[str, PretrainHeadKind],
        name: str = "pretrain",
        **kwargs: Any,
    ) -> None:
        super().__init__(inputs=inputs, outputs=outputs, name=name, **kwargs)
        self._head_kinds: dict[str, PretrainHeadKind] = dict(head_kinds)
        self._log_sigma: dict[str, Any] = {}
        for head_name in self._head_kinds:
            self._log_sigma[head_name] = self.add_weight(
                shape=(),
                initializer="zeros",
                trainable=True,
                name=f"log_sigma_{head_name}",
            )
        self._head_loss_fns: dict[str, Any] = {}
        self._head_metric_objs: dict[str, list[keras.metrics.Metric]] = {}
        self._trunk_optimizer: keras.optimizers.Optimizer | None = None
        self._embed_optimizer: keras.optimizers.Optimizer | None = None
        self._vicreg_target_layer_names: tuple[str, ...] = ()
        self._siglip_input_col: str | None = None
        self._siglip_layer_name: str | None = None
        self._siglip_log_temp = self.add_weight(
            shape=(),
            initializer=keras.initializers.Constant(SIGLIP_INIT_LOG_TEMP),
            trainable=True,
            name="siglip_log_temp",
        )
        self._siglip_bias = self.add_weight(
            shape=(),
            initializer=keras.initializers.Constant(SIGLIP_INIT_BIAS),
            trainable=True,
            name="siglip_bias",
        )
        self._siglip_log_sigma = self.add_weight(
            shape=(),
            initializer="zeros",
            trainable=True,
            name="log_sigma_siglip",
        )
        self._train_step_counter: int = 0
        self._vicreg_stride: int = 1
        self._siglip_subsample: int | None = None

    def configure_pretrain(
        self,
        *,
        trunk_optimizer: keras.optimizers.Optimizer,
        embed_optimizer: keras.optimizers.Optimizer,
        head_losses: dict[str, Any],
        head_metrics: dict[str, list[keras.metrics.Metric]] | None = None,
        vicreg_target_layer_names: tuple[str, ...] = (),
        siglip_input_col: str | None = None,
        siglip_layer_name: str | None = None,
        vicreg_stride: int = 1,
        siglip_subsample: int | None = None,
    ) -> None:
        self._trunk_optimizer = trunk_optimizer
        self._embed_optimizer = embed_optimizer
        self._head_loss_fns = dict(head_losses)
        self._head_metric_objs = {
            name: list(mets) for name, mets in (head_metrics or {}).items()
        }
        self._vicreg_target_layer_names = tuple(vicreg_target_layer_names)
        self._siglip_input_col = siglip_input_col
        self._siglip_layer_name = siglip_layer_name
        self._vicreg_stride = max(1, int(vicreg_stride))
        self._siglip_subsample = (
            int(siglip_subsample) if siglip_subsample is not None and siglip_subsample > 0 else None
        )
        self.compile(
            optimizer=trunk_optimizer,
            loss=head_losses,
            weighted_metrics=self._head_metric_objs,
        )

    @property
    def metrics(self) -> list[keras.metrics.Metric]:
        base = list(super().metrics)
        for mets in self._head_metric_objs.values():
            for m in mets:
                if m not in base:
                    base.append(m)
        return base

    def _per_sample_head_loss(
        self, head_name: str, y_true: Any, y_pred: Any
    ) -> Any:
        kind = self._head_kinds[head_name]
        eps = 1e-7
        if kind == "multiclass":
            y_pred_clipped = keras.ops.clip(y_pred, eps, 1.0 - eps)
            y_true_int = keras.ops.cast(
                keras.ops.reshape(y_true, (-1,)), "int32"
            )
            idx = keras.ops.expand_dims(y_true_int, axis=-1)
            p_t = keras.ops.take_along_axis(y_pred_clipped, idx, axis=-1)
            p_t = keras.ops.reshape(p_t, (-1,))
            return -keras.ops.log(p_t)
        if kind == "binary":
            y_pred_flat = keras.ops.reshape(y_pred, (-1,))
            y_true_flat = keras.ops.cast(
                keras.ops.reshape(y_true, (-1,)), "float32"
            )
            y_pred_clipped = keras.ops.clip(y_pred_flat, eps, 1.0 - eps)
            return -(
                y_true_flat * keras.ops.log(y_pred_clipped)
                + (1.0 - y_true_flat) * keras.ops.log(1.0 - y_pred_clipped)
            )
        raise ValueError(f"unsupported head kind {kind!r}")

    def _siglip_loss(self, x: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
        if self._siglip_input_col is None or self._siglip_layer_name is None:
            return keras.ops.zeros(()), {}
        if self._siglip_input_col not in x:
            return keras.ops.zeros(()), {}
        try:
            embed_layer = self.get_layer(self._siglip_layer_name)
        except ValueError:
            return keras.ops.zeros(()), {}
        codes_raw = x[self._siglip_input_col]
        if self._siglip_subsample is not None:
            codes_raw = codes_raw[: self._siglip_subsample]
        codes = keras.ops.reshape(keras.ops.cast(codes_raw, "int32"), (-1,))
        emb = embed_layer(codes_raw)
        emb = keras.ops.reshape(emb, (keras.ops.shape(codes)[0], -1))
        norm = keras.ops.sqrt(
            keras.ops.sum(emb * emb, axis=-1, keepdims=True) + 1e-8
        )
        emb_n = emb / norm
        sim = keras.ops.matmul(emb_n, keras.ops.transpose(emb_n))
        codes_i = keras.ops.expand_dims(codes, axis=1)
        codes_j = keras.ops.expand_dims(codes, axis=0)
        same = keras.ops.cast(codes_i == codes_j, "float32")
        not_oov = keras.ops.cast(codes > 0, "float32")
        not_oov_i = keras.ops.expand_dims(not_oov, axis=1)
        not_oov_j = keras.ops.expand_dims(not_oov, axis=0)
        eye = keras.ops.eye(keras.ops.shape(codes)[0])
        pair_mask = (1.0 - eye) * not_oov_i * not_oov_j
        z = 2.0 * same - 1.0
        t = keras.ops.exp(self._siglip_log_temp)
        logits = t * sim + self._siglip_bias
        pair_loss = keras.ops.softplus(-z * logits)
        denom = keras.ops.maximum(keras.ops.sum(pair_mask), 1.0)
        loss = keras.ops.sum(pair_loss * pair_mask) / denom
        positive_pairs = keras.ops.sum(same * pair_mask)
        diag: dict[str, Any] = {
            "siglip_loss": loss,
            "siglip_temp": t,
            "siglip_bias": self._siglip_bias,
            "siglip_log_sigma": self._siglip_log_sigma,
            "siglip_pos_pairs": positive_pairs,
        }
        return loss, diag

    def _vicreg_loss(self) -> tuple[Any, dict[str, Any]]:
        if not self._vicreg_target_layer_names:
            return keras.ops.zeros(()), {}
        total = keras.ops.zeros(())
        diagnostics: dict[str, Any] = {}
        for layer_name in self._vicreg_target_layer_names:
            try:
                layer = self.get_layer(layer_name)
            except ValueError:
                continue
            weights_list = layer.weights
            if not weights_list:
                continue
            w = weights_list[0]
            mean = keras.ops.mean(w, axis=0, keepdims=True)
            centered = w - mean
            std = keras.ops.sqrt(
                keras.ops.mean(centered * centered, axis=0) + 1e-6
            )
            var_loss = keras.ops.mean(
                keras.ops.maximum(0.0, VICREG_VARIANCE_GAMMA - std)
            )
            n_rows = keras.ops.cast(keras.ops.shape(w)[0], "float32")
            dim = keras.ops.cast(keras.ops.shape(w)[1], "float32")
            cov = keras.ops.matmul(
                keras.ops.transpose(centered), centered
            ) / keras.ops.maximum(n_rows - 1.0, 1.0)
            squared = cov * cov
            diag_sum = keras.ops.sum(
                cov * cov * keras.ops.eye(int(w.shape[1]))
            )
            off_diag_sum_sq = keras.ops.sum(squared) - diag_sum
            cov_loss = off_diag_sum_sq / dim
            var_w = float(
                os.environ.get("BC_PRETRAIN_VICREG_VAR_WEIGHT", VICREG_VARIANCE_WEIGHT)
            )
            cov_w = float(
                os.environ.get("BC_PRETRAIN_VICREG_COV_WEIGHT", VICREG_COVARIANCE_WEIGHT)
            )
            total = total + var_w * var_loss + cov_w * cov_loss
            diagnostics[f"vicreg_var_{layer_name}"] = var_loss
            diagnostics[f"vicreg_cov_{layer_name}"] = cov_loss
            diagnostics[f"vicreg_std_mean_{layer_name}"] = keras.ops.mean(std)
        return total, diagnostics

    def _compute_uw_total(
        self,
        y_true: dict[str, Any],
        y_pred: dict[str, Any],
        sample_weight: dict[str, Any] | None,
    ) -> tuple[Any, dict[str, Any]]:
        head_losses: dict[str, Any] = {}
        for head_name in self._head_kinds:
            per_sample = self._per_sample_head_loss(
                head_name, y_true[head_name], y_pred[head_name]
            )
            if sample_weight is not None and head_name in sample_weight:
                sw = keras.ops.reshape(
                    keras.ops.cast(sample_weight[head_name], "float32"), (-1,)
                )
                weighted = per_sample * sw
                denom = keras.ops.maximum(keras.ops.sum(sw), 1.0)
                head_losses[head_name] = keras.ops.sum(weighted) / denom
            else:
                head_losses[head_name] = keras.ops.mean(per_sample)
        total = None
        for head_name, hl in head_losses.items():
            ls = self._log_sigma[head_name]
            precision = keras.ops.exp(-ls)
            contrib = 0.5 * precision * hl + 0.5 * ls
            total = contrib if total is None else total + contrib
        if total is None:
            total = keras.ops.zeros(())
        return total, head_losses

    def _split_apply_gradients(
        self, gradients: list[Any], trainable_vars: list[Any]
    ) -> None:
        embed_grads: list[Any] = []
        embed_vars: list[Any] = []
        trunk_grads: list[Any] = []
        trunk_vars: list[Any] = []
        for g, v in zip(gradients, trainable_vars, strict=True):
            if g is None:
                continue
            if _is_embedding_variable(v):
                embed_grads.append(g)
                embed_vars.append(v)
            else:
                trunk_grads.append(g)
                trunk_vars.append(v)
        if self._trunk_optimizer is None or self._embed_optimizer is None:
            raise RuntimeError(
                "PretrainModel.configure_pretrain must be called before fit()"
            )
        if trunk_vars:
            self._trunk_optimizer.apply(trunk_grads, trunk_vars)
        if embed_vars:
            self._embed_optimizer.apply(embed_grads, embed_vars)

    def train_step(self, data: Any) -> dict[str, Any]:
        x, y, sample_weight = keras.utils.unpack_x_y_sample_weight(data)
        if y is None:
            raise RuntimeError("PretrainModel.train_step requires per-head targets")

        for v in self.trainable_weights:
            t = v.value
            if hasattr(t, "grad") and t.grad is not None:
                t.grad.zero_()

        y_pred = self(x, training=True)
        uw_total, head_losses = self._compute_uw_total(
            dict(y), y_pred, dict(sample_weight) if sample_weight is not None else None
        )
        self._train_step_counter += 1
        if self._vicreg_stride <= 1 or (self._train_step_counter % self._vicreg_stride) == 0:
            vicreg_total, vicreg_diag = self._vicreg_loss()
        else:
            vicreg_total, vicreg_diag = keras.ops.zeros(()), {}
        siglip_loss, siglip_diag = self._siglip_loss(dict(x))
        siglip_precision = keras.ops.exp(-self._siglip_log_sigma)
        siglip_uw = 0.5 * siglip_precision * siglip_loss + 0.5 * self._siglip_log_sigma
        total_loss = uw_total + vicreg_total + siglip_uw

        total_loss.backward()

        trainable_vars = list(self.trainable_weights)
        gradients = [v.value.grad for v in trainable_vars]
        self._split_apply_gradients(gradients, trainable_vars)

        logs: dict[str, Any] = {"loss": total_loss}
        for hname, hl in head_losses.items():
            logs[f"{hname}_loss"] = hl
            logs[f"log_sigma_{hname}"] = self._log_sigma[hname]
        for k, v in vicreg_diag.items():
            logs[k] = v
        for k, v in siglip_diag.items():
            logs[k] = v
        for hname, mets in self._head_metric_objs.items():
            sw = None
            if sample_weight is not None:
                sw = sample_weight.get(hname)
            for m in mets:
                m.update_state(y[hname], y_pred[hname], sample_weight=sw)
                logs[f"{hname}_{m.name}"] = m.result()
        return logs

    def test_step(self, data: Any) -> dict[str, Any]:
        x, y, sample_weight = keras.utils.unpack_x_y_sample_weight(data)
        if y is None:
            raise RuntimeError("PretrainModel.test_step requires per-head targets")
        y_pred = self(x, training=False)
        uw_total, head_losses = self._compute_uw_total(
            dict(y), y_pred, dict(sample_weight) if sample_weight is not None else None
        )
        logs: dict[str, Any] = {"loss": uw_total}
        for hname, hl in head_losses.items():
            logs[f"{hname}_loss"] = hl
        for hname, mets in self._head_metric_objs.items():
            sw = None
            if sample_weight is not None:
                sw = sample_weight.get(hname)
            for m in mets:
                m.update_state(y[hname], y_pred[hname], sample_weight=sw)
                logs[f"{hname}_{m.name}"] = m.result()
        return logs

    def reset_metrics(self) -> None:
        super().reset_metrics()
        for mets in self._head_metric_objs.values():
            for m in mets:
                m.reset_state()

    def get_config(self) -> dict[str, Any]:
        cfg = super().get_config()
        cfg["head_kinds"] = self._head_kinds
        return cfg


def build_pretrain_model(
    *,
    layout: FeatureLayout,
    vocab_sizes: dict[str, int],
    numeric_means: dict[str, float],
    numeric_variances: dict[str, float],
    head_builds: tuple[PretrainHeadBuild, ...],
    steps_per_epoch: int | None = None,
    total_epochs: int | None = None,
    name: str = "pretrain",
    trunk_lr: float = 1e-3,
    embed_lr: float = 5e-3,
    offset_inputs: Mapping[str, keras.KerasTensor] | None = None,
    loss_type: str = "cross_entropy",
    focal_gamma: float = 2.0,
) -> PretrainModel:
    if not head_builds:
        raise ValueError("at least one pretext head is required")
    if loss_type not in ("cross_entropy", "focal"):
        raise ValueError(f"unsupported loss_type {loss_type!r}; expected 'cross_entropy' or 'focal'")

    backbone = _build_backbone(
        layout=layout,
        vocab_sizes=vocab_sizes,
        numeric_means=numeric_means,
        numeric_variances=numeric_variances,
    )

    inputs: dict[str, keras.KerasTensor] = dict(backbone.inputs)
    outputs: dict[str, keras.KerasTensor] = {}
    head_losses: dict[str, Any] = {}
    head_metrics: dict[str, list[keras.metrics.Metric]] = {}
    head_kinds: dict[str, PretrainHeadKind] = {}
    for head in head_builds:
        if head.kind == "multiclass":
            if head.num_classes < 2:
                raise ValueError(
                    f"multiclass head {head.name!r} needs num_classes>=2 (got {head.num_classes})"
                )
            logits = layers.Dense(
                head.num_classes, activation=None, name=f"{head.name}_logits"
            )(backbone.trunk)
            offset = offset_inputs.get(head.name) if offset_inputs else None
            if offset is not None:
                inputs[f"offset_{head.name}"] = offset
                logits = layers.Add(name=f"{head.name}_sum")([logits, offset])
            out = layers.Activation("softmax", name=head.name)(logits)
            outputs[head.name] = out
            if loss_type == "focal":
                head_losses[head.name] = sparse_categorical_focal_loss(gamma=focal_gamma)
            else:
                head_losses[head.name] = keras.losses.SparseCategoricalCrossentropy()
            head_metrics[head.name] = [
                keras.metrics.SparseCategoricalAccuracy(name="sparse_categorical_accuracy")
            ]
            head_kinds[head.name] = "multiclass"
        elif head.kind == "binary":
            logits = layers.Dense(1, activation=None, name=f"{head.name}_logits")(
                backbone.trunk
            )
            offset = offset_inputs.get(head.name) if offset_inputs else None
            if offset is not None:
                inputs[f"offset_{head.name}"] = offset
                logits = layers.Add(name=f"{head.name}_sum")([logits, offset])
            out = layers.Activation("sigmoid", name=head.name)(logits)
            outputs[head.name] = out
            head_losses[head.name] = keras.losses.BinaryCrossentropy()
            head_metrics[head.name] = [
                keras.metrics.BinaryAccuracy(name="binary_accuracy")
            ]
            head_kinds[head.name] = "binary"
        else:
            raise ValueError(f"unsupported pretext head kind {head.kind!r}")

    model = PretrainModel(
        inputs=inputs,
        outputs=outputs,
        head_kinds=head_kinds,
        name=name,
    )

    total_steps = 0
    if steps_per_epoch is not None and total_epochs is not None:
        total_steps = max(0, int(steps_per_epoch) * int(total_epochs))
    trunk_opt, embed_opt = make_pretrain_optimizers(
        total_steps=total_steps,
        trunk_lr=trunk_lr,
        embed_lr=embed_lr,
    )
    model.configure_pretrain(
        trunk_optimizer=trunk_opt,
        embed_optimizer=embed_opt,
        head_losses=head_losses,
        head_metrics=head_metrics,
    )
    return model


def set_pretrained_embeddings(
    model: keras.Model,
    *,
    pretrain_artifact_dir: Path,
    high_card_columns: tuple[str, ...],
    target_vocab_values: dict[str, tuple[str, ...]],
    embedding_groups: tuple[tuple[str, tuple[str, ...]], ...] = (),
) -> dict[str, dict[str, int]]:
    """Copy aligned embedding rows from a pretrain artifact into ``model``.

    The artifact emits ONE matrix per embedding unit (group name for
    grouped cols, column name otherwise). For each high-card column we
    look up its unit, find the matching pretrain matrix, then copy rows
    keyed by entity id. Target vocabularies are still per-column — copies
    are by entity id, so each consumer column receives the players that
    appear in its own vocabulary.

    Returns per-column alignment stats ``{col: {"aligned": n,
    "target_vocab_size": m, "pretrain_vocab_size": k}}``.
    """
    pretrain_artifact_dir = Path(pretrain_artifact_dir)
    embeddings_path = pretrain_artifact_dir / "exports" / "embeddings.parquet"
    vocab_path = pretrain_artifact_dir / "exports" / "vocab.json"
    manifest_path = pretrain_artifact_dir / "manifest.json"
    if not embeddings_path.exists():
        raise FileNotFoundError(f"pretrain embeddings parquet missing: {embeddings_path}")
    if not vocab_path.exists():
        raise FileNotFoundError(f"pretrain vocab.json missing: {vocab_path}")

    embeddings_df = pl.read_parquet(embeddings_path)
    pretrain_vocabs: dict[str, list[str]] = json.loads(
        vocab_path.read_text(encoding="utf-8")
    )

    pretrain_col_to_unit: dict[str, str] = {}
    if manifest_path.exists():
        try:
            manifest_payload = json.loads(manifest_path.read_text(encoding="utf-8"))
            groups_raw = manifest_payload.get("metadata", {}).get("embedding_groups_json")
            if isinstance(groups_raw, str) and groups_raw:
                for entry in json.loads(groups_raw):
                    if not isinstance(entry, list) or len(entry) != 2:
                        continue
                    name, cols = entry
                    for c in cols:
                        pretrain_col_to_unit[str(c)] = str(name)
        except (json.JSONDecodeError, OSError) as exc:
            _log.warning(
                "set_pretrained_embeddings could not parse pretrain manifest groups: %s",
                exc,
            )

    target_layer_unit: dict[str, str] = {}
    for col in high_card_columns:
        target_layer_unit[col] = col
    for group_name, cols in embedding_groups:
        for col in cols:
            target_layer_unit[col] = group_name

    pretrain_unit_for_col: dict[str, str] = {}
    for col in high_card_columns:
        local_unit = target_layer_unit[col]
        if local_unit in pretrain_vocabs:
            pretrain_unit_for_col[col] = local_unit
        elif col in pretrain_col_to_unit and pretrain_col_to_unit[col] in pretrain_vocabs:
            pretrain_unit_for_col[col] = pretrain_col_to_unit[col]
            _log.info(
                "set_pretrained_embeddings col=%s target_layer=embed_%s falling back to pretrain unit=%s",
                col,
                local_unit,
                pretrain_col_to_unit[col],
            )

    pretrain_unit_matrices: dict[str, NDArray[np.float64]] = {}
    pretrain_unit_index: dict[str, dict[str, int]] = {}
    for unit, ids in pretrain_vocabs.items():
        sub = embeddings_df.filter(pl.col("entity_type") == unit)
        if sub.height == 0:
            continue
        if sub.height != len(ids):
            raise ValueError(
                f"pretrain embeddings/vocab row count mismatch for {unit!r}: "
                f"parquet={sub.height} vocab={len(ids)}"
            )
        pretrain_unit_matrices[unit] = np.asarray(
            sub["embedding_value"].to_list(), dtype=np.float64
        )
        pretrain_unit_index[unit] = {eid: i for i, eid in enumerate(ids)}

    seen_target_layers: set[str] = set()
    stats: dict[str, dict[str, int]] = {}
    for col in high_card_columns:
        layer_unit = target_layer_unit[col]
        pretrain_unit = pretrain_unit_for_col.get(col)
        try:
            embed_layer = model.get_layer(f"embed_{layer_unit}")
        except ValueError:
            _log.warning(
                "set_pretrained_embeddings col=%s layer_unit=%s: no embed_%s layer; skipping",
                col,
                layer_unit,
                layer_unit,
            )
            continue
        if pretrain_unit is None or pretrain_unit not in pretrain_unit_matrices:
            _log.warning(
                "set_pretrained_embeddings col=%s layer_unit=%s: no matching pretrain unit; skipping",
                col,
                layer_unit,
            )
            continue
        pretrain_matrix = pretrain_unit_matrices[pretrain_unit]
        pretrain_index = pretrain_unit_index[pretrain_unit]
        pretrain_dim = int(pretrain_matrix.shape[1])
        current_weights = embed_layer.get_weights()
        if not current_weights:
            raise RuntimeError(
                f"layer embed_{layer_unit} has no weights; build the model before loading"
            )
        target_matrix = np.asarray(current_weights[0], dtype=np.float64).copy()
        target_dim = int(target_matrix.shape[1])
        if pretrain_dim != target_dim:
            if os.environ.get("BC_PRETRAIN_SKIP_DIM_MISMATCH", "0") in (
                "1", "true", "TRUE"
            ):
                _log.warning(
                    "skip pretrain warm-start col=%s layer_unit=%s pretrain_unit=%s: dim mismatch pretrain=%d target=%d",
                    col, layer_unit, pretrain_unit, pretrain_dim, target_dim,
                )
                stats[col] = {
                    "aligned": 0,
                    "target_vocab_size": int(target_matrix.shape[0]),
                    "pretrain_vocab_size": len(pretrain_index),
                    "skipped_dim_mismatch": True,
                }
                continue
            raise ValueError(
                f"embedding dim mismatch for {col!r} layer_unit={layer_unit!r} "
                f"pretrain_unit={pretrain_unit!r}: pretrain={pretrain_dim} target={target_dim}"
            )

        target_values = target_vocab_values.get(col, ())
        target_size = int(target_matrix.shape[0])

        if embed_layer.name in seen_target_layers:
            stats[col] = {
                "aligned": stats.get(col, {}).get("aligned", 0),
                "target_vocab_size": target_size,
                "pretrain_vocab_size": len(pretrain_index),
            }
            continue
        seen_target_layers.add(embed_layer.name)

        if target_size != len(target_values) + 1:
            raise ValueError(
                f"target embedding matrix rows for {col!r} layer_unit={layer_unit!r}: "
                f"got {target_size} but vocabulary size+OOV is {len(target_values) + 1}"
            )

        aligned = 0
        oov_idx = pretrain_index.get("<oov>")
        if oov_idx is not None:
            target_matrix[0] = pretrain_matrix[oov_idx]
            aligned += 1
        for ti, value in enumerate(target_values, start=1):
            pi = pretrain_index.get(value)
            if pi is None:
                continue
            target_matrix[ti] = pretrain_matrix[pi]
            aligned += 1

        embed_layer.set_weights([target_matrix.astype(current_weights[0].dtype)])
        _log.info(
            "loaded pretrained embeddings col=%s layer_unit=%s pretrain_unit=%s "
            "aligned=%d target_vocab=%d pretrain_vocab=%d",
            col,
            layer_unit,
            pretrain_unit,
            aligned,
            target_size,
            len(pretrain_index),
        )
        stats[col] = {
            "aligned": aligned,
            "target_vocab_size": target_size,
            "pretrain_vocab_size": len(pretrain_index),
        }
    return stats
