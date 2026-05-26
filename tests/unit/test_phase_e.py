"""Phase E6: unit tests for E1-E5 (conformal coverage, rank-coupled MC, blend
lineage round-trip, drift warnings, spearman blend).

Each surface is exercised against a tiny synthetic frame / mock hook so the
test runs in a few hundred ms without hitting Airflow or MariaDB.
"""

from __future__ import annotations

import json
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from include.nascar.ml import monte_carlo_task, predict_task
from include.nascar.ml.dnf import DNF_TARGET_KEY
from include.nascar.ml.model_scope import (
    DEFAULT_INTERVAL_METHOD,
    INTERVAL_METHOD_CONFORMAL,
    INTERVAL_METHOD_GLOBAL_MAE,
    VALID_BLEND_STRATEGIES,
    VALID_INTERVAL_METHODS,
    interval_method,
)
from include.nascar.ml.monte_carlo import (
    MIN_SIMULATIONS,
    MonteCarloDriverInput,
    _reconcile_finish_ranks,
    simulate_driver_distributions,
)
from include.nascar.ml.predict_task import (
    PRIMARY_TARGET_RAW_KEY,
    _blend_weights,
    _drift_warnings_for_cohort,
    _weight_average_intervals,
)
from include.nascar.ml.xgb_train_pipeline import (
    PRIMARY_TARGET,
    _compute_residual_quantiles,
    train_models_for_targets,
)


# ---------------------------------------------------------------------------
# E1: conformal residual quantiles
# ---------------------------------------------------------------------------


def test_e1_residual_quantiles_are_monotonic() -> None:
    """The five quantile labels must be ordered q05 <= q10 <= q50 <= q90 <= q95."""
    rng = np.random.default_rng(0)
    y_true = rng.normal(loc=10.0, scale=2.0, size=200)
    y_pred = y_true + rng.normal(scale=1.5, size=200)
    quantiles = _compute_residual_quantiles(y_true, y_pred)

    assert quantiles is not None
    values = [quantiles[k] for k in ("q05", "q10", "q50", "q90", "q95")]
    assert values == sorted(values)
    assert all(v >= 0.0 for v in values)


def test_e1_residual_quantiles_returns_none_when_insufficient_data() -> None:
    """Single observation can't form a non-degenerate interval."""
    assert _compute_residual_quantiles(np.array([1.0]), np.array([2.0])) is None
    assert _compute_residual_quantiles(np.array([]), np.array([])) is None


def test_e1_q90_covers_approximately_ninety_percent() -> None:
    """Conformal coverage check: q90 is the 90th percentile of |residuals|."""
    rng = np.random.default_rng(7)
    n = 1000
    y_true = rng.normal(loc=0.0, scale=5.0, size=n)
    y_pred = y_true + rng.normal(scale=1.0, size=n)
    quantiles = _compute_residual_quantiles(y_true, y_pred)

    assert quantiles is not None
    residuals = np.abs(y_true - y_pred)
    covered = int(np.sum(residuals <= quantiles["q90"]))
    # Allow ±2% sampling slack at n=1000.
    assert 0.88 * n <= covered <= 0.92 * n


def test_e1_train_pipeline_persists_residual_quantiles(
    tmp_path: Any,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end E1: train returns ``holdout_residual_quantiles`` per target."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    rng = np.random.default_rng(11)
    frame: list[dict[str, Any]] = []
    for week_index in range(1, 5):
        for driver_index in range(10):
            skill = float(rng.normal())
            features = {"skill": skill, "speed": float(rng.normal())}
            frame.append(
                {
                    "ml_training_snapshot_id": week_index,
                    "week_id": 200 + week_index,
                    "car_driver_id": 1000 + driver_index,
                    "driver_id": 2000 + driver_index,
                    "r_start_position": 1.0,
                    "features": features,
                    "targets": {
                        PRIMARY_TARGET: float(
                            max(1, min(10, int(round(6 - 2 * skill))))
                        ),
                        "r_total_laps_led": float(
                            max(0, int(round(4 + 2 * features["speed"])))
                        ),
                    },
                    "dfs_points": 0.0,
                    "dfs_points_breakdown": {},
                    "target_keys": [PRIMARY_TARGET, "r_total_laps_led"],
                }
            )

    trained = train_models_for_targets(
        frame,
        targets=(PRIMARY_TARGET, "r_total_laps_led"),
        holdout_weeks=1,
        dev_weeks=0,
        artifact_dir=tmp_path,
        boruta_max_iter=4,
    )

    for entry in trained:
        assert entry.holdout_residual_quantiles is not None, (
            f"{entry.target}: missing holdout_residual_quantiles"
        )
        for key in ("q05", "q10", "q50", "q90", "q95"):
            assert key in entry.holdout_residual_quantiles


# ---------------------------------------------------------------------------
# E1 / FS_ML_INTERVAL_METHOD env knob
# ---------------------------------------------------------------------------


def test_e1_interval_method_default_is_conformal(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("FS_ML_INTERVAL_METHOD", raising=False)
    assert interval_method() == DEFAULT_INTERVAL_METHOD == INTERVAL_METHOD_CONFORMAL


def test_e1_interval_method_unknown_falls_back_to_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_INTERVAL_METHOD", "garbage_value")
    assert interval_method() == DEFAULT_INTERVAL_METHOD


def test_e1_interval_method_accepts_all_valid_values(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for value in VALID_INTERVAL_METHODS:
        monkeypatch.setenv("FS_ML_INTERVAL_METHOD", value)
        assert interval_method() == value


# ---------------------------------------------------------------------------
# E2: rank-coupled Monte Carlo
# ---------------------------------------------------------------------------


def test_e2_reconcile_finish_ranks_yields_permutation() -> None:
    """Argsort on raw finish samples produces a permutation of 1..N."""
    raw = np.array([5.5, 1.2, 3.3, 4.4, 2.1], dtype=float)
    ranks = _reconcile_finish_ranks(raw)
    assert sorted(ranks.tolist()) == [1, 2, 3, 4, 5]
    # Lowest raw -> rank 1 (best finish).
    assert ranks[1] == 1
    # Highest raw -> rank N (worst finish).
    assert ranks[0] == 5


def _build_driver_pool(n_drivers: int) -> list[MonteCarloDriverInput]:
    return [
        MonteCarloDriverInput(
            car_driver_id=i + 1,
            r_start_position=float(i + 1),
            predicted_targets={
                "r_finish_position": float(i + 1),
                "r_total_laps_led": 30.0,
            },
            target_sigmas={
                "r_finish_position": 4.0,
                "r_total_laps_led": 8.0,
            },
        )
        for i in range(n_drivers)
    ]


_SIMPLE_RULES = {
    "components": [
        {
            "name": "place",
            "type": "finish_table",
            "source": "r_finish_position",
            "formula": {"subtract_position": True, "base": 41, "min": 0},
        },
        {
            "name": "laps_led",
            "type": "linear",
            "source": "r_total_laps_led",
            "multiplier": 0.25,
        },
    ]
}


def test_e2_every_simulation_iteration_is_a_permutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Capture every ``r_finish_position`` seen by the scorer and verify each
    iteration's vector is a permutation of ``1..n_drivers``."""
    drivers = _build_driver_pool(8)
    n_drivers = len(drivers)
    n_sims = MIN_SIMULATIONS
    captured: list[float] = []

    real_compute = (
        __import__(
            "include.nascar.ml.monte_carlo",
            fromlist=["compute_dfs_points_from_targets"],
        ).compute_dfs_points_from_targets
    )

    def _capture(start: object, targets: dict, rules: object):
        captured.append(targets["r_finish_position"])
        return real_compute(start, targets, rules)

    with patch(
        "include.nascar.ml.monte_carlo.compute_dfs_points_from_targets",
        side_effect=_capture,
    ):
        simulate_driver_distributions(
            drivers,
            scoring_rules=_SIMPLE_RULES,
            n_simulations=n_sims,
            seed=11,
        )

    assert len(captured) == n_drivers * n_sims
    # Reshape so each row is one iteration's per-driver finish slot.
    matrix = np.array(captured, dtype=float).reshape(n_sims, n_drivers)
    expected = np.arange(1, n_drivers + 1, dtype=float)
    for s in range(n_sims):
        assert sorted(matrix[s].tolist()) == expected.tolist()


# ---------------------------------------------------------------------------
# E3: blend metadata persisted on ml_predictions
# ---------------------------------------------------------------------------


@patch("xgboost.XGBClassifier")
@patch("xgboost.XGBRegressor")
@patch("include.nascar.ml.predict_task._predict_values_for_target")
@patch("include.nascar.ml.predict_task.track_type_ids_for_track")
@patch("include.nascar.ml.predict_task._predict_rows")
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_e3_blend_metadata_round_trips_via_predict(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_rows: MagicMock,
    mock_type_ids: MagicMock,
    mock_predict_values: MagicMock,
    _mock_xgb: MagicMock,
    _mock_xgbc: MagicMock,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("FS_ML_BLEND_WEIGHTS", "equal")
    """``ml_predictions.blend_metadata_json`` carries the lineage round-trip."""
    hook = MagicMock()
    mock_hook_cls.return_value = hook
    mock_rules.return_value = (1, {"components": []})

    mock_snapshot.return_value = {
        "id": 50,
        "series_id": 1,
        "week_id": 100,
        "track_id": 7,
    }
    rows = [
        {"car_driver_id": 101, "r_start_position": 5, "features": {}},
        {"car_driver_id": 102, "r_start_position": 8, "features": {}},
    ]
    mock_rows.return_value = rows
    mock_type_ids.return_value = [3, 5]

    # Cohort entries carry holdout_residual_quantiles so the predict task
    # can populate ``conformal_intervals`` in the blend metadata blob.
    cohort_entries = [
        {
            "id": 22,
            "track_type_id": 3,
            "selected_features": ["skill"],
            "boruta_metadata": {},
            "holdout_residual_quantiles": {
                "q05": 0.2,
                "q10": 0.4,
                "q50": 1.0,
                "q90": 2.5,
                "q95": 3.0,
            },
        },
        {
            "id": 25,
            "track_type_id": 5,
            "selected_features": ["skill"],
            "boruta_metadata": {},
            "holdout_residual_quantiles": {
                "q05": 0.3,
                "q10": 0.5,
                "q50": 1.5,
                "q90": 3.0,
                "q95": 4.0,
            },
        },
    ]
    blend_meta = {
        "series_id": 1,
        "model_scope": "track_type",
        "track_id": 7,
        "track_type_ids": [3, 5],
        "model_ids": [22, 25],
        "model_kinds": ["regression", "regression"],
        "weights": [0.5, 0.5],
        "spearmans": [None, None],
        "blend_strategy_used": "equal",
        "blend_strategy_requested": "equal",
        "maes": [1.0, 1.5],
        "cohort_n_weeks": [4, 6],
        "skipped_track_type_ids": [],
        "_cohort_entries": cohort_entries,
    }

    def _fake_predict_values(
        _hook: object,
        *,
        target: str,
        **_kwargs: Any,
    ) -> tuple[np.ndarray | None, dict[str, Any] | None, int | None]:
        if target == DNF_TARGET_KEY:
            return None, None, None
        if target == PRIMARY_TARGET:
            return np.array([10.0, 20.0]), blend_meta, 22
        return None, None, None

    mock_predict_values.side_effect = _fake_predict_values

    with patch.object(predict_task, "DEFAULT_TARGETS", (PRIMARY_TARGET,)):
        result = predict_task.predict_upcoming_week_task()

    assert result["skipped"] is None

    # Pull insert_params off the captured transaction callback.
    hook.run_in_transaction.assert_called_once()
    callback = hook.run_in_transaction.call_args.args[0]
    cursor = MagicMock()
    callback(MagicMock(), cursor)
    insert_params = cursor.executemany.call_args.args[1]
    assert len(insert_params) == len(rows)
    # Row tuple now ends with blend_metadata_json.
    for params in insert_params:
        assert len(params) == 7
        blend_blob = json.loads(params[6])
        assert blend_blob["primary_model_id"] == 22
        assert blend_blob["track_type_ids"] == [3, 5]
        assert blend_blob["blend_strategy_requested"] == "equal"
        # E1: conformal intervals weight-averaged onto the lineage.
        assert PRIMARY_TARGET in blend_blob["conformal_intervals"]
        intervals = blend_blob["conformal_intervals"][PRIMARY_TARGET]
        assert intervals["q90"] == pytest.approx((2.5 + 3.0) / 2.0)
        # E4: drift_warnings list is present (empty on healthy snapshot).
        assert "drift_warnings" in blend_blob
        # The resolved_scopes blob must NOT leak internal cohort entries.
        primary_scope = blend_blob["resolved_scopes"][PRIMARY_TARGET]
        assert "_cohort_entries" not in primary_scope


# ---------------------------------------------------------------------------
# E4: drift guard
# ---------------------------------------------------------------------------


def test_e4_drift_warnings_emitted_when_feature_mean_shifts() -> None:
    """A feature whose predict-time mean drifts >3σ surfaces in the warning list."""
    model_entry = {
        "id": 42,
        "track_type_id": 3,
        "selected_features": ["stable_feat", "drifted_feat"],
        "boruta_metadata": {
            "train_feature_stats": {
                "stable_feat": {"mean": 0.0, "std": 1.0, "p05": -1.6, "p95": 1.6},
                "drifted_feat": {"mean": 0.0, "std": 1.0, "p05": -1.6, "p95": 1.6},
            }
        },
    }
    # 10 predict-time rows: stable_feat stays near 0, drifted_feat sits at 5.
    rows = [
        {"car_driver_id": i, "features": {"stable_feat": 0.05, "drifted_feat": 5.0}}
        for i in range(10)
    ]

    warnings = _drift_warnings_for_cohort(rows, model_entry=model_entry)
    feature_names = {w["feature"] for w in warnings}

    assert "drifted_feat" in feature_names
    assert "stable_feat" not in feature_names
    drifted = next(w for w in warnings if w["feature"] == "drifted_feat")
    assert drifted["cohort_track_type_id"] == 3
    assert drifted["ml_model_id"] == 42
    assert abs(drifted["z"]) > 3.0


def test_e4_drift_skips_constant_train_columns() -> None:
    """A train column with std=0 must not produce divide-by-zero warnings."""
    model_entry = {
        "id": 1,
        "track_type_id": 3,
        "selected_features": ["constant_feat"],
        "boruta_metadata": {
            "train_feature_stats": {
                "constant_feat": {"mean": 1.0, "std": 0.0, "p05": 1.0, "p95": 1.0}
            }
        },
    }
    rows = [{"car_driver_id": i, "features": {"constant_feat": 50.0}} for i in range(5)]
    warnings = _drift_warnings_for_cohort(rows, model_entry=model_entry)
    assert warnings == []


def test_e4_drift_empty_when_train_feature_stats_missing() -> None:
    """Cohort without ``train_feature_stats`` (legacy) emits no warnings."""
    model_entry = {
        "id": 1,
        "selected_features": ["any_feat"],
        "boruta_metadata": {},
    }
    rows = [{"car_driver_id": i, "features": {"any_feat": 999.0}} for i in range(5)]
    assert _drift_warnings_for_cohort(rows, model_entry=model_entry) == []


# ---------------------------------------------------------------------------
# E1 + E3: weighted conformal interval combiner
# ---------------------------------------------------------------------------


def test_weight_average_intervals_handles_partial_cohorts() -> None:
    """Missing per-cohort entries skip that contributor for the affected quantile."""
    cohort_entries = [
        {"holdout_residual_quantiles": {"q10": 0.1, "q90": 1.0}},
        {"holdout_residual_quantiles": {"q10": 0.2}},  # missing q90
        {"holdout_residual_quantiles": None},  # missing entirely
    ]
    weights = [0.5, 0.3, 0.2]
    averaged = _weight_average_intervals(cohort_entries, weights)
    assert averaged is not None
    # q10: (0.1*0.5 + 0.2*0.3) / (0.5 + 0.3) = 0.11 / 0.8 = 0.1375
    assert averaged["q10"] == pytest.approx((0.1 * 0.5 + 0.2 * 0.3) / 0.8)
    # q90: only the first cohort contributes -> 1.0.
    assert averaged["q90"] == pytest.approx(1.0)


def test_weight_average_intervals_returns_none_for_empty() -> None:
    assert _weight_average_intervals([], []) is None
    assert _weight_average_intervals(
        [{"holdout_residual_quantiles": None}], [1.0]
    ) is None


# ---------------------------------------------------------------------------
# E5: Spearman blend strategy
# ---------------------------------------------------------------------------


def test_e5_spearman_is_in_valid_blend_strategies() -> None:
    assert "spearman" in VALID_BLEND_STRATEGIES


def _spearman_contributor(
    model_id: int,
    *,
    spearman: float | None,
    preds: np.ndarray | None = None,
) -> tuple[int, int, np.ndarray, float | None, int, float | None]:
    return (
        model_id,
        model_id,
        preds if preds is not None else np.array([0.0]),
        None,  # mae
        4,
        spearman,
    )


def test_e5_blend_weights_spearman_strategy_weights_by_correlation() -> None:
    contributors = [
        _spearman_contributor(1, spearman=0.4),
        _spearman_contributor(2, spearman=0.2),
    ]
    weights, used = _blend_weights(contributors, strategy="spearman")
    np.testing.assert_allclose(weights, [2 / 3, 1 / 3], atol=1e-9)
    assert used == "spearman"


def test_e5_blend_weights_spearman_falls_back_to_equal_on_missing() -> None:
    contributors = [
        _spearman_contributor(1, spearman=0.4),
        _spearman_contributor(2, spearman=None),  # missing
    ]
    weights, used = _blend_weights(contributors, strategy="spearman")
    np.testing.assert_allclose(weights, [0.5, 0.5])
    assert used == "equal_fallback"


def test_e5_blend_weights_spearman_falls_back_to_equal_on_negative() -> None:
    """All negative Spearman = total positive weight is zero -> equal fallback."""
    contributors = [
        _spearman_contributor(1, spearman=-0.1),
        _spearman_contributor(2, spearman=-0.2),
    ]
    weights, used = _blend_weights(contributors, strategy="spearman")
    np.testing.assert_allclose(weights, [0.5, 0.5])
    assert used == "equal_fallback"


def test_e5_blend_weights_spearman_negative_values_clipped_to_zero() -> None:
    contributors = [
        _spearman_contributor(1, spearman=0.5),
        _spearman_contributor(2, spearman=-0.1),  # negative -> 0 contribution
    ]
    weights, used = _blend_weights(contributors, strategy="spearman")
    # First contributor takes all weight; second gets zero.
    np.testing.assert_allclose(weights, [1.0, 0.0])
    assert used == "spearman"


# ---------------------------------------------------------------------------
# Monte Carlo task: per-driver σ resolution from blend metadata
# ---------------------------------------------------------------------------


def test_e2_per_driver_sigma_resolved_from_conformal_intervals() -> None:
    """``_resolve_per_driver_sigmas`` reads per-target intervals off the lineage."""
    row = {
        "blend_metadata": {
            "conformal_intervals": {
                "r_finish_position": {"q10": 0.0, "q90": 4.0},
                "r_total_laps_led": {"q10": 0.0, "q90": 6.0},
            }
        }
    }
    sigmas = monte_carlo_task._resolve_per_driver_sigmas(
        row,
        method=INTERVAL_METHOD_CONFORMAL,
        fallback={
            "r_finish_position": 99.0,
            "r_total_laps_led": 99.0,
        },
        targets={"r_finish_position": 5.0, "r_total_laps_led": 20.0},
    )
    # Half-width = (q90 - q10) / 2.
    assert sigmas["r_finish_position"] == pytest.approx(2.0)
    assert sigmas["r_total_laps_led"] == pytest.approx(3.0)


def test_e2_per_driver_sigma_falls_back_to_global_mae_when_missing() -> None:
    """No conformal entry -> use the ``fallback`` per-target sigma."""
    row = {"blend_metadata": {}}
    sigmas = monte_carlo_task._resolve_per_driver_sigmas(
        row,
        method=INTERVAL_METHOD_CONFORMAL,
        fallback={"r_finish_position": 1.5},
        targets={"r_finish_position": 5.0},
    )
    assert sigmas == {"r_finish_position": 1.5}


def test_e2_per_driver_sigma_global_mae_method_bypasses_intervals() -> None:
    """``INTERVAL_METHOD_GLOBAL_MAE`` ignores ``conformal_intervals`` entirely."""
    row = {
        "blend_metadata": {
            "conformal_intervals": {
                "r_finish_position": {"q10": 0.0, "q90": 100.0}  # huge half-width
            }
        }
    }
    sigmas = monte_carlo_task._resolve_per_driver_sigmas(
        row,
        method=INTERVAL_METHOD_GLOBAL_MAE,
        fallback={"r_finish_position": 1.5},
        targets={"r_finish_position": 5.0},
    )
    # GLOBAL_MAE path returns the legacy fallback, not the (huge) conformal σ.
    assert sigmas == {"r_finish_position": 1.5}
