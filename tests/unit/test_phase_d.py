"""Phase D5: unit tests for D1 rank reconciliation, D2 rank objective, D3 two-stage DNF.

These tests cover the three Phase D surfaces without spinning up Airflow or
MariaDB: pure numpy / dataclass assertions for the math, and ``MagicMock``
shims for the predict-task orchestration. The intent is to lock in the
properties the plan promises so a future regression (e.g. someone replacing
``argsort`` with ``rankdata``) fails fast.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

from include.nascar.ml.dnf import (
    DNF_TARGET_KEY,
    MODEL_KIND_DNF_CLASSIFIER,
    MODEL_KIND_REGRESSION,
    MODEL_KIND_REGRESSION_CLEAN,
    compose_two_stage_primary,
    compose_two_stage_secondary,
    derive_is_dnf,
)
from include.nascar.ml import predict_task
from include.nascar.ml.predict_task import (
    PRIMARY_TARGET_RAW_KEY,
    reconcile_primary_to_integer_ranks,
)
from include.nascar.ml.xgb_train_pipeline import (
    PRIMARY_TARGET,
    _rank_groups_and_relevance,
    train_models_for_targets,
)
from include.nascar.ml.xgb_tuning import (
    CLASSIFIER_DEFAULT_PARAMS,
    DEFAULT_PARAMS,
    RANKER_DEFAULT_PARAMS,
    TASK_CLASSIFIER,
    TASK_RANKER,
    TASK_REGRESSION,
    _task_config,
    tune_xgb,
)


# ---------------------------------------------------------------------------
# D1: post-hoc rank reconciliation
# ---------------------------------------------------------------------------


def test_d1_reconcile_returns_permutation_of_1_to_n() -> None:
    """D1: integer ranks must form a permutation of ``1..n``."""
    blended = np.array([12.3, 5.1, 18.7, 9.0, 22.4], dtype=float)
    raw, ranks = reconcile_primary_to_integer_ranks(blended)

    assert raw.dtype == float
    assert ranks.dtype == int
    # Permutation invariant: sorted ranks == 1..n.
    np.testing.assert_array_equal(np.sort(ranks), np.arange(1, blended.size + 1))


def test_d1_reconcile_lowest_float_gets_rank_one() -> None:
    """Default (ascending) direction: lowest blended float = best finish = rank 1."""
    blended = np.array([12.3, 5.1, 18.7, 9.0, 22.4], dtype=float)
    _, ranks = reconcile_primary_to_integer_ranks(blended)

    # Driver at index 1 had the lowest predicted finish -> rank 1.
    assert ranks[1] == 1
    # Driver at index 4 had the highest -> last rank.
    assert ranks[4] == blended.size


def test_d1_reconcile_descending_flips_direction() -> None:
    """D2 path: with ``descending=True``, highest score wins rank 1."""
    scores = np.array([0.1, 0.9, 0.5, 0.3], dtype=float)
    _, ranks = reconcile_primary_to_integer_ranks(scores, descending=True)

    assert ranks[1] == 1  # highest score
    assert ranks[0] == ranks.size  # lowest score


def test_d1_reconcile_handles_ties_stably() -> None:
    """Stable argsort gives deterministic tie-breaking by row order."""
    blended = np.array([10.0, 10.0, 5.0, 10.0], dtype=float)
    _, ranks = reconcile_primary_to_integer_ranks(blended)

    # Driver at index 2 (only 5.0) gets rank 1.
    assert ranks[2] == 1
    # Tied 10.0 rows get ranks 2, 3, 4 in their original positional order.
    assert ranks[0] == 2
    assert ranks[1] == 3
    assert ranks[3] == 4


def test_d1_reconcile_permutation_invariant_under_shuffle() -> None:
    """Permuting input rows must produce a correspondingly permuted rank vector."""
    rng = np.random.default_rng(42)
    blended = rng.normal(size=25)
    perm = rng.permutation(blended.size)

    _, ranks_original = reconcile_primary_to_integer_ranks(blended)
    _, ranks_shuffled = reconcile_primary_to_integer_ranks(blended[perm])

    np.testing.assert_array_equal(ranks_original[perm], ranks_shuffled)


def test_d1_reconcile_empty_input() -> None:
    """Empty input must not crash; returns empty arrays."""
    raw, ranks = reconcile_primary_to_integer_ranks(np.array([], dtype=float))
    assert raw.size == 0
    assert ranks.size == 0


@patch("xgboost.XGBClassifier")
@patch("xgboost.XGBRegressor")
@patch("include.nascar.ml.predict_task._predict_values_for_target")
@patch("include.nascar.ml.predict_task.track_type_ids_for_track")
@patch("include.nascar.ml.predict_task._predict_rows")
@patch("include.nascar.ml.predict_task._latest_predict_snapshot")
@patch("include.nascar.ml.predict_task._active_rules")
@patch("include.nascar.ml.predict_task.NascarMySqlHook")
def test_d1_predict_persists_raw_and_integer_rank(
    mock_hook_cls: MagicMock,
    mock_rules: MagicMock,
    mock_snapshot: MagicMock,
    mock_rows: MagicMock,
    mock_type_ids: MagicMock,
    mock_predict_values: MagicMock,
    mock_xgb: MagicMock,
    mock_xgbc: MagicMock,
) -> None:
    """End-to-end D1: ``predicted_targets_json`` carries both raw float + int rank."""
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
        {"car_driver_id": 103, "r_start_position": 3, "features": {}},
    ]
    mock_rows.return_value = rows
    mock_type_ids.return_value = [3, 5]

    blend_meta = {
        "series_id": 1,
        "model_scope": "track_type",
        "track_id": 7,
        "track_type_ids": [3, 5],
        "model_ids": [9, 11],
        "model_kinds": [MODEL_KIND_REGRESSION, MODEL_KIND_REGRESSION],
        "weights": [0.5, 0.5],
        "blend_strategy_used": "equal",
        "blend_strategy_requested": "equal",
        "maes": [None, None],
        "cohort_n_weeks": [4, 6],
        "skipped_track_type_ids": [],
    }

    # First call (DNF target) returns ``None`` so the composer no-ops; the
    # primary head returns blended floats that we expect to be argsort'd
    # into ranks 1..3.
    raw_primary = np.array([12.0, 5.0, 18.0], dtype=float)

    def _fake_predict_values(
        _hook: object,
        *,
        target: str,
        **_kwargs: Any,
    ) -> tuple[np.ndarray | None, dict[str, Any] | None, int | None]:
        if target == DNF_TARGET_KEY:
            return None, None, None
        if target == PRIMARY_TARGET:
            return raw_primary, blend_meta, 9
        # Secondary targets fall back to the cohort training mean path.
        return None, None, None

    mock_predict_values.side_effect = _fake_predict_values

    with patch.object(predict_task, "DEFAULT_TARGETS", (PRIMARY_TARGET,)):
        result = predict_task.predict_upcoming_week_task()

    assert result["skipped"] is None

    # Pull the executemany call payload off the transaction callback.
    hook.run_in_transaction.assert_called_once()
    callback = hook.run_in_transaction.call_args.args[0]
    cursor = MagicMock()
    callback(MagicMock(), cursor)
    insert_params = cursor.executemany.call_args.args[1]
    # One row per driver.
    assert len(insert_params) == len(rows)

    import json as _json

    # Driver order matches the predict_rows order, so index aligns with
    # ``raw_primary``. Lowest raw = 5.0 at index 1 -> integer rank 1.
    decoded_targets = [_json.loads(params[3]) for params in insert_params]
    integer_ranks = [int(t[PRIMARY_TARGET]) for t in decoded_targets]
    raw_values = [t[PRIMARY_TARGET_RAW_KEY] for t in decoded_targets]

    # D1: persisted raw == input blended floats (unchanged).
    np.testing.assert_array_equal(raw_values, raw_primary)
    # Integer ranks must be a permutation of 1..N.
    assert sorted(integer_ranks) == list(range(1, len(rows) + 1))
    # Lowest raw (5.0 at idx 1) gets rank 1.
    assert integer_ranks[1] == 1


# ---------------------------------------------------------------------------
# D2: ``rank:pairwise`` plumbing
# ---------------------------------------------------------------------------


def test_d2_ranker_default_params_use_pairwise_ndcg() -> None:
    """``RANKER_DEFAULT_PARAMS`` must opt into rank:pairwise + NDCG eval."""
    assert RANKER_DEFAULT_PARAMS["objective"] == "rank:pairwise"
    assert RANKER_DEFAULT_PARAMS["eval_metric"] == "ndcg"
    # The hyperparams shape must mirror the regression defaults so the
    # search distribution still applies if someone enables tuning later.
    assert set(RANKER_DEFAULT_PARAMS) >= set(DEFAULT_PARAMS) - {"objective"}


def test_d2_task_config_routes_to_xgbranker() -> None:
    """``_task_config('ranker')`` returns ``XGBRanker`` and pairwise defaults."""
    from xgboost import XGBRanker

    estimator_cls, defaults, cv_scoring = _task_config(TASK_RANKER)

    assert estimator_cls is XGBRanker
    assert defaults["objective"] == "rank:pairwise"
    # CV scoring is empty for ranker — sklearn's standard scorers don't
    # accept the qid argument the ranker needs.
    assert cv_scoring == ""


def test_d2_task_config_rejects_unknown_task() -> None:
    with pytest.raises(ValueError, match="unsupported task"):
        _task_config("quantile")


def test_d2_tune_xgb_ranker_requires_group_argument() -> None:
    """Ranker fit without a ``group`` array must fail loudly, not silently regress."""
    rng = np.random.default_rng(0)
    X = rng.normal(size=(10, 3))
    y = np.arange(10, dtype=float)

    with pytest.raises(ValueError, match="group"):
        tune_xgb(X, y, task=TASK_RANKER, n_iter=1, cv_splits=2)


def test_d2_tune_xgb_ranker_fits_with_groups(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D2: ranker path bypasses RandomizedSearchCV; metadata reports the task."""
    # The ranker code path always bypasses RandomizedSearchCV regardless of
    # the disable env, but setting it makes the test fast and deterministic.
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    rng = np.random.default_rng(0)
    # 3 weeks, 4 drivers each -> 12 rows, group=[4, 4, 4].
    X = rng.normal(size=(12, 3))
    y = np.tile(np.arange(4, 0, -1, dtype=float), 3)  # relevance: higher = better
    group = np.array([4, 4, 4], dtype=np.int64)

    estimator, params, meta = tune_xgb(X, y, task=TASK_RANKER, group=group)

    from xgboost import XGBRanker

    assert isinstance(estimator, XGBRanker)
    assert params["objective"] == "rank:pairwise"
    assert meta["task"] == TASK_RANKER
    assert meta["n_groups"] == 3
    assert meta["tuning_skipped"] is True


def test_d2_rank_groups_and_relevance_groups_by_week() -> None:
    """``_rank_groups_and_relevance`` must build per-week qid groups."""
    frame = [
        {"week_id": 200, "targets": {PRIMARY_TARGET: 3.0}},
        {"week_id": 201, "targets": {PRIMARY_TARGET: 1.0}},
        {"week_id": 200, "targets": {PRIMARY_TARGET: 1.0}},
        {"week_id": 201, "targets": {PRIMARY_TARGET: 2.0}},
        {"week_id": 200, "targets": {PRIMARY_TARGET: 2.0}},
    ]
    row_mask = np.ones(len(frame), dtype=bool)
    finish_y = np.array([r["targets"][PRIMARY_TARGET] for r in frame], dtype=float)

    ordered, groups, relevance = _rank_groups_and_relevance(
        frame, row_mask, finish_y
    )

    # Two weeks, each with [3, 2] rows respectively.
    assert groups.tolist() == [3, 2]
    assert ordered.size == len(frame)
    # The ordered indices must be contiguous-by-week.
    week_ids_ordered = [int(frame[i]["week_id"]) for i in ordered]
    assert week_ids_ordered == [200, 200, 200, 201, 201]
    # Relevance = field_size - finish: week 200 has 3 rows so finishes
    # (3, 1, 2) -> relevance (0, 2, 1). The winner (finish=1) gets the
    # highest relevance.
    np.testing.assert_array_equal(
        relevance[:3], np.array([0.0, 2.0, 1.0])
    )
    # Week 201 has 2 rows; finishes (1, 2) -> relevance (1, 0).
    np.testing.assert_array_equal(relevance[3:], np.array([1.0, 0.0]))


def test_d2_rank_groups_and_relevance_respects_row_mask() -> None:
    """A False entry in ``row_mask`` drops that row from the groups."""
    frame = [
        {"week_id": 200, "targets": {PRIMARY_TARGET: 1.0}},
        {"week_id": 200, "targets": {PRIMARY_TARGET: 2.0}},
        {"week_id": 201, "targets": {PRIMARY_TARGET: 1.0}},
    ]
    row_mask = np.array([True, False, True])
    finish_y = np.array([1.0, 2.0, 1.0])

    ordered, groups, _relevance = _rank_groups_and_relevance(
        frame, row_mask, finish_y
    )

    assert ordered.tolist() == [0, 2]
    # Each remaining week has exactly one row -> group sizes [1, 1].
    assert groups.tolist() == [1, 1]


def test_d2_train_models_uses_ranker_when_env_enabled(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end D2: env flag flips the primary head to a pairwise ranker."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    monkeypatch.setenv("FS_ML_PRIMARY_RANK_OBJECTIVE", "1")

    from xgboost import XGBRanker, XGBRegressor

    frame = _build_synthetic_frame(n_weeks=3, n_drivers=8, seed=11)
    trained = train_models_for_targets(
        frame,
        targets=(PRIMARY_TARGET, "r_total_laps_led"),
        holdout_weeks=1,
        dev_weeks=0,
        artifact_dir=tmp_path,
        boruta_max_iter=5,
    )

    by_target = {entry.target: entry for entry in trained}
    # Primary head is a ranker; secondary head is still a regressor.
    assert isinstance(by_target[PRIMARY_TARGET].model, XGBRanker)
    assert isinstance(by_target["r_total_laps_led"].model, XGBRegressor)
    # Eval continuity: published holdout MAE on the primary head is still
    # populated by argsort'ing the scores into integer finishes.
    assert by_target[PRIMARY_TARGET].holdout_mae is not None
    assert by_target[PRIMARY_TARGET].holdout_mae >= 0.0


def test_d2_train_models_disables_rank_when_two_stage_present(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """D2 ranker is mutually exclusive with D3 two-stage DNF — flag is ignored."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    monkeypatch.setenv("FS_ML_PRIMARY_RANK_OBJECTIVE", "1")

    from xgboost import XGBRegressor

    # Frame carries ``r_is_dnf`` labels so the trainer goes two-stage and
    # must FALL BACK to reg:absoluteerror for the primary head.
    frame = _build_synthetic_frame(n_weeks=3, n_drivers=8, seed=12, with_dnf=True)
    trained = train_models_for_targets(
        frame,
        targets=(PRIMARY_TARGET, DNF_TARGET_KEY),
        holdout_weeks=1,
        dev_weeks=0,
        artifact_dir=tmp_path,
        boruta_max_iter=5,
    )

    by_target = {entry.target: entry for entry in trained}
    # Primary head must be the regressor, NOT a ranker.
    assert isinstance(by_target[PRIMARY_TARGET].model, XGBRegressor)
    assert by_target[PRIMARY_TARGET].model_kind == MODEL_KIND_REGRESSION_CLEAN


# ---------------------------------------------------------------------------
# D3: two-stage DNF (compose math + trainer wiring)
# ---------------------------------------------------------------------------


def test_d3_compose_primary_with_certain_dnf() -> None:
    """``P_dnf=1`` collapses every driver to the back-of-field finish."""
    clean = np.array([5.0, 10.0, 15.0], dtype=float)
    composed = compose_two_stage_primary(clean, np.ones(3), field_size=40)
    np.testing.assert_array_equal(composed, np.full(3, 40.0))


def test_d3_compose_primary_with_zero_dnf_passthrough() -> None:
    """``P_dnf=0`` returns ``E[finish | clean]`` unchanged."""
    clean = np.array([5.0, 10.0, 15.0], dtype=float)
    composed = compose_two_stage_primary(clean, np.zeros(3), field_size=40)
    np.testing.assert_array_equal(composed, clean)


def test_d3_compose_primary_no_dnf_branch_is_legacy_passthrough() -> None:
    """Legacy single-head case (``dnf_probability=None``) returns clean unchanged."""
    clean = np.array([5.0, 10.0, 15.0], dtype=float)
    composed = compose_two_stage_primary(clean, None, field_size=40)
    np.testing.assert_array_equal(composed, clean)


def test_d3_compose_primary_convex_combination() -> None:
    """Compose formula matches ``P_dnf * field_size + (1 - P_dnf) * clean``."""
    clean = np.array([5.0, 10.0, 20.0], dtype=float)
    dnf = np.array([0.1, 0.5, 0.9], dtype=float)
    field_size = 40
    composed = compose_two_stage_primary(clean, dnf, field_size=field_size)
    expected = dnf * field_size + (1.0 - dnf) * clean
    np.testing.assert_allclose(composed, expected)
    # Each composed value lies between clean and field_size (convex combo).
    assert np.all(composed >= np.minimum(clean, field_size))
    assert np.all(composed <= np.maximum(clean, field_size))


def test_d3_compose_secondary_certain_dnf_zeros_out() -> None:
    """Secondary targets are zero on a certain DNF (counts collapse to 0)."""
    clean = np.array([2.0, 5.0, 8.0], dtype=float)
    composed = compose_two_stage_secondary(clean, np.ones(3))
    np.testing.assert_array_equal(composed, np.zeros(3))


def test_d3_compose_secondary_no_dnf_branch_is_legacy_passthrough() -> None:
    clean = np.array([2.0, 5.0, 8.0], dtype=float)
    composed = compose_two_stage_secondary(clean, None)
    np.testing.assert_array_equal(composed, clean)


def test_d3_compose_secondary_linear_in_dnf() -> None:
    """``E[y] = (1 - P_dnf) * E[y | clean]`` — straight linear damping."""
    clean = np.array([2.0, 5.0, 8.0], dtype=float)
    dnf = np.array([0.25, 0.5, 0.75], dtype=float)
    composed = compose_two_stage_secondary(clean, dnf)
    np.testing.assert_allclose(composed, (1.0 - dnf) * clean)


def test_d3_derive_is_dnf_running_status() -> None:
    assert derive_is_dnf({"r_car_status": "RUNNING"}) == 0
    assert derive_is_dnf({"r_car_status": "running"}) == 0
    # Anything else is a DNF.
    assert derive_is_dnf({"r_car_status": "ENGINE"}) == 1
    assert derive_is_dnf({"r_car_status": "CRASH"}) == 1


def test_d3_derive_is_dnf_null_status_is_not_dnf() -> None:
    """``NULL`` r_car_status conservatively reads as clean to avoid inflating the rate."""
    assert derive_is_dnf({"r_car_status": None}) == 0
    assert derive_is_dnf({}) == 0


def test_d3_classifier_default_params_use_logistic() -> None:
    assert CLASSIFIER_DEFAULT_PARAMS["objective"] == "binary:logistic"
    assert CLASSIFIER_DEFAULT_PARAMS["eval_metric"] == "logloss"


def test_d3_task_config_routes_to_correct_estimator() -> None:
    from xgboost import XGBClassifier, XGBRegressor

    reg_cls, _reg_defaults, reg_scoring = _task_config(TASK_REGRESSION)
    clf_cls, clf_defaults, clf_scoring = _task_config(TASK_CLASSIFIER)

    assert reg_cls is XGBRegressor
    assert reg_scoring == "neg_mean_absolute_error"
    assert clf_cls is XGBClassifier
    assert clf_defaults["objective"] == "binary:logistic"
    assert clf_scoring == "neg_log_loss"


def test_d3_train_two_stage_emits_classifier_and_clean_regressor(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """End-to-end D3 trainer: frame with ``r_is_dnf`` -> classifier + clean regressors."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    monkeypatch.delenv("FS_ML_PRIMARY_RANK_OBJECTIVE", raising=False)

    from xgboost import XGBClassifier, XGBRegressor

    frame = _build_synthetic_frame(
        n_weeks=4, n_drivers=10, seed=21, with_dnf=True
    )

    trained = train_models_for_targets(
        frame,
        targets=(PRIMARY_TARGET, "r_total_laps_led", DNF_TARGET_KEY),
        holdout_weeks=1,
        dev_weeks=0,
        artifact_dir=tmp_path,
        boruta_max_iter=5,
    )

    by_target = {entry.target: entry for entry in trained}
    assert set(by_target) == {PRIMARY_TARGET, "r_total_laps_led", DNF_TARGET_KEY}

    # DNF head: classifier, log_loss populated, MAE left None.
    dnf_entry = by_target[DNF_TARGET_KEY]
    assert isinstance(dnf_entry.model, XGBClassifier)
    assert dnf_entry.model_kind == MODEL_KIND_DNF_CLASSIFIER
    assert dnf_entry.holdout_log_loss is not None
    assert dnf_entry.holdout_mae is None

    # Regression heads: tagged ``regression_clean`` because the frame had DNF labels.
    primary_entry = by_target[PRIMARY_TARGET]
    assert isinstance(primary_entry.model, XGBRegressor)
    assert primary_entry.model_kind == MODEL_KIND_REGRESSION_CLEAN
    assert primary_entry.holdout_mae is not None


def test_d3_train_legacy_frame_collapses_to_single_head(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """When no row has ``r_is_dnf``, every head is plain ``regression``."""
    monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")
    monkeypatch.delenv("FS_ML_PRIMARY_RANK_OBJECTIVE", raising=False)

    frame = _build_synthetic_frame(
        n_weeks=4, n_drivers=10, seed=22, with_dnf=False
    )

    trained = train_models_for_targets(
        frame,
        targets=(PRIMARY_TARGET, "r_total_laps_led", DNF_TARGET_KEY),
        holdout_weeks=1,
        dev_weeks=0,
        artifact_dir=tmp_path,
        boruta_max_iter=5,
    )

    by_target = {entry.target: entry for entry in trained}
    # DNF target is skipped because the frame doesn't carry the label.
    assert DNF_TARGET_KEY not in by_target
    # Regression heads use the legacy ``regression`` kind.
    for target in (PRIMARY_TARGET, "r_total_laps_led"):
        assert by_target[target].model_kind == MODEL_KIND_REGRESSION


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_synthetic_frame(
    *,
    n_weeks: int = 4,
    n_drivers: int = 10,
    seed: int = 0,
    with_dnf: bool = False,
) -> list[dict]:
    """Tiny synthetic training frame for D2/D3 trainer tests.

    Each row has minimal features + the targets the trainer needs. When
    ``with_dnf=True`` a couple of drivers per week are flagged as DNFs so
    the trainer's two-stage path activates.
    """
    rng = np.random.default_rng(seed)
    frame: list[dict] = []
    for week_index in range(1, n_weeks + 1):
        week_id = 200 + week_index
        # Random DNF index set for this week (only when with_dnf).
        dnf_indices = (
            set(rng.choice(n_drivers, size=2, replace=False).tolist())
            if with_dnf
            else set()
        )
        for driver_index in range(n_drivers):
            skill = float(rng.normal())
            speed = float(rng.normal())
            features = {
                "skill_feat": skill,
                "speed_feat": speed,
                "noise_feat": float(rng.normal()),
            }
            finish = max(
                1, min(n_drivers, int(round((n_drivers + 1) / 2 - 3 * skill)))
            )
            laps_led = max(0, int(round(5 + 3 * speed)))
            targets: dict[str, float] = {
                PRIMARY_TARGET: float(finish),
                "r_total_laps_led": float(laps_led),
                "r_total_fastest_laps": float(max(0, int(round(4 + 2 * speed)))),
            }
            if with_dnf:
                targets[DNF_TARGET_KEY] = 1.0 if driver_index in dnf_indices else 0.0
            frame.append(
                {
                    "ml_training_snapshot_id": week_index,
                    "week_id": week_id,
                    "car_driver_id": 1000 + driver_index,
                    "driver_id": 2000 + driver_index,
                    "r_start_position": max(
                        1, min(n_drivers, int(round((n_drivers + 1) / 2 - skill)))
                    ),
                    "features": features,
                    "targets": targets,
                    "dfs_points": 0.0,
                    "dfs_points_breakdown": {},
                    "target_keys": list(targets.keys()),
                }
            )
    return frame
