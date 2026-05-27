"""Tests for the Phase 1-4 memory-tuning knobs (12 GB / 4-core worker profile)
plus the optional GPU acceleration toggle.

The OOM mitigation work introduced one code-level change (``float32`` feature
matrix) and three env-driven knobs (``FS_ML_XGB_N_JOBS``,
``FS_ML_BORUTA_MAX_ITER``, ``FS_ML_BORUTA_SMALL_COHORT_MAX_ITER``). This
module pins the contracts so a future refactor can't silently revert any of
them — e.g. an upstream ``XGBRegressor`` constructor change inadvertently
restoring ``n_jobs=-1`` would re-quadruple per-thread histogram buffers on a
4-core worker.

The GPU toggle (``FS_ML_XGB_DEVICE``) is tested in the same module because
it shares the same pressure-release surface (every XGBoost constructor in
the cohort training path) and a regression in either knob is debugged in
the same place. All tests are CPU-only — they verify the env-var
resolution and constructor-kwarg plumbing, not actual GPU execution.

See the "Memory tuning" and "GPU acceleration" sections of
[docs/nascar/cup.machine_learning.models.md](../../docs/nascar/cup.machine_learning.models.md)
for the full pressure-release ordering these knobs back.
"""

from __future__ import annotations

import sys
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pytest

# ``train_task`` imports ``airflow_utils.nascar_mysql_hook`` which in turn pulls
# the Airflow MySQL provider. Host pytest runs outside the Airflow container,
# so we stub the missing modules before the import below.
for _name in (
    "airflow",
    "airflow.providers",
    "airflow.providers.mysql",
    "airflow.providers.mysql.hooks",
    "airflow.providers.mysql.hooks.mysql",
):
    sys.modules.setdefault(_name, MagicMock())

from include.nascar.ml.feature_matrix import build_feature_matrix  # noqa: E402
from include.nascar.ml.train_task import (  # noqa: E402
    _BORUTA_MAX_ITER_DEFAULT,
    _BORUTA_SMALL_COHORT_MAX_ITER_DEFAULT,
    _resolve_boruta_max_iter,
)
from include.nascar.ml.xgb_tuning import xgb_device, xgb_n_jobs  # noqa: E402


class TestFeatureMatrixDtype:
    """Phase 2: ``build_feature_matrix`` returns ``float32`` end-to-end.

    Float32 halves the dense feature matrix's per-cell footprint and
    transitively halves Boruta's 2x shadow-matrix copy — both biggest
    contributors to the cohort training peak. XGBoost ``tree_method='hist'``
    accepts float32 natively, so the dtype change is invisible to callers.
    """

    def test_clean_frame_returns_float32(self) -> None:
        frame = [
            {"features": {"x": 1.0, "y": 2.0}},
            {"features": {"x": 3.0, "y": 4.0}},
        ]
        matrix = build_feature_matrix(frame, ["x", "y"])
        assert matrix.dtype == np.float32
        assert matrix.shape == (2, 2)
        # Values still round-trip — float32 has ~7 decimal digits of
        # precision which is plenty for our feature ranges (counts, means,
        # rates).
        np.testing.assert_array_equal(matrix, np.array([[1.0, 2.0], [3.0, 4.0]], dtype=np.float32))

    def test_zero_fill_branch_preserves_float32(self) -> None:
        """Non-finite zero-fill path goes through ``np.nan_to_num`` — must keep float32."""
        frame = [
            {"features": {"x": 1.0, "y": 2.0}},
            {"features": {"x": float("nan"), "y": float("inf")}},
        ]
        # Non-strict branch: warns + zero-fills the offending cells.
        matrix = build_feature_matrix(frame, ["x", "y"], strict=False)
        assert matrix.dtype == np.float32
        assert matrix[1, 0] == 0.0
        assert matrix[1, 1] == 0.0

    def test_empty_frame_returns_float32(self) -> None:
        matrix = build_feature_matrix([], ["x", "y"])
        assert matrix.dtype == np.float32
        assert matrix.shape == (0, 2)


class TestXgbNJobsEnv:
    """Phase 4: ``xgb_n_jobs()`` resolves ``FS_ML_XGB_N_JOBS`` with fallbacks."""

    def test_unset_returns_legacy_minus_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FS_ML_XGB_N_JOBS", raising=False)
        assert xgb_n_jobs() == -1

    def test_blank_returns_legacy_minus_one(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", "   ")
        assert xgb_n_jobs() == -1

    @pytest.mark.parametrize("value,expected", [("1", 1), ("2", 2), ("4", 4), ("-1", -1)])
    def test_valid_int_passes_through(
        self,
        monkeypatch: pytest.MonkeyPatch,
        value: str,
        expected: int,
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", value)
        assert xgb_n_jobs() == expected

    def test_zero_falls_back_to_legacy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """XGBoost rejects ``n_jobs=0``; we treat it as a misconfiguration."""
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", "0")
        assert xgb_n_jobs() == -1

    def test_non_int_falls_back_to_legacy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", "all")
        assert xgb_n_jobs() == -1


class TestSelectFeaturesHonorsXgbNJobs:
    """Phase 4: ``xgb_boruta.select_features`` constructs its inner XGBRegressor with ``xgb_n_jobs()``.

    A regression here (e.g. someone hardcoding ``n_jobs=-1`` back) would
    re-quadruple Boruta's per-thread histogram buffers on a 4-core worker,
    which is the single dial that mattered most for getting under the
    12 GB budget. Patches ``BorutaPy`` and ``XGBRegressor`` so the test
    doesn't actually fit anything — we just need to capture the
    constructor kwargs.
    """

    @staticmethod
    def _capture_xgb_kwargs(
        monkeypatch: pytest.MonkeyPatch,
    ) -> dict[str, Any]:
        captured: dict[str, Any] = {}

        def fake_xgb_regressor(**kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            return MagicMock(name="FakeXGBRegressor")

        class FakeBorutaPy:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.support_ = np.array([True])
                self.support_weak_ = None
                self.ranking_ = np.array([1])
                self.n_iter_ = 1

            def fit(self, *args: Any, **kwargs: Any) -> None:
                return None

        # Patch the modules ``select_features`` imports from at call time.
        import boruta
        import xgboost

        monkeypatch.setattr(xgboost, "XGBRegressor", fake_xgb_regressor)
        monkeypatch.setattr(boruta, "BorutaPy", FakeBorutaPy)
        return captured

    def test_select_features_uses_xgb_n_jobs_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FS_ML_XGB_N_JOBS", raising=False)
        captured = self._capture_xgb_kwargs(monkeypatch)

        from include.nascar.ml.xgb_boruta import select_features

        X = np.zeros((3, 1), dtype=np.float32)
        y = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        select_features(X, y, ["x"], max_iter=1)

        # Unset env -> legacy ``-1`` (preserves old caller behavior outside
        # the configured worker).
        assert captured["n_jobs"] == -1

    def test_select_features_respects_env_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", "1")
        captured = self._capture_xgb_kwargs(monkeypatch)

        from include.nascar.ml.xgb_boruta import select_features

        X = np.zeros((3, 1), dtype=np.float32)
        y = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        select_features(X, y, ["x"], max_iter=1)

        # Compose default is 2; this test pins ``1`` as the "extra
        # squeeze" lever documented in the Memory tuning table.
        assert captured["n_jobs"] == 1


class TestTuneXgbHonorsXgbNJobs:
    """Phase 4: ``xgb_tuning.tune_xgb`` threads ``xgb_n_jobs()`` through every fit.

    We patch ``XGBRegressor`` and the ``RandomizedSearchCV`` so the test
    exercises only the constructor-kwarg plumbing. The TUNE_DISABLE
    short-circuit is the cheapest entry point that still hits the
    constructor.
    """

    def test_disabled_tuning_path_uses_xgb_n_jobs(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_N_JOBS", "1")
        monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")

        captured: dict[str, Any] = {}

        def fake_xgb_regressor(**kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            estimator = MagicMock(name="FakeXGBRegressor")
            estimator.best_iteration = 5
            return estimator

        import xgboost

        # ``tune_xgb`` resolves the estimator class via
        # ``_task_config`` which does ``from xgboost import XGBRegressor``;
        # patching the attribute on the ``xgboost`` module is the natural
        # interception point.
        monkeypatch.setattr(xgboost, "XGBRegressor", fake_xgb_regressor)

        from include.nascar.ml.xgb_tuning import tune_xgb

        X = np.zeros((6, 2), dtype=np.float32)
        y = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        tune_xgb(X, y)

        assert captured["n_jobs"] == 1


class TestBorutaMaxIterEnv:
    """Phase 4: ``_resolve_boruta_max_iter`` reads the right env var per branch."""

    def test_regular_cohort_defaults_to_legacy_50(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FS_ML_BORUTA_MAX_ITER", raising=False)
        assert _resolve_boruta_max_iter(small_cohort=False) == _BORUTA_MAX_ITER_DEFAULT
        assert _BORUTA_MAX_ITER_DEFAULT == 50  # pin the documented legacy

    def test_small_cohort_defaults_to_legacy_10(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FS_ML_BORUTA_SMALL_COHORT_MAX_ITER", raising=False)
        assert (
            _resolve_boruta_max_iter(small_cohort=True)
            == _BORUTA_SMALL_COHORT_MAX_ITER_DEFAULT
        )
        assert _BORUTA_SMALL_COHORT_MAX_ITER_DEFAULT == 10

    def test_regular_cohort_env_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_BORUTA_MAX_ITER", "7")
        assert _resolve_boruta_max_iter(small_cohort=False) == 7

    def test_small_cohort_env_override(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_BORUTA_SMALL_COHORT_MAX_ITER", "3")
        assert _resolve_boruta_max_iter(small_cohort=True) == 3

    def test_regular_cohort_ignores_small_cohort_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Small-cohort env must not leak into the regular-cohort path."""
        monkeypatch.setenv("FS_ML_BORUTA_SMALL_COHORT_MAX_ITER", "3")
        monkeypatch.delenv("FS_ML_BORUTA_MAX_ITER", raising=False)
        assert _resolve_boruta_max_iter(small_cohort=False) == _BORUTA_MAX_ITER_DEFAULT

    @pytest.mark.parametrize(
        "raw,expected",
        [("0", _BORUTA_MAX_ITER_DEFAULT), ("-5", _BORUTA_MAX_ITER_DEFAULT), ("abc", _BORUTA_MAX_ITER_DEFAULT)],
    )
    def test_invalid_value_falls_back_to_default(
        self,
        monkeypatch: pytest.MonkeyPatch,
        raw: str,
        expected: int,
    ) -> None:
        """Non-positive or non-int values must not silently push Boruta into a 0-iter (no-op) run."""
        monkeypatch.setenv("FS_ML_BORUTA_MAX_ITER", raw)
        assert _resolve_boruta_max_iter(small_cohort=False) == expected


class TestTrainCohortThreadsBorutaMaxIter:
    """Phase 4: ``_train_cohort`` forwards the resolved ``boruta_max_iter`` to ``train_models_for_targets``.

    A regression here would mean the env knob silently no-ops at the
    cohort entrypoint — the unit test on ``_resolve_boruta_max_iter`` is
    insufficient because the value still has to make it through
    ``_train_cohort``'s call site.
    """

    def test_train_cohort_forwards_env_override_to_pipeline(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_BORUTA_MAX_ITER", "11")

        captured: dict[str, Any] = {}

        def fake_train_models_for_targets(*args: Any, **kwargs: Any) -> list[Any]:
            captured.update(kwargs)
            return []

        with patch(
            "include.nascar.ml.train_task.train_models_for_targets",
            side_effect=fake_train_models_for_targets,
        ):
            from include.nascar.ml import train_task

            # Provide just enough state for ``_train_cohort`` to reach
            # ``train_models_for_targets``; everything downstream (the
            # persistence loop, evals) never runs because ``trained`` is
            # empty.
            rows = [
                {
                    "ml_training_snapshot_id": 1,
                    "week_id": 100,
                    "track_id": 7,
                    "car_driver_id": 42,
                    "features": {"x": 1.0},
                    "targets": {"r_finish_position": 5.0},
                    "dfs_points": 25.0,
                }
            ]
            train_task._train_cohort(
                series_id=1,
                track_type_id=2,
                track_id_anchor=7,
                rows=rows,
                feature_keys=["x"],
                cohort_n_weeks=20,  # well above ``2 * cv_splits``
                holdout_weeks=1,
                dev_weeks=0,
                artifact_root=__import__("pathlib").Path("/tmp/test_artifacts"),
                hook=MagicMock(),
                scoring_rules={"version": 1, "components": []},
                targets=("r_finish_position",),
            )

        assert captured.get("boruta_max_iter") == 11


class TestXgbDeviceEnv:
    """``xgb_device()`` resolves ``FS_ML_XGB_DEVICE`` with defensive fallbacks.

    The env var is the single dial that flips every cohort-training
    XGBoost constructor between CPU and GPU. A regression in the
    parser (e.g. tightening to ``cpu`` / ``cuda`` only and rejecting
    ``cuda:0``) would silently route the worker back to CPU when an
    operator pinned a specific GPU index — exactly the kind of
    drift we want a unit test to catch before it hits production.
    """

    def test_unset_returns_cpu(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("FS_ML_XGB_DEVICE", raising=False)
        assert xgb_device() == "cpu"

    def test_blank_returns_cpu(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Blank / whitespace is what Compose substitutes when an unset
        ``${FS_ML_XGB_DEVICE:-}`` is interpolated; it must not raise."""
        monkeypatch.setenv("FS_ML_XGB_DEVICE", "  ")
        assert xgb_device() == "cpu"

    @pytest.mark.parametrize("value", ["cpu", "CPU", "Cpu"])
    def test_cpu_case_insensitive(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_DEVICE", value)
        assert xgb_device() == "cpu"

    @pytest.mark.parametrize("value", ["cuda", "CUDA", "Cuda"])
    def test_cuda_case_insensitive(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_DEVICE", value)
        assert xgb_device() == "cuda"

    @pytest.mark.parametrize("value", ["cuda:0", "cuda:1", "CUDA:2"])
    def test_cuda_with_index_preserved(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        """``cuda:N`` is passed through verbatim because XGBoost ≥ 2.0
        parses the suffix natively. Case-folded so operators can be
        loose about capitalization in ``.env``."""
        monkeypatch.setenv("FS_ML_XGB_DEVICE", value)
        assert xgb_device() == value.lower()

    @pytest.mark.parametrize(
        "value",
        ["gpu", "rocm", "metal", "cuda;0", "0", "1", "nvidia"],
    )
    def test_invalid_value_falls_back_to_cpu(
        self, monkeypatch: pytest.MonkeyPatch, value: str
    ) -> None:
        """A typo or invalid device string must not crash training.
        Falling back to CPU with a warning is the documented contract
        (see ``FS_ML_XGB_DEVICE`` entry in .env.example)."""
        monkeypatch.setenv("FS_ML_XGB_DEVICE", value)
        assert xgb_device() == "cpu"


class TestSelectFeaturesHonorsXgbDevice:
    """``xgb_boruta.select_features`` constructs its inner XGBRegressor with ``xgb_device()``.

    Boruta is the highest-fit-count GPU consumer in the pipeline (one
    ``estimator.fit`` per Boruta iteration, up to 50 per target per
    cohort). A regression that dropped the ``device`` kwarg would
    pin Boruta to CPU even when the rest of the pipeline is on GPU
    — splitting the audit story across two devices and producing
    inconsistent ``ml_models.hyperparams_json`` rows. Mirrors
    ``TestSelectFeaturesHonorsXgbNJobs``.
    """

    @staticmethod
    def _capture_xgb_kwargs(
        monkeypatch: pytest.MonkeyPatch,
    ) -> dict[str, Any]:
        captured: dict[str, Any] = {}

        def fake_xgb_regressor(**kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            return MagicMock(name="FakeXGBRegressor")

        class FakeBorutaPy:
            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self.support_ = np.array([True])
                self.support_weak_ = None
                self.ranking_ = np.array([1])
                self.n_iter_ = 1

            def fit(self, *args: Any, **kwargs: Any) -> None:
                return None

        import boruta
        import xgboost

        monkeypatch.setattr(xgboost, "XGBRegressor", fake_xgb_regressor)
        monkeypatch.setattr(boruta, "BorutaPy", FakeBorutaPy)
        return captured

    def test_select_features_uses_cpu_by_default(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("FS_ML_XGB_DEVICE", raising=False)
        captured = self._capture_xgb_kwargs(monkeypatch)

        from include.nascar.ml.xgb_boruta import select_features

        X = np.zeros((3, 1), dtype=np.float32)
        y = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        select_features(X, y, ["x"], max_iter=1)

        assert captured["device"] == "cpu"

    def test_select_features_respects_cuda_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_DEVICE", "cuda")
        captured = self._capture_xgb_kwargs(monkeypatch)

        from include.nascar.ml.xgb_boruta import select_features

        X = np.zeros((3, 1), dtype=np.float32)
        y = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        select_features(X, y, ["x"], max_iter=1)

        assert captured["device"] == "cuda"

    def test_select_features_respects_cuda_index(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Index-pinned form must reach the constructor verbatim so an
        operator can route training to a specific GPU when more than
        one is visible."""
        monkeypatch.setenv("FS_ML_XGB_DEVICE", "cuda:1")
        captured = self._capture_xgb_kwargs(monkeypatch)

        from include.nascar.ml.xgb_boruta import select_features

        X = np.zeros((3, 1), dtype=np.float32)
        y = np.array([1.0, 2.0, 3.0], dtype=np.float32)
        select_features(X, y, ["x"], max_iter=1)

        assert captured["device"] == "cuda:1"


class TestTuneXgbHonorsXgbDevice:
    """``xgb_tuning.tune_xgb`` threads ``xgb_device()`` through the disabled-tuning fit.

    The TUNE_DISABLE short-circuit is the cheapest entry point that
    still hits the XGBoost constructor — sufficient to pin the
    contract that ``device`` flows from the env var to the
    estimator kwargs. The full ``RandomizedSearchCV`` path is
    exercised by the integration tests under ``tests/integration``;
    here we only need the constructor plumbing.
    """

    def test_disabled_tuning_path_uses_xgb_device(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("FS_ML_XGB_DEVICE", "cuda")
        monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")

        captured: dict[str, Any] = {}

        def fake_xgb_regressor(**kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            estimator = MagicMock(name="FakeXGBRegressor")
            estimator.best_iteration = 5
            return estimator

        import xgboost

        monkeypatch.setattr(xgboost, "XGBRegressor", fake_xgb_regressor)

        from include.nascar.ml.xgb_tuning import tune_xgb

        X = np.zeros((6, 2), dtype=np.float32)
        y = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        tune_xgb(X, y)

        assert captured["device"] == "cuda"

    def test_disabled_tuning_path_defaults_to_cpu(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """The disabled-tuning path must not silently route to GPU when
        the env var is unset — explicit opt-in only."""
        monkeypatch.delenv("FS_ML_XGB_DEVICE", raising=False)
        monkeypatch.setenv("FS_ML_TUNE_DISABLE", "1")

        captured: dict[str, Any] = {}

        def fake_xgb_regressor(**kwargs: Any) -> MagicMock:
            captured.update(kwargs)
            estimator = MagicMock(name="FakeXGBRegressor")
            estimator.best_iteration = 5
            return estimator

        import xgboost

        monkeypatch.setattr(xgboost, "XGBRegressor", fake_xgb_regressor)

        from include.nascar.ml.xgb_tuning import tune_xgb

        X = np.zeros((6, 2), dtype=np.float32)
        y = np.array([1, 2, 3, 4, 5, 6], dtype=np.float32)
        tune_xgb(X, y)

        assert captured["device"] == "cpu"


class TestBilliardAlias:
    """Pins the ``import billiard as multiprocessing`` + ``fork`` context contract.

    Two failure modes this guards against, both observed in
    production:

    - Someone "fixes" the unusual ``import billiard as
      multiprocessing`` alias by reverting to stdlib. The next
      training run dies with ``AssertionError: daemonic processes
      are not allowed to have children`` because the Celery prefork
      worker is daemonic and stdlib's Process.start() refuses to
      let daemons spawn children.
    - Someone "fixes" the unusual ``get_context('fork')`` by
      reverting to ``spawn``. The spawn child re-imports
      ``train_task`` from scratch, which transitively runs
      ``airflow.__init__`` → ``configure_orm()``, which crashes on
      ``sqlalchemy.exc.ArgumentError`` because the Celery worker
      container intentionally has no ``SQL_ALCHEMY_CONN``.

    The cost of either regression is a 100% failure rate on
    ``train_models_if_needed``. CPU-only — no actual subprocess is
    spawned; we only verify the context object's surface.
    """

    def test_billiard_fork_context_resolves(self) -> None:
        """``train_task.multiprocessing`` must be billiard, and its
        fork context must expose the ``Process`` / ``Queue`` surface
        that :func:`_run_cohort_isolated` relies on."""
        from include.nascar.ml import train_task

        # Sanity: the module is billiard, not stdlib. ``__name__`` is
        # how billiard identifies itself even when imported under
        # the ``multiprocessing`` alias.
        assert train_task.multiprocessing.__name__ == "billiard"

        ctx = train_task.multiprocessing.get_context("fork")
        # Required surface for ``_run_cohort_isolated``: ``Queue`` and
        # ``Process`` factories on the context.
        assert hasattr(ctx, "Queue")
        assert hasattr(ctx, "Process")
        # Sanity-check the context flavor so an accidental swap to spawn
        # gets caught: ForkContext is the billiard class we want.
        assert type(ctx).__name__ == "ForkContext"

        # ``multiprocessing.queues.Queue`` is referenced as a *string*
        # forward-ref in the ``_isolated_cohort_entry`` annotation, so it
        # never needs to be resolved at runtime. The submodule still has
        # to exist for static checkers and for anyone who replaces the
        # forward-ref with a real import — pin that it does (billiard
        # lazily attaches it on import, hence the explicit import here
        # rather than ``hasattr(train_task.multiprocessing, "queues")``
        # which would fail before the submodule is touched).
        import importlib

        queues_mod = importlib.import_module(
            f"{train_task.multiprocessing.__name__}.queues"
        )
        assert hasattr(queues_mod, "Queue")

    def test_billiard_spawn_context_still_available(self) -> None:
        """Document that the swap to fork was deliberate, not because billiard
        broke spawn. ``billiard.get_context('spawn')`` still resolves; we
        just don't use it for cohort isolation because the spawn child
        crashes on airflow's ORM init (see
        :func:`include.nascar.ml.train_task._run_cohort_isolated`).
        """
        from include.nascar.ml import train_task

        spawn_ctx = train_task.multiprocessing.get_context("spawn")
        assert hasattr(spawn_ctx, "Process")
        assert type(spawn_ctx).__name__ == "SpawnContext"
