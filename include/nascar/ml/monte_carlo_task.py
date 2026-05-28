"""Airflow entrypoint for the Monte Carlo race-simulation task.

Loads the latest active ``ml_predictions`` for the upcoming-week predict snapshot,
attaches a per-target σ derived from the most recent ``ml_model_evaluations.mae``
row for each target's series, runs the pure simulator, and persists one batch
header plus per-driver summaries inside a single transaction. Any prior active
batch for the same snapshot is soft-deleted in the same transaction so re-runs
remain idempotent.

Skip conditions mirror predict / lineup generator:

* ``no_active_rules`` — no active ``dfs_scoring_rules`` row for DraftKings.
* ``no_predict_snapshot`` — no current-week ``kind='predict'`` snapshot.
* ``no_ml_predictions`` — predict snapshot has no active ``ml_predictions`` rows.
* ``no_primary_target_sigma`` — finish-position model has no usable mae/rmse.
"""

from __future__ import annotations

import json
import logging
import os
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import numpy as np

from airflow_utils.db_cursor import DbCursor
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.model_scope import (
    INTERVAL_METHOD_CONFORMAL,
    INTERVAL_METHOD_GLOBAL_MAE,
    interval_method,
)
from include.nascar.ml.monte_carlo import (
    DEFAULT_SIMULATIONS,
    MIN_SIMULATIONS,
    MonteCarloBatchResult,
    MonteCarloDriverInput,
    MonteCarloDriverSummary,
    simulate_driver_distributions,
)
from include.nascar.ml.predict_task import _active_rules, _latest_predict_snapshot
from include.nascar.ml.snapshot_config import DRAFTKINGS_DFS_ID
from include.nascar.ml.xgb_train_pipeline import DEFAULT_TARGETS, PRIMARY_TARGET

_LOG = logging.getLogger(__name__)

# MAE for normally distributed residuals is σ * sqrt(2/π); converting back to σ
# requires dividing by this factor when only MAE is available.
_MAE_TO_SIGMA = 1.0 / 0.7978845608028654

# E1 + E2: conformal half-width = (q90 - q10) / 2. Treating this as the σ of a
# Normal that covers ~80% of holdout residuals lets the Monte Carlo draws use
# the same shape they do today while sourcing the dispersion per-cohort. This
# is the same trick scikit's split-conformal helpers apply.
_CONFORMAL_QUANTILES = ("q10", "q90")


def _parse_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def n_simulations() -> int:
    """Resolve ``FS_MC_N_SIMULATIONS``.

    Returns :data:`DEFAULT_SIMULATIONS` (``10000``) when the env var is
    unset or unparseable; downstream validation in
    :func:`simulate_driver_distributions` still enforces the
    :data:`MIN_SIMULATIONS` (``1000``) floor for any explicit override.
    """
    raw = os.environ.get("FS_MC_N_SIMULATIONS")
    if raw is None or raw.strip() == "":
        return DEFAULT_SIMULATIONS
    try:
        value = int(raw)
    except ValueError:
        _LOG.warning(
            "Invalid FS_MC_N_SIMULATIONS=%r; falling back to default %s",
            raw,
            DEFAULT_SIMULATIONS,
        )
        return DEFAULT_SIMULATIONS
    return value


def random_seed(snapshot_id: int) -> int:
    """Use ``FS_MC_RANDOM_SEED`` when set, else derive a stable seed from snapshot id."""
    raw = os.environ.get("FS_MC_RANDOM_SEED")
    if raw is not None and raw.strip() != "":
        try:
            return int(raw)
        except ValueError:
            _LOG.warning(
                "Invalid FS_MC_RANDOM_SEED=%r; deriving from snapshot_id",
                raw,
            )
    return int(snapshot_id) * 1_000_003 + 17


def pipeline_version() -> str | None:
    return os.environ.get("FS_PIPELINE_VERSION") or os.environ.get("GIT_COMMIT")


_DEFAULT_MC_ARTIFACT_ROOT = "/opt/airflow/artifacts/mc"


def mc_artifact_root() -> Path:
    """Resolve ``FS_MC_ARTIFACT_ROOT`` for joint-samples sidecar files.

    Defaults to ``/opt/airflow/artifacts/mc`` so the path is identical
    between worker containers and the host (compose mounts a host
    ``artifacts/`` directory onto the worker). Returned as :class:`Path`
    so callers can ``.mkdir(parents=True, exist_ok=True)`` defensively.
    """
    raw = os.environ.get("FS_MC_ARTIFACT_ROOT")
    return Path(raw) if raw and raw.strip() else Path(_DEFAULT_MC_ARTIFACT_ROOT)


def _write_joint_samples_artifact(
    batch_result: MonteCarloBatchResult,
    *,
    batch_id: int,
    n_simulations_value: int,
    random_seed_value: int,
    snapshot_id: int,
) -> str | None:
    """Persist the joint draws as a ``.npz`` sidecar; return absolute path or None on failure.

    Uses numpy's compressed archive instead of parquet to avoid adding a
    pyarrow dependency to the worker image. The on-disk layout is:

    - ``joint_dfs_points``: float matrix ``(n_drivers, n_simulations)``
    - ``car_driver_ids``: int64 vector ``(n_drivers,)`` aligned with the matrix rows
    - ``joint_finish_position``: float matrix ``(n_drivers, n_simulations)`` —
      per-iteration assigned finish position (post tail-permutation for DNFed)
    - ``joint_total_fastest_laps``: float matrix ``(n_drivers, n_simulations)`` —
      post-constraint fastest-laps count (already completion-fraction scaled
      for DNFed iterations)
    - ``joint_total_laps_led``: float matrix ``(n_drivers, n_simulations)`` —
      post-constraint laps-led count (completion-fraction scaled for DNFed)
    - ``joint_is_dnf``: int8 matrix ``(n_drivers, n_simulations)`` —
      per-iteration Bernoulli outcome from the DNF draw (0 / 1)
    - ``meta``: ``np.void`` carrying ``batch_id`` / ``snapshot_id`` /
      ``n_simulations`` / ``random_seed`` for offline lineage cross-checks

    The per-target / DNF arrays are written best-effort; legacy MC pipeline
    versions wrote only ``joint_dfs_points`` + ``car_driver_ids`` + ``meta``
    so downstream readers (lineup task) must keep treating the per-target
    arrays as optional.

    Failure to write is logged but never raised — the per-driver summary
    persistence is the source of truth for the lineup task's fallback
    branch, so a degraded artifact write leaves the pipeline running.
    """
    root = mc_artifact_root()
    try:
        root.mkdir(parents=True, exist_ok=True)
        # ``np.savez_compressed`` is lossless and built into numpy, so no
        # extra dependency is required. File size for 40 drivers x 10000
        # draws x 8 bytes x 4 float matrices ≈ 13 MB before compression;
        # compressed is typically 30-50% of that, plus a sub-MB int8
        # ``joint_is_dnf`` archive entry.
        artifact_path = root / f"{batch_id}.npz"
        meta = np.array(
            (
                int(batch_id),
                int(snapshot_id),
                int(n_simulations_value),
                int(random_seed_value),
            ),
            dtype=[
                ("batch_id", "i8"),
                ("snapshot_id", "i8"),
                ("n_simulations", "i8"),
                ("random_seed", "i8"),
            ],
        )
        extras: dict[str, np.ndarray] = {}
        if batch_result.joint_finish_position is not None:
            extras["joint_finish_position"] = batch_result.joint_finish_position
        if batch_result.joint_total_fastest_laps is not None:
            extras["joint_total_fastest_laps"] = batch_result.joint_total_fastest_laps
        if batch_result.joint_total_laps_led is not None:
            extras["joint_total_laps_led"] = batch_result.joint_total_laps_led
        if batch_result.joint_is_dnf is not None:
            extras["joint_is_dnf"] = batch_result.joint_is_dnf
        np.savez_compressed(
            artifact_path,
            joint_dfs_points=batch_result.joint_dfs_points,
            car_driver_ids=batch_result.driver_index,
            meta=meta,
            **extras,
        )
    except Exception:
        _LOG.exception(
            "run_monte_carlo_simulation: joint_samples artifact write failed "
            "batch_id=%s snapshot_id=%s",
            batch_id,
            snapshot_id,
        )
        return None
    return str(artifact_path.resolve())


def _update_batch_artifact_uri(
    hook: NascarMySqlHook, *, batch_id: int, artifact_uri: str
) -> None:
    """Stamp the joint-samples artifact URI onto an already-inserted batch row.

    Done in a separate write so the transactional ``_persist_batch`` insert
    can finish before the (potentially slow) parquet/npz file flush; if the
    file write fails the UPDATE is simply skipped and the column stays NULL.
    """
    hook.run(
        """
        UPDATE nascar.ml_race_simulation_batches
        SET joint_samples_artifact_uri = %s
        WHERE id = %s
          AND deleted_at IS NULL
        """,
        parameters=(artifact_uri, batch_id),
    )


def _load_predictions(
    hook: NascarMySqlHook, snapshot_id: int
) -> list[dict[str, Any]]:
    rows = hook.get_records(
        """
        SELECT mp.car_driver_id, mtr.r_start_position,
               mp.predicted_targets_json, mp.blend_metadata_json
        FROM nascar.ml_predictions mp
        JOIN nascar.ml_training_rows mtr
            ON mtr.ml_training_snapshot_id = mp.ml_training_snapshot_id
           AND mtr.car_driver_id = mp.car_driver_id
           AND mtr.deleted_at IS NULL
        WHERE mp.ml_training_snapshot_id = %s
          AND mp.deleted_at IS NULL
        ORDER BY mp.car_driver_id ASC
        """,
        parameters=(snapshot_id,),
    )
    parsed: list[dict[str, Any]] = []
    for car_driver_id, r_start_position, predicted_targets_json, blend_meta_json in rows:
        parsed.append(
            {
                "car_driver_id": int(car_driver_id),
                "r_start_position": (
                    float(r_start_position) if r_start_position is not None else None
                ),
                "predicted_targets": _parse_json(predicted_targets_json) or {},
                # E2: per-driver blend lineage. ``conformal_intervals``
                # under this dict is the per-target ``{q05, q10, q50,
                # q90, q95}`` already weight-averaged by predict-time
                # blend so Monte Carlo can size each driver's σ from
                # the cohorts that produced that driver's prediction.
                "blend_metadata": _parse_json(blend_meta_json) or {},
            }
        )
    return parsed


def _conformal_sigma_from_intervals(
    intervals: Mapping[str, float] | None,
) -> float | None:
    """E1 + E2: ``σ ≈ (q90 - q10) / 2`` from a per-target quantile dict.

    Returns ``None`` when either bound is missing or the half-width is
    non-positive (which would shrink the Monte Carlo draws to a delta).
    A small positive floor protects against degenerate cohorts whose
    holdout residuals are all identical.
    """
    if not intervals:
        return None
    q10 = intervals.get(_CONFORMAL_QUANTILES[0])
    q90 = intervals.get(_CONFORMAL_QUANTILES[1])
    if q10 is None or q90 is None:
        return None
    try:
        half_width = (float(q90) - float(q10)) / 2.0
    except (TypeError, ValueError):
        return None
    if half_width <= 0.0:
        return None
    return half_width


def _load_target_sigma(
    hook: NascarMySqlHook,
    *,
    series_id: int,
    target: str,
) -> float | None:
    """Return σ for ``target`` derived from the most recent active mae evaluation.

    Each model targets a single stat (``JSON_LENGTH(fs.target_keys) = 1`` in
    routing), so the join keeps the σ aligned with the model that produced
    predictions for that target. ``mae`` is the per-target metric written by
    :func:`include.nascar.ml.train_task._persist_trained_model`; we convert it
    to σ assuming Gaussian residuals (σ ≈ mae / 0.7979). When ``rmse`` is added
    in a later iteration it should be preferred and used directly.
    """
    rows = hook.get_records(
        """
        SELECT mme.metric, mme.metric_value
        FROM nascar.ml_model_evaluations mme
        JOIN nascar.ml_models mm
            ON mm.id = mme.ml_model_id
           AND mm.deleted_at IS NULL
           AND mm.series_id = %s
        JOIN nascar.ml_feature_sets fs
            ON fs.id = mm.ml_feature_set_id
           AND fs.deleted_at IS NULL
        WHERE mme.deleted_at IS NULL
          AND mme.metric IN ('rmse', 'mae')
          AND JSON_CONTAINS(fs.target_keys, JSON_QUOTE(%s))
          AND JSON_LENGTH(fs.target_keys) = 1
        ORDER BY (mme.metric = 'rmse') DESC, mme.created_at DESC, mme.id DESC
        LIMIT 1
        """,
        parameters=(int(series_id), target),
    )
    if not rows:
        return None
    metric, metric_value = rows[0]
    value = float(metric_value)
    if metric == "rmse":
        return value
    return value * _MAE_TO_SIGMA


# Keys that ride along in ``predicted_targets_json`` but are NOT scoring sources.
# ``r_finish_position_raw`` is the D1 audit float behind the reconciled rank;
# ``r_is_dnf`` is the D3 blended P_dnf the two-stage classifier exports for the
# MC Bernoulli draw. Both are stripped from the simulator-facing target dict
# (one is replayed into :attr:`MonteCarloDriverInput.p_dnf`, the other is
# pure lineage).
_MC_METADATA_KEYS: frozenset[str] = frozenset({"r_finish_position_raw", "r_is_dnf"})
# Kept as a back-compat alias for older callers.
_NON_NUMERIC_TARGET_SUFFIXES: frozenset[str] = _MC_METADATA_KEYS


def _resolve_per_driver_sigmas(
    row: Mapping[str, Any],
    *,
    method: str,
    fallback: Mapping[str, float],
    targets: Mapping[str, float],
) -> dict[str, float]:
    """E1 + E2 + E4 σ resolution for one ml_predictions row.

    ``method=conformal`` (default) reads ``blend_metadata_json.conformal_intervals``
    (a per-target ``{q05, q10, q50, q90, q95}`` dict written by predict)
    and converts each entry to ``σ ≈ (q90 - q10) / 2``. Missing or
    degenerate per-target slots silently fall back to the ``global_mae``
    σ for that target — never to zero — so Monte Carlo never shrinks to
    a delta. ``method=global_mae`` uses the legacy global σ unchanged.
    """
    if method == INTERVAL_METHOD_GLOBAL_MAE:
        return {
            target: sigma
            for target, sigma in fallback.items()
            if target in targets
        }
    intervals_root = (row.get("blend_metadata") or {}).get("conformal_intervals") or {}
    resolved: dict[str, float] = {}
    for target in targets:
        sigma = _conformal_sigma_from_intervals(intervals_root.get(target))
        if sigma is None:
            sigma = fallback.get(target)
        if sigma is not None and sigma > 0.0:
            resolved[target] = float(sigma)
    return resolved


def _build_driver_inputs(
    predictions: list[dict[str, Any]],
    target_sigmas: dict[str, float],
    *,
    method: str = INTERVAL_METHOD_CONFORMAL,
) -> list[MonteCarloDriverInput]:
    drivers: list[MonteCarloDriverInput] = []
    for row in predictions:
        targets_raw = row["predicted_targets"] or {}
        # ``predicted_targets_json`` carries two non-scoring metadata keys:
        # ``r_finish_position_raw`` (D1 audit float behind the integer rank)
        # and ``r_is_dnf`` (D3 blended P_dnf from the two-stage classifier).
        # Strip both before handing the dict to the simulator and surface
        # ``r_is_dnf`` separately on :attr:`MonteCarloDriverInput.p_dnf`
        # so the per-iteration Bernoulli DNF draw can fire.
        p_dnf_raw = targets_raw.get("r_is_dnf")
        p_dnf = (
            float(p_dnf_raw)
            if isinstance(p_dnf_raw, (int, float))
            else None
        )
        targets: dict[str, float] = {
            key: float(value)
            for key, value in targets_raw.items()
            if isinstance(value, (int, float)) and key not in _MC_METADATA_KEYS
        }
        sigmas = _resolve_per_driver_sigmas(
            row,
            method=method,
            fallback=target_sigmas,
            targets=targets,
        )
        drivers.append(
            MonteCarloDriverInput(
                car_driver_id=row["car_driver_id"],
                r_start_position=row["r_start_position"],
                predicted_targets=targets,
                target_sigmas=sigmas,
                p_dnf=p_dnf,
            )
        )
    return drivers


def _persist_batch(
    hook: NascarMySqlHook,
    *,
    snapshot_id: int,
    n_sims: int,
    seed: int,
    pipeline_ver: str | None,
    summaries: list[MonteCarloDriverSummary],
) -> int:
    """Soft-delete prior active batch for ``snapshot_id`` and insert the new one."""
    inserted_id_holder: dict[str, int] = {}

    def _write(_conn: object, cursor: DbCursor) -> None:
        cursor.execute(
            """
            UPDATE nascar.ml_race_simulation_batches b
            LEFT JOIN nascar.ml_race_simulation_driver_summaries s
                ON s.ml_race_simulation_batch_id = b.id
            SET b.deleted_at = UTC_TIMESTAMP(),
                s.deleted_at = UTC_TIMESTAMP()
            WHERE b.ml_training_snapshot_id = %s
              AND b.deleted_at IS NULL
            """,
            (snapshot_id,),
        )
        cursor.execute(
            """
            INSERT INTO nascar.ml_race_simulation_batches (
                ml_training_snapshot_id,
                n_simulations,
                random_seed,
                pipeline_version,
                status
            ) VALUES (%s, %s, %s, %s, 'completed')
            """,
            (snapshot_id, n_sims, seed, pipeline_ver),
        )
        batch_id = int(cursor.lastrowid)
        if not batch_id:
            raise RuntimeError(
                "INSERT ml_race_simulation_batches did not return an auto-increment id"
            )
        inserted_id_holder["id"] = batch_id
        if summaries:
            cursor.executemany(
                """
                INSERT INTO nascar.ml_race_simulation_driver_summaries (
                    ml_race_simulation_batch_id,
                    car_driver_id,
                    n_simulations,
                    mean_dfs_points,
                    std_dfs_points,
                    min_dfs_points,
                    p05_dfs_points,
                    p50_dfs_points,
                    p95_dfs_points,
                    max_dfs_points,
                    mean_r_finish_position,
                    std_r_finish_position,
                    min_r_finish_position,
                    p05_r_finish_position,
                    p50_r_finish_position,
                    p95_r_finish_position,
                    max_r_finish_position,
                    mean_r_total_fastest_laps,
                    std_r_total_fastest_laps,
                    min_r_total_fastest_laps,
                    p05_r_total_fastest_laps,
                    p50_r_total_fastest_laps,
                    p95_r_total_fastest_laps,
                    max_r_total_fastest_laps,
                    mean_r_total_laps_led,
                    std_r_total_laps_led,
                    min_r_total_laps_led,
                    p05_r_total_laps_led,
                    p50_r_total_laps_led,
                    p95_r_total_laps_led,
                    max_r_total_laps_led,
                    dnf_pct
                ) VALUES (
                    %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s,
                    %s
                )
                """,
                [
                    (
                        batch_id,
                        summary.car_driver_id,
                        summary.n_simulations,
                        round(summary.mean_dfs_points, 3),
                        round(summary.std_dfs_points, 3),
                        round(summary.min_dfs_points, 3),
                        round(summary.p05_dfs_points, 3),
                        round(summary.p50_dfs_points, 3),
                        round(summary.p95_dfs_points, 3),
                        round(summary.max_dfs_points, 3),
                        round(summary.mean_r_finish_position, 3),
                        round(summary.std_r_finish_position, 3),
                        round(summary.min_r_finish_position, 3),
                        round(summary.p05_r_finish_position, 3),
                        round(summary.p50_r_finish_position, 3),
                        round(summary.p95_r_finish_position, 3),
                        round(summary.max_r_finish_position, 3),
                        round(summary.mean_r_total_fastest_laps, 3),
                        round(summary.std_r_total_fastest_laps, 3),
                        round(summary.min_r_total_fastest_laps, 3),
                        round(summary.p05_r_total_fastest_laps, 3),
                        round(summary.p50_r_total_fastest_laps, 3),
                        round(summary.p95_r_total_fastest_laps, 3),
                        round(summary.max_r_total_fastest_laps, 3),
                        round(summary.mean_r_total_laps_led, 3),
                        round(summary.std_r_total_laps_led, 3),
                        round(summary.min_r_total_laps_led, 3),
                        round(summary.p05_r_total_laps_led, 3),
                        round(summary.p50_r_total_laps_led, 3),
                        round(summary.p95_r_total_laps_led, 3),
                        round(summary.max_r_total_laps_led, 3),
                        round(summary.dnf_pct, 4),
                    )
                    for summary in summaries
                ],
            )

    hook.run_in_transaction(_write)
    return inserted_id_holder["id"]


_POST_RACE_MC_PRESENT_SQL = """
SELECT
    EXISTS (
        SELECT 1
        FROM nascar.agg_week_results awr
        WHERE awr.week_id = %s
          AND awr.deleted_at IS NULL
          AND awr.r_finish_position IS NOT NULL
    ) AS race_finished,
    EXISTS (
        SELECT 1
        FROM nascar.ml_race_simulation_batches b
        WHERE b.ml_training_snapshot_id = %s
          AND b.deleted_at IS NULL
    ) AS mc_present
"""


def _post_race_mc_already_present(
    hook: NascarMySqlHook,
    *,
    week_id: int,
    snapshot_id: int,
) -> bool:
    """Return True when the race has finished AND an MC batch already exists.

    Symmetric to predict / lineup post-race guards: once the snapshot's inputs
    are frozen (race results landed) and we've already produced a Monte Carlo
    batch for it, re-simulating every hour would just append redundant batches
    keyed on the same snapshot. Pre-race we still re-simulate hourly so the
    sims track refreshed predictions.
    """
    rows = hook.get_records(
        _POST_RACE_MC_PRESENT_SQL,
        parameters=(int(week_id), int(snapshot_id)),
    )
    if not rows:
        return False
    race_finished, mc_present = rows[0]
    return bool(race_finished) and bool(mc_present)


def run_monte_carlo_simulation_task(**_context: object) -> dict[str, Any]:
    """Run Monte Carlo over the latest predict snapshot and persist summaries."""
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    rules = _active_rules(hook, DRAFTKINGS_DFS_ID)
    if rules is None:
        _LOG.warning("run_monte_carlo_simulation: skipped reason=no_active_rules")
        return {"skipped": True, "reason": "no_active_rules"}
    rules_id, scoring_rules = rules

    snapshot = _latest_predict_snapshot(hook, rules_id)
    if snapshot is None:
        _LOG.warning("run_monte_carlo_simulation: skipped reason=no_predict_snapshot")
        return {"skipped": True, "reason": "no_predict_snapshot"}

    if _post_race_mc_already_present(
        hook,
        week_id=int(snapshot["week_id"]),
        snapshot_id=int(snapshot["id"]),
    ):
        _LOG.info(
            "run_monte_carlo_simulation: snapshot_id=%s week_id=%s race finished and "
            "MC batch already present; skipping post-race re-simulation",
            snapshot["id"],
            snapshot["week_id"],
        )
        return {
            "skipped": True,
            "reason": "mc_already_present_post_race",
            "snapshot_id": snapshot["id"],
            "week_id": snapshot["week_id"],
        }

    predictions = _load_predictions(hook, snapshot["id"])
    if not predictions:
        _LOG.warning(
            "run_monte_carlo_simulation: skipped reason=no_ml_predictions snapshot_id=%s",
            snapshot["id"],
        )
        return {
            "skipped": True,
            "reason": "no_ml_predictions",
            "snapshot_id": snapshot["id"],
        }

    series_id = int(snapshot.get("series_id") or NascarSeries.CUP.series_id)
    target_sigmas: dict[str, float] = {}
    for target in DEFAULT_TARGETS:
        sigma = _load_target_sigma(hook, series_id=series_id, target=target)
        if sigma is not None and sigma > 0.0:
            target_sigmas[target] = sigma

    if PRIMARY_TARGET not in target_sigmas:
        _LOG.warning(
            "run_monte_carlo_simulation: skipped reason=no_primary_target_sigma "
            "snapshot_id=%s target=%s",
            snapshot["id"],
            PRIMARY_TARGET,
        )
        return {
            "skipped": True,
            "reason": "no_primary_target_sigma",
            "snapshot_id": snapshot["id"],
            "primary_target": PRIMARY_TARGET,
        }

    method = interval_method()
    drivers = _build_driver_inputs(predictions, target_sigmas, method=method)
    n_sims = n_simulations()
    seed = random_seed(snapshot["id"])
    field_size = len(drivers)

    batch_result = simulate_driver_distributions(
        drivers,
        scoring_rules=scoring_rules,
        n_simulations=n_sims,
        field_size=field_size,
        seed=seed,
    )
    summaries = batch_result.summaries

    batch_id = _persist_batch(
        hook,
        snapshot_id=snapshot["id"],
        n_sims=n_sims,
        seed=seed,
        pipeline_ver=pipeline_version(),
        summaries=summaries,
    )

    # Joint-samples sidecar: written AFTER the batch insert so the
    # ``ml_race_simulation_driver_summaries`` source-of-truth lands first
    # and an artifact-write failure cannot corrupt the per-driver
    # persistence. The lineup task gracefully falls back to summary-derived
    # independent samples when this URI is NULL.
    artifact_uri = _write_joint_samples_artifact(
        batch_result,
        batch_id=batch_id,
        n_simulations_value=n_sims,
        random_seed_value=seed,
        snapshot_id=int(snapshot["id"]),
    )
    if artifact_uri is not None:
        _update_batch_artifact_uri(hook, batch_id=batch_id, artifact_uri=artifact_uri)

    _LOG.info(
        "run_monte_carlo_simulation: snapshot_id=%s batch_id=%s drivers=%s "
        "n_simulations=%s joint_samples_artifact_uri=%s",
        snapshot["id"],
        batch_id,
        len(summaries),
        n_sims,
        artifact_uri,
    )
    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "batch_id": batch_id,
        "n_simulations": n_sims,
        "random_seed": seed,
        "field_size": field_size,
        "drivers": len(summaries),
        "interval_method": method,
        "target_sigmas": {target: round(sigma, 6) for target, sigma in target_sigmas.items()},
        "joint_samples_artifact_uri": artifact_uri,
    }


def validate_simulation_batch_task(**_context: object) -> dict[str, Any]:
    """Fail the DAG run if the latest batch has fewer than ``MIN_SIMULATIONS`` draws.

    Also validates that the number of active driver summaries matches the
    active prediction pool for the snapshot, so re-run / partial-write bugs
    surface as a hard task failure rather than silent under-counting.
    """
    hook = NascarMySqlHook(mysql_conn_id="nascar_conn")

    rules = _active_rules(hook, DRAFTKINGS_DFS_ID)
    if rules is None:
        _LOG.warning("validate_simulation_batch: skipped reason=no_active_rules")
        return {"skipped": True, "reason": "no_active_rules"}
    rules_id, _ = rules

    snapshot = _latest_predict_snapshot(hook, rules_id)
    if snapshot is None:
        _LOG.warning("validate_simulation_batch: skipped reason=no_predict_snapshot")
        return {"skipped": True, "reason": "no_predict_snapshot"}

    batch_rows = hook.get_records(
        """
        SELECT id, n_simulations
        FROM nascar.ml_race_simulation_batches
        WHERE ml_training_snapshot_id = %s
          AND deleted_at IS NULL
        ORDER BY id DESC
        LIMIT 1
        """,
        parameters=(snapshot["id"],),
    )
    if not batch_rows:
        raise RuntimeError(
            f"validate_simulation_batch: no active batch for snapshot_id={snapshot['id']}"
        )
    batch_id = int(batch_rows[0][0])
    batch_n = int(batch_rows[0][1])
    if batch_n < MIN_SIMULATIONS:
        raise RuntimeError(
            f"validate_simulation_batch: batch_id={batch_id} has n_simulations={batch_n} "
            f"(< minimum {MIN_SIMULATIONS})"
        )

    summary_rows = hook.get_records(
        """
        SELECT COUNT(*)
        FROM nascar.ml_race_simulation_driver_summaries
        WHERE ml_race_simulation_batch_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(batch_id,),
    )
    summaries_n = int(summary_rows[0][0]) if summary_rows else 0

    prediction_rows = hook.get_records(
        """
        SELECT COUNT(*)
        FROM nascar.ml_predictions
        WHERE ml_training_snapshot_id = %s
          AND deleted_at IS NULL
        """,
        parameters=(snapshot["id"],),
    )
    predictions_n = int(prediction_rows[0][0]) if prediction_rows else 0

    if summaries_n != predictions_n:
        raise RuntimeError(
            f"validate_simulation_batch: batch_id={batch_id} summaries={summaries_n} "
            f"does not match prediction pool={predictions_n}"
        )

    _LOG.info(
        "validate_simulation_batch: snapshot_id=%s batch_id=%s n_simulations=%s drivers=%s",
        snapshot["id"],
        batch_id,
        batch_n,
        summaries_n,
    )
    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "batch_id": batch_id,
        "n_simulations": batch_n,
        "drivers": summaries_n,
    }
