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
from typing import Any

from airflow_utils.db_cursor import DbCursor
from airflow_utils.nascar_mysql_hook import NascarMySqlHook
from include.nascar.enums import NascarSeries
from include.nascar.ml.monte_carlo import (
    MIN_SIMULATIONS,
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


def _parse_json(raw: Any) -> Any:
    if raw is None:
        return None
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8")
    if isinstance(raw, str):
        return json.loads(raw)
    return raw


def n_simulations() -> int:
    """Resolve ``FS_MC_N_SIMULATIONS`` (default ``1000``); validation enforces ≥ 1000."""
    raw = os.environ.get("FS_MC_N_SIMULATIONS")
    if raw is None or raw.strip() == "":
        return MIN_SIMULATIONS
    try:
        value = int(raw)
    except ValueError:
        _LOG.warning(
            "Invalid FS_MC_N_SIMULATIONS=%r; falling back to default %s",
            raw,
            MIN_SIMULATIONS,
        )
        return MIN_SIMULATIONS
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


def _load_predictions(
    hook: NascarMySqlHook, snapshot_id: int
) -> list[dict[str, Any]]:
    rows = hook.get_records(
        """
        SELECT mp.car_driver_id, mtr.r_start_position, mp.predicted_targets_json
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
    for car_driver_id, r_start_position, predicted_targets_json in rows:
        parsed.append(
            {
                "car_driver_id": int(car_driver_id),
                "r_start_position": (
                    float(r_start_position) if r_start_position is not None else None
                ),
                "predicted_targets": _parse_json(predicted_targets_json) or {},
            }
        )
    return parsed


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


def _build_driver_inputs(
    predictions: list[dict[str, Any]],
    target_sigmas: dict[str, float],
) -> list[MonteCarloDriverInput]:
    drivers: list[MonteCarloDriverInput] = []
    for row in predictions:
        targets_raw = row["predicted_targets"] or {}
        targets: dict[str, float] = {
            key: float(value)
            for key, value in targets_raw.items()
            if isinstance(value, (int, float))
        }
        drivers.append(
            MonteCarloDriverInput(
                car_driver_id=row["car_driver_id"],
                r_start_position=row["r_start_position"],
                predicted_targets=targets,
                target_sigmas={
                    target: sigma
                    for target, sigma in target_sigmas.items()
                    if target in targets
                },
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
                    max_dfs_points
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    )
                    for summary in summaries
                ],
            )

    hook.run_in_transaction(_write)
    return inserted_id_holder["id"]


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

    drivers = _build_driver_inputs(predictions, target_sigmas)
    n_sims = n_simulations()
    seed = random_seed(snapshot["id"])
    field_size = len(drivers)

    summaries = simulate_driver_distributions(
        drivers,
        scoring_rules=scoring_rules,
        n_simulations=n_sims,
        field_size=field_size,
        seed=seed,
    )

    batch_id = _persist_batch(
        hook,
        snapshot_id=snapshot["id"],
        n_sims=n_sims,
        seed=seed,
        pipeline_ver=pipeline_version(),
        summaries=summaries,
    )

    _LOG.info(
        "run_monte_carlo_simulation: snapshot_id=%s batch_id=%s drivers=%s n_simulations=%s",
        snapshot["id"],
        batch_id,
        len(summaries),
        n_sims,
    )
    return {
        "skipped": False,
        "snapshot_id": snapshot["id"],
        "batch_id": batch_id,
        "n_simulations": n_sims,
        "random_seed": seed,
        "field_size": field_size,
        "drivers": len(summaries),
        "target_sigmas": {target: round(sigma, 6) for target, sigma in target_sigmas.items()},
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
