"""Shared DNF (did-not-finish) helpers for the D3 two-stage model design.

Two surfaces:

- :func:`derive_is_dnf` — given an ``agg_week_results``-shaped row dict,
  return ``1`` if the car did not finish (per the ``r_car_status``
  heuristic) and ``0`` otherwise. The snapshot builder writes this into
  ``ml_labels.targets_json`` so the trainer can fit a binary classifier
  on it.
- :data:`DNF_TARGET_KEY` — canonical target key the trainer / predict
  task switch on to route ``r_is_dnf`` to the binary head.
- :func:`compose_two_stage_primary` / :func:`compose_two_stage_secondary` —
  the predict-time blend formulas from the plan. Pure math so the
  arithmetic is straightforward to unit-test without an XGBoost runtime.

The DNF heuristic mirrors :func:`include.nascar.ml.driver_form._is_dnf` so
both feature engineering (C1 ``df_*`` family) and label derivation (D3)
agree on what counts as a DNF. Schema-wise the underlying column is
``agg_week_results.r_car_status``; ``RUNNING`` (any case) is treated as a
clean finish and any other non-null status as a DNF. ``NULL`` is
conservatively treated as not-DNF so an unknown status doesn't inflate
the DNF rate.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import numpy as np

DNF_TARGET_KEY = "r_is_dnf"
"""Target name in ``ml_labels.targets_json`` for the D3 binary classifier."""

MODEL_KIND_REGRESSION = "regression"
MODEL_KIND_REGRESSION_CLEAN = "regression_clean"
MODEL_KIND_DNF_CLASSIFIER = "dnf_classifier"
MODEL_KIND_QUANTILE = "quantile"

_RUNNING_STATUS = "RUNNING"


def derive_is_dnf(agg_row: Mapping[str, Any]) -> int:
    """Return ``1`` when the row reads as a DNF, ``0`` otherwise.

    Inputs that lack ``r_car_status`` (``NULL`` / missing) return ``0`` so
    a stray-null upstream row doesn't bias the classifier's positive class
    rate upward.
    """
    status = agg_row.get("r_car_status")
    if status is None:
        return 0
    return 0 if str(status).strip().upper() == _RUNNING_STATUS else 1


def compose_two_stage_primary(
    clean_finish: np.ndarray,
    dnf_probability: np.ndarray | None,
    field_size: int,
) -> np.ndarray:
    """``E[finish] = P_dnf * field_size + (1 - P_dnf) * E[finish | clean]``.

    When ``dnf_probability`` is ``None`` the model is treated as legacy
    single-head ``model_kind='regression'`` and ``clean_finish`` passes
    through untouched. ``field_size`` is the number of drivers in the
    predict snapshot — DNFs collapse to that finish position because the
    NASCAR convention sorts them at the back of the field by laps run.
    """
    clean = np.asarray(clean_finish, dtype=float)
    if dnf_probability is None:
        return clean
    dnf = np.asarray(dnf_probability, dtype=float)
    return dnf * float(field_size) + (1.0 - dnf) * clean


def compose_two_stage_secondary(
    clean_value: np.ndarray,
    dnf_probability: np.ndarray | None,
) -> np.ndarray:
    """``E[y] = (1 - P_dnf) * E[y | clean]``.

    The DNF branch contributes zero because the secondary targets
    (``r_total_laps_led``, ``r_total_fastest_laps``, ...) are
    non-negative event counts that collapse to zero on a DNF.
    ``dnf_probability=None`` passes the clean prediction through as a
    legacy single-head fallback.
    """
    clean = np.asarray(clean_value, dtype=float)
    if dnf_probability is None:
        return clean
    dnf = np.asarray(dnf_probability, dtype=float)
    return (1.0 - dnf) * clean
