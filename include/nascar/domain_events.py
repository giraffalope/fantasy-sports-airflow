"""
Application-level domain events for NASCAR importer DAGs (Airflow-first).

Events are emitted **after** successful templated SQL execution in Python callables, or from
``on_success_callback`` on :class:`airflow_utils.mysql_template_file_operator.MySQLTemplateFileOperatorFromContext`
when there is no surrounding callable (Cup schedule load/import).

**Event catalog (Cup importer ``nascar.cup.importer``)**

Each row is one successful execution of the corresponding step. ``entity_tables`` are logical
core names (staging/temp tables use ``table_suffix`` / ``week_id`` in the database).

+-------------------------------+------------------------------------------+----------------------------------+
| Importer step / task_id       | ``event_type``                           | ``entity_tables`` (typical)      |
+===============================+==========================================+==================================+
| ``load_stage_schedule_csvs``   | ``CupImporterEventType.SCHEDULE_STAGED`` | ``stg_cup_frcs_schedule_import``, ``stg_cup_racing_reference_schedule_import`` |
+-------------------------------+------------------------------------------+----------------------------------+
| ``import_schedule``           | ``CupImporterEventType.SCHEDULE_IMPORTED`` | ``series``, ``seasons``, ``tracks``, ``weeks``, ``data_sources``, ``data_sources_tracks`` |
+-------------------------------+------------------------------------------+----------------------------------+
| entrylist staging (loop)      | ``ENTRYLIST_STAGED``                     | ``stg_frcs_entrylist``           |
+-------------------------------+------------------------------------------+----------------------------------+
| ``import_entrylist``          | ``ENTRYLIST_IMPORTED``                   | ``teams``, ``data_sources_teams`` |
+-------------------------------+------------------------------------------+----------------------------------+
| practice staging              | ``PRACTICE_STAGED``                      | ``stg_frcs_practice``            |
+-------------------------------+------------------------------------------+----------------------------------+
| ``import_practices``          | ``PRACTICE_IMPORTED``                    | ``practices``, ``practice_results``, ``cars``, ``drivers``, ``cars_drivers``, ``manufacturers`` |
+-------------------------------+------------------------------------------+----------------------------------+
| qualifying staging / import   | ``QUALIFYING_*``                         | staging / ``qualifyings``, ``qualifying_results``, ``cars_drivers``, … |
+-------------------------------+------------------------------------------+----------------------------------+
| lineup staging / import       | ``LINEUP_*``                             | staging / ``races``, ``race_results``, ``cars_drivers``, … |
+-------------------------------+------------------------------------------+----------------------------------+
| race staging / import         | ``RACE_*``                               | staging / ``race_results``, ``cars_drivers``, … |
+-------------------------------+------------------------------------------+----------------------------------+
| penalties staging / import    | ``PENALTIES_*``                          | ``penalties``, …                 |
+-------------------------------+------------------------------------------+----------------------------------+
| pit stops staging / import    | ``PIT_STOPS_*``                          | ``pit_stops``, …                 |
+-------------------------------+------------------------------------------+----------------------------------+
| DraftKings staging / import   | ``DRAFTKINGS_*``                         | ``dfs``, ``dfs_salaries``, …     |
+-------------------------------+------------------------------------------+----------------------------------+
| FanDuel staging / import      | ``FANDUEL_*``                            | ``dfs``, ``dfs_salaries``, …     |
+-------------------------------+------------------------------------------+----------------------------------+

**Consumers:** configure ``NASCAR_DOMAIN_EVENTS_BACKEND`` (and optional HTTP env vars). Any HTTP
receiver (for example a future Laravel webhook) should validate auth, return **202** quickly, and
queue heavy work. This module does **not** ship a consumer.

**Non-goals (v1):** per-row payloads, MySQL triggers on all writer tables, NASCAR DB outbox, coupling
to a specific SaaS.

Environment:

- ``NASCAR_DOMAIN_EVENTS_BACKEND``: ``noop`` (default), ``log``, or ``http``.
- ``NASCAR_DOMAIN_EVENTS_HTTP_URL``: POST target for ``http`` backend (JSON body).
- ``NASCAR_DOMAIN_EVENTS_HTTP_BEARER``: optional ``Authorization: Bearer …``.
- ``NASCAR_DOMAIN_EVENTS_HTTP_HMAC_SECRET``: optional hex signature
  ``X-Nascar-Domain-Events-Signature: sha256=<hmac_sha256_hex_of_raw_body>``.
- ``NASCAR_DOMAIN_EVENTS_HTTP_TIMEOUT_SECONDS``: default ``10``.
- ``NASCAR_DOMAIN_EVENTS_HTTP_RAISE_ON_ERROR``: if ``1``/``true``, HTTP non-success or network
  errors raise; default is log-only so tasks are not failed by the notifier.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from typing import Any

_LOG = logging.getLogger(__name__)

SCHEMA_VERSION = 1

_BACKEND_ENV = "NASCAR_DOMAIN_EVENTS_BACKEND"
_HTTP_URL_ENV = "NASCAR_DOMAIN_EVENTS_HTTP_URL"
_HTTP_BEARER_ENV = "NASCAR_DOMAIN_EVENTS_HTTP_BEARER"
_HTTP_HMAC_SECRET_ENV = "NASCAR_DOMAIN_EVENTS_HTTP_HMAC_SECRET"
_HTTP_TIMEOUT_ENV = "NASCAR_DOMAIN_EVENTS_HTTP_TIMEOUT_SECONDS"
_HTTP_RAISE_ENV = "NASCAR_DOMAIN_EVENTS_HTTP_RAISE_ON_ERROR"


class CupImporterEventType:
    """Stable ``event_type`` strings for the Cup scraper importer DAG."""

    SCHEDULE_STAGED = "nascar.cup.schedule.staged"
    SCHEDULE_IMPORTED = "nascar.cup.schedule.imported"
    ENTRYLIST_STAGED = "nascar.cup.entrylist.staged"
    ENTRYLIST_IMPORTED = "nascar.cup.entrylist.imported"
    PRACTICE_STAGED = "nascar.cup.practice.staged"
    PRACTICE_IMPORTED = "nascar.cup.practice.imported"
    QUALIFYING_STAGED = "nascar.cup.qualifying.staged"
    QUALIFYING_IMPORTED = "nascar.cup.qualifying.imported"
    LINEUP_STAGED = "nascar.cup.lineup.staged"
    LINEUP_IMPORTED = "nascar.cup.lineup.imported"
    RACE_STAGED = "nascar.cup.race.staged"
    RACE_IMPORTED = "nascar.cup.race.imported"
    PENALTIES_STAGED = "nascar.cup.penalties.staged"
    PENALTIES_IMPORTED = "nascar.cup.penalties.imported"
    PIT_STOPS_STAGED = "nascar.cup.pit_stops.staged"
    PIT_STOPS_IMPORTED = "nascar.cup.pit_stops.imported"
    DRAFTKINGS_STAGED = "nascar.cup.draftkings_import.staged"
    DRAFTKINGS_IMPORTED = "nascar.cup.draftkings_import.imported"
    FANDUEL_STAGED = "nascar.cup.fanduel_import.staged"
    FANDUEL_IMPORTED = "nascar.cup.fanduel_import.imported"


@dataclass(frozen=True, slots=True)
class DomainEventAirflow:
    dag_id: str
    run_id: str
    task_id: str
    logical_date: str | None


@dataclass(frozen=True, slots=True)
class DomainEvent:
    event_type: str
    entity_tables: tuple[str, ...]
    airflow: DomainEventAirflow
    payload: dict[str, Any] = field(default_factory=dict)
    schema_version: int = SCHEMA_VERSION

    def to_json_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["entity_tables"] = list(self.entity_tables)
        return d


def _logical_date_iso(m: Mapping[str, Any]) -> str | None:
    for key in ("logical_date", "data_interval_start"):
        v = m.get(key)
        if v is not None:
            iso = getattr(v, "isoformat", None)
            if callable(iso):
                return iso()
            return str(v)
    dr = m.get("dag_run")
    if dr is not None:
        for attr in ("logical_date", "data_interval_start"):
            v = getattr(dr, attr, None)
            if v is not None:
                iso = getattr(v, "isoformat", None)
                if callable(iso):
                    return iso()
                return str(v)
    return None


def airflow_context_from_mapping(context: Mapping[str, Any]) -> DomainEventAirflow:
    """
    Build :class:`DomainEventAirflow` from an Airflow task context dict or PythonOperator kwargs.

    ``logical_date`` is an ISO-8601 string when available.
    """
    dag = context["dag"]
    ti = context["ti"]
    dag_id = getattr(dag, "dag_id", "") or ""
    task_id = getattr(ti, "task_id", "") or ""
    dr = context.get("dag_run")
    run_id = ""
    if dr is not None:
        run_id = str(getattr(dr, "run_id", "") or "")
    if not run_id:
        run_id = str(context.get("run_id") or "")
    return DomainEventAirflow(
        dag_id=dag_id,
        run_id=run_id,
        task_id=task_id,
        logical_date=_logical_date_iso(context),
    )


def idempotency_key(event: DomainEvent) -> str:
    """Stable short key for ``Idempotency-Key`` (hash of run, task, type, and payload)."""
    af = event.airflow
    payload = json.dumps(event.payload, sort_keys=True, separators=(",", ":"), default=str)
    raw = f"{af.run_id}\n{af.task_id}\n{event.event_type}\n{payload}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class DomainEventBackend(ABC):
    @abstractmethod
    def emit(self, event: DomainEvent) -> None: ...


class NoopDomainEventBackend(DomainEventBackend):
    def emit(self, _event: DomainEvent) -> None:
        return


class LogJsonDomainEventBackend(DomainEventBackend):
    def emit(self, event: DomainEvent) -> None:
        line = json.dumps(event.to_json_dict(), separators=(",", ":"), default=str)
        _LOG.info("nascar_domain_event %s", line)


class HttpDomainEventBackend(DomainEventBackend):
    def __init__(
        self,
        *,
        url: str,
        bearer: str | None,
        hmac_secret: str | None,
        timeout_seconds: float,
        raise_on_error: bool,
    ) -> None:
        self._url = url
        self._bearer = bearer
        self._hmac_secret = hmac_secret
        self._timeout_seconds = timeout_seconds
        self._raise_on_error = raise_on_error

    def emit(self, event: DomainEvent) -> None:
        import requests

        body = json.dumps(event.to_json_dict(), separators=(",", ":"), default=str).encode("utf-8")
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "Idempotency-Key": idempotency_key(event),
        }
        if self._bearer:
            headers["Authorization"] = f"Bearer {self._bearer}"
        if self._hmac_secret:
            sig = hmac.new(self._hmac_secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
            headers["X-Nascar-Domain-Events-Signature"] = f"sha256={sig}"
        try:
            resp = requests.post(
                self._url,
                data=body,
                headers=headers,
                timeout=self._timeout_seconds,
            )
            if 200 <= resp.status_code < 300:
                return
            msg = (
                f"nascar_domain_event_http status={resp.status_code} "
                f"body_prefix={resp.text[:500]!r}"
            )
            if self._raise_on_error:
                raise RuntimeError(msg)
            _LOG.warning(msg)
        except Exception:
            if self._raise_on_error:
                raise
            _LOG.exception("nascar_domain_event_http failed (suppressed)")


def _parse_bool(raw: str) -> bool:
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _resolve_backend() -> DomainEventBackend:
    name = os.environ.get(_BACKEND_ENV, "noop").strip().lower() or "noop"
    if name in {"", "noop", "none", "off"}:
        return NoopDomainEventBackend()
    if name == "log":
        return LogJsonDomainEventBackend()
    if name == "http":
        url = os.environ.get(_HTTP_URL_ENV, "").strip()
        if not url:
            _LOG.warning(
                "%s is unset; NASCAR_DOMAIN_EVENTS_BACKEND=http is ignored (noop)",
                _HTTP_URL_ENV,
            )
            return NoopDomainEventBackend()
        bearer = os.environ.get(_HTTP_BEARER_ENV, "").strip() or None
        hmac_secret = os.environ.get(_HTTP_HMAC_SECRET_ENV, "").strip() or None
        timeout_raw = os.environ.get(_HTTP_TIMEOUT_ENV, "10").strip()
        try:
            timeout_seconds = float(timeout_raw)
        except ValueError:
            timeout_seconds = 10.0
        raise_on_error = _parse_bool(os.environ.get(_HTTP_RAISE_ENV, ""))
        return HttpDomainEventBackend(
            url=url,
            bearer=bearer,
            hmac_secret=hmac_secret,
            timeout_seconds=timeout_seconds,
            raise_on_error=raise_on_error,
        )
    _LOG.warning("unknown %s=%r; using noop backend", _BACKEND_ENV, name)
    return NoopDomainEventBackend()


_backend: DomainEventBackend | None = None


def _get_backend() -> DomainEventBackend:
    global _backend
    if _backend is None:
        _backend = _resolve_backend()
    return _backend


def reset_backend_cache_for_tests() -> None:
    global _backend
    _backend = None


def emit(event: DomainEvent) -> None:
    """Dispatch ``event`` to the backend selected by ``NASCAR_DOMAIN_EVENTS_BACKEND``."""
    _get_backend().emit(event)
