"""Tests for :mod:`include.nascar.domain_events`."""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from unittest import mock

import pytest

from include.nascar import domain_events as de


@pytest.fixture(autouse=True)
def reset_backend() -> None:
    de.reset_backend_cache_for_tests()
    yield
    de.reset_backend_cache_for_tests()


def test_airflow_context_logical_date_from_dag_run() -> None:
    class _Dag:
        dag_id = "d"

    class _TI:
        task_id = "t"

    class _DR:
        run_id = "manual__2026-04-14"
        logical_date = datetime(2026, 4, 14, 12, 0, tzinfo=UTC)

    af = de.airflow_context_from_mapping({"dag": _Dag(), "ti": _TI(), "dag_run": _DR()})
    assert af.logical_date is not None
    assert "2026-04-14" in af.logical_date


def test_airflow_context_from_mapping() -> None:
    class _Dag:
        dag_id = "nascar.cup.scraper.importer"

    class _TI:
        task_id = "import_practices"

    class _DR:
        run_id = "scheduled__2026-04-14T12:00:00+00:00"
        logical_date = None

    ctx = {"dag": _Dag(), "ti": _TI(), "dag_run": _DR()}
    af = de.airflow_context_from_mapping(ctx)
    assert af.dag_id == "nascar.cup.scraper.importer"
    assert af.task_id == "import_practices"
    assert af.run_id.startswith("scheduled__")
    assert af.logical_date is None


def test_idempotency_key_stable() -> None:
    af = de.DomainEventAirflow(
        dag_id="d",
        run_id="r1",
        task_id="t1",
        logical_date=None,
    )
    e1 = de.DomainEvent(
        event_type="nascar.cup.test",
        entity_tables=("a", "b"),
        airflow=af,
        payload={"x": 1},
    )
    e2 = de.DomainEvent(
        event_type="nascar.cup.test",
        entity_tables=("a", "b"),
        airflow=af,
        payload={"x": 1},
    )
    assert de.idempotency_key(e1) == de.idempotency_key(e2)
    e3 = de.DomainEvent(
        event_type="nascar.cup.test",
        entity_tables=("a", "b"),
        airflow=af,
        payload={"x": 2},
    )
    assert de.idempotency_key(e1) != de.idempotency_key(e3)


def test_to_json_dict_roundtrip() -> None:
    af = de.DomainEventAirflow(
        dag_id="d",
        run_id="r",
        task_id="t",
        logical_date="2026-04-14T00:00:00+00:00",
    )
    ev = de.DomainEvent(
        event_type="nascar.cup.schedule.imported",
        entity_tables=("weeks", "tracks"),
        airflow=af,
        payload={"season_year": 2026},
    )
    raw = json.dumps(ev.to_json_dict(), separators=(",", ":"))
    d = json.loads(raw)
    assert d["schema_version"] == de.SCHEMA_VERSION
    assert d["entity_tables"] == ["weeks", "tracks"]
    assert d["airflow"]["dag_id"] == "d"


def test_emit_noop_default(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NASCAR_DOMAIN_EVENTS_BACKEND", raising=False)
    de.emit(
        de.DomainEvent(
            event_type="x",
            entity_tables=(),
            airflow=de.DomainEventAirflow("d", "r", "t", None),
        )
    )


def test_emit_log_backend(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_BACKEND", "log")
    caplog.set_level(logging.INFO)
    de.reset_backend_cache_for_tests()
    de.emit(
        de.DomainEvent(
            event_type="nascar.cup.test",
            entity_tables=("weeks",),
            airflow=de.DomainEventAirflow("d", "r", "t", None),
            payload={"k": "v"},
        )
    )
    assert any("nascar_domain_event" in r.message for r in caplog.records)


def test_http_backend_posts_json(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_BACKEND", "http")
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_HTTP_URL", "https://example.test/hook")
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_HTTP_BEARER", "secret-token")
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_HTTP_HMAC_SECRET", "hmac-secret")
    de.reset_backend_cache_for_tests()
    ev = de.DomainEvent(
        event_type="e",
        entity_tables=("t",),
        airflow=de.DomainEventAirflow("dag", "run", "task", None),
        payload={},
    )
    with mock.patch("requests.post") as post:
        de.emit(ev)
    post.assert_called_once()
    _args, kwargs = post.call_args
    assert str(_args[0]) == "https://example.test/hook"
    assert kwargs["headers"]["Authorization"] == "Bearer secret-token"
    assert kwargs["headers"]["Content-Type"] == "application/json"
    assert kwargs["headers"]["Idempotency-Key"] == de.idempotency_key(ev)
    assert kwargs["headers"]["X-Nascar-Domain-Events-Signature"].startswith("sha256=")
    body = kwargs["data"]
    assert b'"dag_id":"dag"' in body


def test_http_backend_without_url_is_noop(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_BACKEND", "http")
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_HTTP_URL", "")
    caplog.set_level(logging.WARNING)
    de.reset_backend_cache_for_tests()
    de.emit(
        de.DomainEvent(
            event_type="e",
            entity_tables=(),
            airflow=de.DomainEventAirflow("d", "r", "t", None),
        )
    )
    assert any("NASCAR_DOMAIN_EVENTS_HTTP_URL" in r.message for r in caplog.records)


def test_unknown_backend_is_noop(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NASCAR_DOMAIN_EVENTS_BACKEND", "kafka")
    de.reset_backend_cache_for_tests()
    de.emit(
        de.DomainEvent(
            event_type="e",
            entity_tables=(),
            airflow=de.DomainEventAirflow("d", "r", "t", None),
        )
    )


def test_schedule_operators_have_domain_event_callbacks() -> None:
    import nascar.cup.scraper.importer as importer_dag

    load_t = importer_dag.dag.get_task("load_stage_schedule_csvs")
    import_t = importer_dag.dag.get_task("import_schedule")
    load_cb = load_t.on_success_callback
    import_cb = import_t.on_success_callback
    if isinstance(load_cb, (list, tuple)):
        load_cb = load_cb[0]
    if isinstance(import_cb, (list, tuple)):
        import_cb = import_cb[0]
    assert load_cb is importer_dag._emit_cup_schedule_staged_domain_event
    assert import_cb is importer_dag._emit_cup_schedule_imported_domain_event
