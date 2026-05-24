"""Unit tests for DFS path helpers."""

from __future__ import annotations

from pathlib import Path

from include.nascar.dfs import paths


def test_draftkings_salaries_paths(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("AIRFLOW_DFS_ROOT", str(tmp_path))
    assert paths.dfs_root() == tmp_path
    directory = paths.draftkings_salaries_dir(2026, 10)
    assert directory == tmp_path / "draftkings" / "nascar" / "cup" / "2026" / "10"
    assert paths.draftkings_salaries_csv(2026, 10) == directory / "DKSalaries.csv"


def test_parse_draftkings_salary_path() -> None:
    from include.nascar.dfs.salary_import_task import _parse_draftkings_salary_path

    path = Path(
        "/home/airflow/dfs/draftkings/nascar/cup/2026/10/DKSalaries.csv"
    )
    assert _parse_draftkings_salary_path(path) == (2026, 10)
    assert _parse_draftkings_salary_path(path.with_name("other.csv")) is None
