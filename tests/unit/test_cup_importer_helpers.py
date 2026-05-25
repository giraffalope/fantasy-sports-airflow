"""Unit tests for Cup importer helper functions."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import nascar.cup.importer as importer_mod
import pytest


def test_require_car_map_table_returns_name_when_table_exists() -> None:
    hook = MagicMock()
    with patch.object(importer_mod, "_nascar_staging_table_exists", return_value=True):
        name = importer_mod._require_car_map_table(
            hook, week_id=101, suffix="202601010000", context="practice"
        )
    assert name == "tmp_car_101_202601010000"


def test_require_car_map_table_raises_when_missing() -> None:
    hook = MagicMock()
    with patch.object(importer_mod, "_nascar_staging_table_exists", return_value=False):
        with pytest.raises(RuntimeError, match="tmp_car_77_202601010000.*week_id=77"):
            importer_mod._require_car_map_table(
                hook, week_id=77, suffix="202601010000", context="qualifying"
            )
