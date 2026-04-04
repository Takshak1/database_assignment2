"""Path resolution behavior for schema registry API runtime files."""

from __future__ import annotations

from pathlib import Path

import schema_registry_api as api


def test_resolve_runtime_path_keeps_absolute_path(tmp_path: Path) -> None:
    absolute = tmp_path / "custom_metadata.json"
    resolved = api._resolve_runtime_path(str(absolute), "metadata.json")
    assert Path(resolved) == absolute


def test_resolve_runtime_path_anchors_relative_to_api_dir() -> None:
    resolved = api._resolve_runtime_path("metadata.json", "fallback.json")
    expected = (api.BASE_DIR / "metadata.json").resolve()
    assert Path(resolved) == expected
