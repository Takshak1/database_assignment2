"""Regression tests: CRUD executor should populate metadata.json from schema registry."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict

from crud_executor import HybridCRUDExecutor
from schema_registry import SchemaRegistry


def test_crud_execute_syncs_metadata_file(tmp_path: Path, sample_schema_definition: Dict[str, Dict[str, object]]) -> None:
    metadata_file = tmp_path / "metadata.json"
    metadata_file.write_text("{}", encoding="utf-8")

    registry = SchemaRegistry(db_path=str(tmp_path / "registry.db"))
    stored = registry.register_schema("post", sample_schema_definition)

    executor = HybridCRUDExecutor(
        registry=registry,
        metadata_file=str(metadata_file),
    )

    # Dry-run read should still trigger metadata sync from schema registry.
    result = executor.execute(
        int(stored["schema_id"]),
        operation="read",
        fields=["username", "comments"],
        filters={"username": "neo"},
        execute=False,
    )

    assert result.operation == "read"

    payload = json.loads(metadata_file.read_text(encoding="utf-8"))
    assert payload, "metadata.json should be populated after CRUD execution"
    assert any(key.endswith("username") for key in payload.keys())

    username_key = next(key for key in payload if key.endswith("username"))
    assert payload[username_key]["placement_decision"] in {"sql", "mongo", "buffer"}
