"""Tests for the buffer promotion service."""

from __future__ import annotations

from pathlib import Path

import pytest

from schema_registry import SchemaRegistry
from crud_executor import HybridCRUDExecutor
from buffer_queue import BufferQueue
from buffer_promoter import BufferPromoter


@pytest.fixture()
def promoter_setup(tmp_path: Path):
    registry = SchemaRegistry(db_path=str(tmp_path / "registry.db"))
    buffer_queue = BufferQueue(db_path=registry.db_path)
    crud_executor = HybridCRUDExecutor(registry=registry)
    promoter = BufferPromoter(
        registry=registry,
        buffer_queue=buffer_queue,
        crud_executor=crud_executor,
    )
    return promoter, registry, buffer_queue


def test_promoter_processes_ready_entries(promoter_setup) -> None:
    promoter, registry, buffer_queue = promoter_setup
    schema = {
        "username": {"type": "string", "unique": True},
        "post_id": {"type": "integer"},
    }
    stored = registry.register_schema("entity", schema)
    payload = {"username": "user1", "post_id": 42}
    buffer_queue.enqueue(
        stored["schema_id"],
        "username",
        payload["username"],
        payload=payload,
    )

    stats = promoter.promote(schema_id=stored["schema_id"], execute=False)

    assert stats["processed"] == 1
    pending = buffer_queue.list_entries(status="pending")
    assert pending == []
