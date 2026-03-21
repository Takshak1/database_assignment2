"""Integration test for the Step 8 ingestion endpoint."""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import schema_registry_api as api
from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from crud_executor import HybridCRUDExecutor
from buffer_queue import BufferQueue


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    """Boot the FastAPI app with isolated registry/DB for each test run."""

    db_path = tmp_path / "schema_registry_test.db"
    registry = SchemaRegistry(db_path=str(db_path))
    api.registry = registry
    api.query_engine = CRUDQueryEngine(registry=registry)
    api.crud_executor = HybridCRUDExecutor(registry=registry)
    api.buffer_queue = BufferQueue(db_path=registry.db_path)
    return TestClient(api.app)


def _register_demo_schema(client: TestClient) -> int:
    schema_payload = {
        "username": {"type": "string", "unique": True},
        "post_id": {"type": "integer"},
        "comments": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "text": {"type": "string"},
                    "time": {"type": "integer"},
                },
            },
        },
    }
    response = client.post(
        "/register_schema",
        json={"entity": "test_entity", "schema": schema_payload},
    )
    response.raise_for_status()
    return response.json()["schema"]["schema_id"]


def test_ingest_endpoint_returns_insert_plan(client: TestClient) -> None:
    schema_id = _register_demo_schema(client)

    payload = {
        "username": "user1",
        "post_id": 123,
        "comments": [{"text": "nice", "time": 123}],
    }
    response = client.post(
        f"/ingest/{schema_id}",
        json={"payload": payload, "execute": False},
    )

    assert response.status_code == 200
    body = response.json()
    assert body["crud"]["operation"] == "insert"
    assert body["crud"]["executed"] is False
    assert "sql" in body["crud"]["details"]
    assert "mongo" in body["crud"]["details"]
    assert body["message"] == "Ingestion processed"