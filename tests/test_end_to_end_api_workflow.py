"""Comprehensive end-to-end API workflow test.

Covers schema registration, metadata retrieval, query planning,
CRUD insert/read/update/delete planning, and ingestion routing.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

import schema_registry_api as api
from buffer_queue import BufferQueue
from crud_executor import HybridCRUDExecutor
from crud_query_engine import CRUDQueryEngine
from schema_registry import SchemaRegistry


@pytest.fixture()
def client(tmp_path: Path) -> TestClient:
    """Boot app with isolated registry and queue for deterministic end-to-end validation."""

    db_path = tmp_path / "e2e_registry.db"
    registry = SchemaRegistry(db_path=str(db_path))
    api.registry = registry
    api.query_engine = CRUDQueryEngine(registry=registry)
    api.crud_executor = HybridCRUDExecutor(registry=registry)
    api.buffer_queue = BufferQueue(db_path=registry.db_path)
    return TestClient(api.app)


def _schema_payload() -> dict:
    return {
        "entity": "user_activity",
        "schema": {
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
            "profile": {
                "type": "object",
                "properties": {
                    "address": {"type": "string"},
                    "city": {"type": "string"},
                },
            },
        },
    }


def _record_payload() -> dict:
    return {
        "username": "user1",
        "post_id": 42,
        "comments": [
            {"text": "nice", "time": 100},
            {"text": "great", "time": 120},
        ],
        "profile": {"address": "MG Road", "city": "Ahmedabad"},
    }


def test_full_api_workflow_from_query_to_metadata_and_crud(client: TestClient) -> None:
    """Validate end-to-end workflow: register -> metadata -> plan -> CRUD -> ingest."""

    # 1) Register schema
    register_res = client.post("/register_schema", json=_schema_payload())
    assert register_res.status_code == 200
    schema_id = register_res.json()["schema"]["schema_id"]

    # 2) Validate schema + metadata retrieval
    schema_res = client.get(f"/schemas/{schema_id}")
    assert schema_res.status_code == 200
    schema_body = schema_res.json()
    assert schema_body["entity_name"] == "user_activity"
    assert schema_body["sql_blueprint"]["root_table"] == "user_activity"
    assert len(schema_body["fields"]) >= 4
    assert schema_body["storage_strategy"]["mappings"]["fields"]
    analysis_entries = schema_body["analysis"]["entries"]
    assert any(entry["field_path"] == "comments" for entry in analysis_entries)
    comments_entry = next(entry for entry in analysis_entries if entry["field_path"] == "comments")
    assert comments_entry["classification"] == "repeating_entity"
    assert comments_entry["pipeline"] in {"sql", "mongo", "buffer"}
    sql_tables = {table["name"]: table for table in schema_body["sql_blueprint"]["tables"]}
    assert {"user_activity", "comments", "profile"}.issubset(set(sql_tables))
    assert sql_tables["comments"]["primary_key"].endswith("_id")
    assert sql_tables["comments"]["foreign_keys"][0]["to_table"] == "user_activity"

    # 3) Query plan generation (read)
    query_plan_res = client.post(
        f"/schemas/{schema_id}/query_plan",
        json={
            "operation": "read",
            "fields": ["username", "comments"],
            "filters": {"username": "user1"},
            "limit": 10,
        },
    )
    assert query_plan_res.status_code == 200
    read_plan = query_plan_res.json()["plan"]
    assert read_plan["operation"] == "read"
    assert read_plan["sql"]["statement"].startswith("SELECT")
    assert "LIMIT 10" in read_plan["sql"]["statement"]
    assert read_plan["sql"]["where"] and "username" in read_plan["sql"]["where"]
    assert list(read_plan["sql"]["parameters"].values()) == ["user1"]
    assert "comments" in read_plan["sql"]["tables"]
    locations = {loc["requested"]: loc for loc in read_plan["field_locations"]}
    assert locations["username"]["status"] in {"resolved", "hint"}
    assert locations["username"]["storage"] in {"sql", "mongo"}
    assert locations["comments"]["status"] in {"resolved", "hint"}
    assert read_plan["merge"]["merge_key"]
    assert read_plan["merge"]["response_shape"]["requested_fields"] == ["username", "comments"]

    # 4) CRUD Insert dry-run
    insert_res = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "insert",
            "payload": _record_payload(),
            "execute": False,
        },
    )
    assert insert_res.status_code == 200
    insert_details = insert_res.json()["result"]["details"]
    assert insert_details["plan"]["operation"] == "insert"
    assert insert_details["plan"]["consistency"]["metadata_source"] == "schema_storage_strategies"
    assert insert_details["sql"]["order"][0] == "user_activity"
    assert set(insert_details["sql"]["order"]) == {"user_activity", "comments", "profile"}
    root_row = insert_details["sql"]["rows"]["user_activity"][0]
    assert isinstance(root_row, dict)
    assert insert_details["sql"]["rows"]["profile"][0]["city"] == "Ahmedabad"
    assert insert_details["sql"]["rows"]["profile"][0]["address"] == "MG Road"
    assert len(insert_details["sql"]["rows"]["comments"]) == 2
    assert insert_details["sql"]["rows"]["comments"][0]["text"] == "nice"
    assert insert_details["sql"]["rows"]["comments"][1]["time"] == 120
    assert "comments" in insert_details["sql"]["foreign_keys"]
    assert insert_details["mongo"]["collections"] == {}

    # 5) CRUD Read dry-run
    read_res = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "read",
            "fields": ["username", "comments"],
            "filters": {"username": "user1"},
            "execute": False,
            "limit": 5,
        },
    )
    assert read_res.status_code == 200
    read_details = read_res.json()["result"]["details"]
    assert read_details["operation"] == "read"
    assert "sql" in read_details and read_details["sql"]["statement"].startswith("SELECT")
    assert read_details["sql"]["limit"] == 5
    assert read_details["note"] == "Set execute=true to fetch live data"
    assert read_details["merge"]["response_shape"]["requested_fields"] == ["username", "comments"]

    # 6) CRUD Update dry-run (simple strategy)
    update_res = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "update",
            "strategy": "simple",
            "filters": {"post_id": 42},
            "payload": _record_payload(),
            "execute": False,
        },
    )
    assert update_res.status_code == 200
    update_details = update_res.json()["result"]["details"]
    assert update_details["strategy"] == "simple"
    assert update_details["plan"]["consistency"]["mode"] == "delete_then_insert"
    delete_plan = update_details["plan"]["delete"]
    insert_plan = update_details["plan"]["insert"]
    assert delete_plan["operation"] == "delete"
    assert delete_plan["strategy"] == "entity"
    assert delete_plan["consistency"]["cascade"] is True
    assert delete_plan["sql"]["tables"][-1] == "user_activity"
    assert insert_plan["operation"] == "insert"
    assert len(insert_plan["sql"]["rows"]["comments"]) == 2
    assert insert_plan["sql"]["rows"]["profile"][0]["city"] == "Ahmedabad"

    # 7) CRUD Delete dry-run (sub-entity)
    delete_res = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "delete",
            "strategy": "sub-entity",
            "filters": {"target": "comments", "criteria": {"post_id": 42, "comment_id": 1}},
            "execute": False,
        },
    )
    assert delete_res.status_code == 200
    delete_details = delete_res.json()["result"]["details"]
    assert delete_details["plan"]["strategy"] == "sub-entity"
    assert delete_details["plan"]["consistency"]["cascade"] is False
    assert delete_details["plan"]["filters"] == {"post_id": 42, "comment_id": 1}
    assert delete_details["sql"]["tables"] == ["comments"]
    assert delete_details["mongo"]["collections"] in ([], ["comments"])

    # 8) Ingestion endpoint dry-run with buffer visibility
    ingest_res = client.post(
        f"/ingest/{schema_id}",
        json={"payload": _record_payload(), "execute": False},
    )
    assert ingest_res.status_code == 200
    ingest_body = ingest_res.json()
    assert ingest_body["message"] == "Ingestion processed"
    assert "buffered_fields" in ingest_body
    assert ingest_body["crud"]["operation"] == "insert"
    assert ingest_body["crud"]["executed"] is False
    ingest_details = ingest_body["crud"]["details"]
    assert ingest_details["plan"]["operation"] == "insert"
    assert len(ingest_details["sql"]["rows"]["comments"]) == 2
    assert ingest_details["sql"]["rows"]["comments"][0]["text"] == "nice"
    assert len(ingest_body["buffered_fields"]) == 0

    # 9) Optional sanity check list endpoint still includes this schema
    list_res = client.get("/schemas")
    assert list_res.status_code == 200
    list_body = list_res.json()
    assert list_body["count"] >= 1
    matching = [item for item in list_body["schemas"] if item["schema_id"] == schema_id]
    assert matching, "Registered schema should be discoverable via /schemas"
    assert matching[0]["storage_strategy_summary"]["mapped_fields"] >= 1
