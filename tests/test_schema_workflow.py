"""End-to-end tests covering schema registration, planning, CRUD, and aggregation."""

from __future__ import annotations

from pathlib import Path

import pytest

from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from crud_executor import HybridCRUDExecutor
from result_aggregator import ResultAggregator


@pytest.fixture()
def registry(tmp_path: Path) -> SchemaRegistry:
    return SchemaRegistry(db_path=str(tmp_path / "registry.db"))


def _sample_schema() -> dict:
    return {
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


def test_schema_registration_generates_artifacts(registry: SchemaRegistry) -> None:
    stored = registry.register_schema("entity", _sample_schema())

    assert stored["sql_blueprint"]["tables"]
    assert stored["storage_strategy"]["mappings"]["fields"]
    assert stored["mongo_strategy"]


def test_query_engine_produces_sql_and_merge_plan(registry: SchemaRegistry) -> None:
    stored = registry.register_schema("entity", _sample_schema())
    engine = CRUDQueryEngine(registry=registry)

    plan = engine.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "comments"],
            "filters": {"username": "user1"},
        },
    )

    assert plan["sql"]
    assert "statement" in plan["sql"]
    assert plan["merge"]["merge_key"] is not None


def test_crud_executor_returns_read_plan(registry: SchemaRegistry) -> None:
    stored = registry.register_schema("entity", _sample_schema())
    executor = HybridCRUDExecutor(registry=registry)

    result = executor.execute(
        stored["schema_id"],
        operation="read",
        fields=["username", "post_id"],
        filters={"username": "user1"},
        execute=False,
    )

    details = result.details  # type: ignore[attr-defined]
    assert "sql" in details
    assert "note" in details
    assert details["merge"]


def test_result_aggregator_merges_sql_and_mongo(registry: SchemaRegistry) -> None:
    stored = registry.register_schema("entity", _sample_schema())
    engine = CRUDQueryEngine(registry=registry)
    aggregator = ResultAggregator(registry=registry)

    plan = engine.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "post_id"],
        },
    )
    sql_row = _build_fake_sql_row(plan["sql"]["select"], username="user1", post_id=42)
    mongo_doc = {"username": "user1", "comments": [{"text": "nice", "time": 123}]}

    merged = aggregator.aggregate(
        stored["schema_id"],
        sql_rows=[sql_row],
        mongo_rows=[mongo_doc],
        merge_plan={"merge_key": "entity.username"},
    )

    assert merged[0]["username"] == "user1"
    assert merged[0]["comments"][0]["text"] == "nice"
    assert merged[0]["post_id"] == 42


def _build_fake_sql_row(select_clauses, *, username: str, post_id: int) -> dict:
    row = {}
    for clause in select_clauses:
        if " AS " not in clause:
            continue
        alias = clause.split(" AS ")[1]
        if alias.endswith("username"):
            row[alias] = username
        elif alias.endswith("post_id"):
            row[alias] = post_id
    return row
