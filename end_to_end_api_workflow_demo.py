"""Standalone end-to-end API workflow demo (non-pytest).

Runs the full lifecycle and prints outputs for:
- Normalization Strategy
- MongoDB Design
- Query Engine
- Metadata System
- CRUD Functionality
"""

from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Dict, List, Tuple

from fastapi.testclient import TestClient

import schema_registry_api as api
from buffer_queue import BufferQueue
from crud_executor import HybridCRUDExecutor
from crud_query_engine import CRUDQueryEngine
from schema_registry import SchemaRegistry


def _pretty(data: Any) -> str:
    return json.dumps(data, indent=2, ensure_ascii=False)


def _print_header(title: str) -> None:
    print("\n" + "=" * 90)
    print(title)
    print("=" * 90)


def _check(results: List[Tuple[str, bool, str]], label: str, condition: bool, detail: str) -> None:
    results.append((label, condition, detail))
    status = "PASS" if condition else "FAIL"
    print(f"[{status}] {label}: {detail}")


def _schema_payload() -> Dict[str, Any]:
    return {
        "entity": "user_activity",
        "schema": {
            "username": {"type": "string", "unique": True},
            "post_id": {"type": "integer"},
            "tags": {
                "type": "array",
                "items": {"type": "string"},
            },
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


def _record_payload() -> Dict[str, Any]:
    return {
        "username": "user1",
        "post_id": 42,
        "tags": ["python", "database", "api"],
        "comments": [
            {"text": "nice", "time": 100},
            {"text": "great", "time": 120},
        ],
        "profile": {"address": "MG Road", "city": "Ahmedabad"},
    }


def run_demo() -> int:
    checks: List[Tuple[str, bool, str]] = []
    tmp_dir = tempfile.mkdtemp(prefix="workflow_demo_")
    db_path = Path(tmp_dir) / "workflow_demo.db"

    try:
        registry = SchemaRegistry(db_path=str(db_path))
        api.registry = registry
        api.query_engine = CRUDQueryEngine(registry=registry)
        api.crud_executor = HybridCRUDExecutor(registry=registry)
        api.buffer_queue = BufferQueue(db_path=registry.db_path)

        client = TestClient(api.app)

        _print_header("1) Register schema and fetch metadata")
        register_res = client.post("/register_schema", json=_schema_payload())
        print(f"register status: {register_res.status_code}")
        register_body = register_res.json()
        print(_pretty({"schema_id": register_body.get("schema", {}).get("schema_id"), "entity": register_body.get("schema", {}).get("entity_name")}))

        schema_id = register_body["schema"]["schema_id"]
        schema_res = client.get(f"/schemas/{schema_id}")
        schema_body = schema_res.json()

        _check(checks, "Schema registration", register_res.status_code == 200, f"schema_id={schema_id}")
        _check(checks, "Schema retrieval", schema_res.status_code == 200, "Fetched schema metadata")

        _print_header("Criterion: Normalization Strategy")
        blueprint = schema_body["sql_blueprint"]
        table_names = [t["name"] for t in blueprint["tables"]]
        relationships = blueprint.get("relationships", [])
        print("SQL tables:", table_names)
        print("Relationships:", _pretty(relationships))
        print("Rules:", _pretty(blueprint.get("rules", {})))
        _check(checks, "Relational decomposition", {"user_activity", "comments", "profile"}.issubset(set(table_names)), "Root + child tables detected")
        _check(checks, "FK decomposition", any(r.get("from_table") == "comments" and r.get("to_table") == "user_activity" for r in relationships), "comments -> user_activity FK found")

        _print_header("Criterion: MongoDB Design")
        mongo_strategy = schema_body.get("mongo_strategy", {})
        print(_pretty(mongo_strategy))
        _check(checks, "Mongo strategy available", "documents" in mongo_strategy, "documents/rules generated")
        _check(checks, "Mongo rules structure", "rules" in mongo_strategy, "embed/reference rule buckets present")
        root_document = (mongo_strategy.get("documents") or [{}])[0]
        _check(
            checks,
            "Mongo embedded fields populated",
            bool(root_document.get("embedded_fields")),
            "At least one field is embedded in Mongo strategy",
        )
        _check(
            checks,
            "Mongo sources/reasons populated",
            bool(root_document.get("sources")) and bool(root_document.get("reasons")),
            "Mongo strategy records source paths and rationale",
        )
        _check(
            checks,
            "Mongo rule lists non-empty",
            bool(mongo_strategy.get("rules", {}).get("embed")) or bool(mongo_strategy.get("rules", {}).get("reference")),
            "Mongo embed/reference decisions captured",
        )

        _print_header("Criterion: Query Engine (including hybrid merge)")
        query_plan_res = client.post(
            f"/schemas/{schema_id}/query_plan",
            json={
                "operation": "read",
                "fields": ["username", "comments", "tags"],
                "filters": {"username": "user1"},
                "limit": 10,
            },
        )
        read_plan = query_plan_res.json().get("plan", {})
        print("Generated SQL:")
        print(read_plan.get("sql", {}).get("statement"))
        print("Generated Mongo plan:")
        print(_pretty(read_plan.get("mongo", [])))
        print("Merge plan:")
        print(_pretty(read_plan.get("merge", {})))
        _check(checks, "Query plan endpoint", query_plan_res.status_code == 200, "Read query plan generated")
        _check(checks, "SQL query synthesis", bool(read_plan.get("sql", {}).get("statement", "").startswith("SELECT")), "SELECT statement created")
        _check(checks, "Mongo query synthesis", len(read_plan.get("mongo", [])) > 0, "Mongo find-plan generated for Mongo-routed fields")
        _check(checks, "Hybrid merge strategy", read_plan.get("merge", {}).get("strategy") == "client_side_join", "SQL + Mongo merge path selected")

        _print_header("Query plan generation for ALL CRUD operations")
        create_plan_res = client.post(
            f"/schemas/{schema_id}/query_plan",
            json={
                "operation": "create",
                "filters": {"username": "user1"},
            },
        )
        update_plan_res = client.post(
            f"/schemas/{schema_id}/query_plan",
            json={
                "operation": "update",
                "filters": {"post_id": 42},
            },
        )
        delete_plan_res = client.post(
            f"/schemas/{schema_id}/query_plan",
            json={
                "operation": "delete",
                "filters": {"target": "comments", "criteria": {"post_id": 42, "comment_id": 1}},
            },
        )
        create_plan = create_plan_res.json().get("plan", {})
        update_plan = update_plan_res.json().get("plan", {})
        delete_plan = delete_plan_res.json().get("plan", {})
        print("CREATE/INSERT plan:")
        print(_pretty(create_plan))
        print("UPDATE plan:")
        print(_pretty(update_plan))
        print("DELETE plan:")
        print(_pretty(delete_plan))
        _check(checks, "Create plan endpoint", create_plan_res.status_code == 200 and create_plan.get("operation") == "insert", "Create maps to insert plan")
        _check(checks, "Update plan endpoint", update_plan_res.status_code == 200 and update_plan.get("operation") == "update", "Update plan generated")
        _check(checks, "Delete plan endpoint", delete_plan_res.status_code == 200 and delete_plan.get("operation") == "delete", "Delete plan generated")
        _check(checks, "Delete plan has SQL/Mongo targets", bool(delete_plan.get("sql")) and bool(delete_plan.get("mongo")), "Delete plan contains backend targets")

        _print_header("Criterion: CRUD Functionality")
        insert_res = client.post(
            f"/schemas/{schema_id}/crud",
            json={"operation": "insert", "payload": _record_payload(), "execute": False},
        )
        read_res = client.post(
            f"/schemas/{schema_id}/crud",
            json={
                "operation": "read",
                "fields": ["username", "comments"],
                "filters": {"username": "user1"},
                "limit": 5,
                "execute": False,
            },
        )
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
        delete_res = client.post(
            f"/schemas/{schema_id}/crud",
            json={
                "operation": "delete",
                "strategy": "sub-entity",
                "filters": {"target": "comments", "criteria": {"post_id": 42, "comment_id": 1}},
                "execute": False,
            },
        )

        insert_details = insert_res.json().get("result", {}).get("details", {})
        read_details = read_res.json().get("result", {}).get("details", {})
        update_details = update_res.json().get("result", {}).get("details", {})
        delete_details = delete_res.json().get("result", {}).get("details", {})

        print("Insert plan summary:")
        print(_pretty({
            "order": insert_details.get("sql", {}).get("order"),
            "rows_keys": list((insert_details.get("sql", {}).get("rows") or {}).keys()),
            "fk_keys": list((insert_details.get("sql", {}).get("foreign_keys") or {}).keys()),
        }))
        print("Read plan statement:", read_details.get("sql", {}).get("statement"))
        print("Update consistency mode:", update_details.get("plan", {}).get("consistency", {}).get("mode"))
        print("Delete target tables:", delete_details.get("sql", {}).get("tables"))

        _check(checks, "Insert dry-run", insert_res.status_code == 200 and insert_details.get("plan", {}).get("operation") == "insert", "Split SQL/Mongo insert plan available")
        _check(checks, "Read dry-run", read_res.status_code == 200 and str(read_details.get("sql", {}).get("statement", "")).startswith("SELECT"), "Read plan generated")
        _check(checks, "Update dry-run", update_res.status_code == 200 and update_details.get("plan", {}).get("consistency", {}).get("mode") == "delete_then_insert", "Simple update strategy validated")
        _check(checks, "Delete dry-run", delete_res.status_code == 200 and delete_details.get("sql", {}).get("tables") == ["comments"], "Sub-entity delete plan validated")

        _print_header("Criterion: Metadata System")
        schemas_res = client.get("/schemas")
        schemas_body = schemas_res.json()
        matched = [s for s in schemas_body.get("schemas", []) if s.get("schema_id") == schema_id]
        metadata_summary = matched[0] if matched else {}
        print(_pretty({
            "count": schemas_body.get("count"),
            "field_count": metadata_summary.get("field_count"),
            "analysis_summary": metadata_summary.get("analysis_summary"),
            "storage_strategy_summary": metadata_summary.get("storage_strategy_summary"),
        }))
        _check(checks, "Schema listing", schemas_res.status_code == 200 and bool(matched), "Schema tracked in registry list")
        _check(checks, "Routing metadata", metadata_summary.get("storage_strategy_summary", {}).get("mapped_fields", 0) >= 1, "Mapped fields tracked")

        _print_header("Ingestion + Buffer visibility")
        ingest_res = client.post(f"/ingest/{schema_id}", json={"payload": _record_payload(), "execute": False})
        ingest_body = ingest_res.json()
        print(_pretty({
            "message": ingest_body.get("message"),
            "buffered_fields": ingest_body.get("buffered_fields"),
            "crud_operation": ingest_body.get("crud", {}).get("operation"),
        }))
        _check(checks, "Ingestion endpoint", ingest_res.status_code == 200, "Ingestion flow returned plan")

        # Explicitly close client to release file handles before temp dir cleanup (Windows)
        client.close()

    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)

    _print_header("FINAL SUMMARY")
    passed = sum(1 for _, ok, _ in checks if ok)
    total = len(checks)
    for idx, (label, ok, detail) in enumerate(checks, start=1):
        status = "PASS" if ok else "FAIL"
        print(f"{idx:02d}. [{status}] {label} -> {detail}")

    print(f"\nOverall: {passed}/{total} checks passed")
    return 0 if passed == total else 1


if __name__ == "__main__":
    raise SystemExit(run_demo())
