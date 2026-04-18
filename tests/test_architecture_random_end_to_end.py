from __future__ import annotations

import json
import os
import random
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

import comparative_evaluation
import performance_benchmark
import schema_registry_api as api_module
from buffer_queue import BufferQueue
from crud_executor import HybridCRUDExecutor, MongoClient as crud_mongo_client, mysql_connector as crud_mysql_connector
from crud_query_engine import CRUDQueryEngine
from schema_registry import SchemaRegistry


def _random_record(rng: random.Random, index: int) -> Dict[str, Any]:
    city = rng.choice(["Ahmedabad", "Pune", "Delhi", "Bengaluru", "Hyderabad"])
    uni_name = f"Random University {index}"
    return {
        "id": f"UNI-{index:04d}",
        "name": uni_name,
        "university_name": uni_name,
        "city": city,
        "established": rng.randint(1950, 2022),
        "contact": {
            "email": f"contact{index}@example.edu",
            "phone": f"+91-98{rng.randint(10000000, 99999999)}",
            "city": city,
            "country": "India",
        },
        "students": [
            {
                "student_id": f"STU-{index:04d}-{student_idx:02d}",
                "name": f"Student {index}-{student_idx}",
                "email": f"stu{index}_{student_idx}@example.edu",
                "cgpa": round(rng.uniform(6.5, 9.9), 2),
                "fees_status": rng.choice(["paid", "pending"]),
            }
            for student_idx in range(3)
        ],
        "faculty_members": [
            {
                "emp_id": f"EMP-{index:04d}-{emp_idx:02d}",
                "name": f"Faculty {index}-{emp_idx}",
                "email": f"faculty{index}_{emp_idx}@example.edu",
                "designation": rng.choice(["Professor", "Associate Professor", "Assistant Professor"]),
                "specializations": [
                    rng.choice(["AI", "Data Science", "Networks", "Databases", "Systems"]),
                    rng.choice(["ML", "Cloud", "Security", "Algorithms", "HCI"]),
                ],
            }
            for emp_idx in range(2)
        ],
        "placements": {
            "avg_package_usd": rng.randint(6000, 25000),
            "top_recruiters": [
                rng.choice(["Google", "Microsoft", "Amazon", "TCS", "Infosys"]),
                rng.choice(["IBM", "Accenture", "Oracle", "NVIDIA", "Adobe"]),
            ],
        },
    }


def _random_dataset(seed: int, size: int) -> List[Dict[str, Any]]:
    rng = random.Random(seed)
    return [_random_record(rng, idx + 1) for idx in range(size)]


def _assert_live_backend_availability(executor: HybridCRUDExecutor) -> None:
    if crud_mysql_connector is None:
        pytest.skip("mysql-connector-python is not installed; skipping live E2E test")
    if crud_mongo_client is None:
        pytest.skip("pymongo is not installed; skipping live E2E test")

    try:
        conn = crud_mysql_connector.connect(**executor.mysql_config)
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.fetchone()
        cursor.close()
        conn.close()
    except Exception as exc:
        pytest.skip(f"MySQL is not reachable for live E2E test: {exc}")

    try:
        uri = f"mongodb://{executor.mongo_config['host']}:{executor.mongo_config['port']}/"
        client = crud_mongo_client(uri, serverSelectionTimeoutMS=4000)
        client.admin.command("ping")
        client.close()
    except Exception as exc:
        pytest.skip(f"MongoDB is not reachable for live E2E test: {exc}")


def test_random_json_database_end_to_end_architecture(tmp_path: Path, monkeypatch) -> None:
    # Build an isolated runtime for the API module so this test does not touch project-level DB files.
    registry_db = tmp_path / "random_registry.db"
    metadata_file = tmp_path / "random_metadata.json"
    dataset_file = tmp_path / "random_university_data.json"

    registry = SchemaRegistry(db_path=str(registry_db))
    query_engine = CRUDQueryEngine(registry=registry, metadata_file=str(metadata_file))
    executor = HybridCRUDExecutor(registry=registry, metadata_file=str(metadata_file))
    queue = BufferQueue(db_path=str(registry_db))

    monkeypatch.setattr(api_module, "registry", registry)
    monkeypatch.setattr(api_module, "query_engine", query_engine)
    monkeypatch.setattr(api_module, "crud_executor", executor)
    monkeypatch.setattr(api_module, "buffer_queue", queue)

    records = _random_dataset(seed=42, size=12)
    dataset_file.write_text(json.dumps(records, indent=2), encoding="utf-8")

    client = TestClient(api_module.app)

    register_response = client.post(
        "/register_schema",
        json={"entity": "random_university", "schema": records[0]},
    )
    assert register_response.status_code == 200
    register_body = register_response.json()
    schema_id = int(register_body["schema"]["schema_id"])

    listed = client.get("/schemas", params={"entity": "random_university"})
    assert listed.status_code == 200
    assert listed.json()["count"] >= 1

    details = client.get(f"/schemas/{schema_id}")
    assert details.status_code == 200
    detail_payload = details.json()
    assert detail_payload["entity_name"] == "random_university"
    assert detail_payload.get("storage_strategy") is not None

    for record in records:
        ingest_response = client.post(
            f"/ingest/{schema_id}",
            json={"payload": record, "execute": False, "strategy": "simple"},
        )
        assert ingest_response.status_code == 200
        ingest_payload = ingest_response.json()
        assert ingest_payload["message"] == "Ingestion processed"
        assert "crud" in ingest_payload
        assert ingest_payload["crud"]["operation"] == "insert"

    plan_response = client.post(
        f"/schemas/{schema_id}/query_plan",
        json={
            "operation": "read",
            "fields": [
                "id",
                "name",
                "university_name",
                "city",
                "faculty_members.specializations",
                "placements.top_recruiters",
            ],
            "filters": {"id": records[0]["id"]},
            "limit": 5,
        },
    )
    assert plan_response.status_code == 200
    assert "plan" in plan_response.json()

    crud_insert = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "insert",
            "payload": records[0],
            "strategy": "simple",
            "execute": False,
        },
    )
    assert crud_insert.status_code == 200
    assert crud_insert.json()["result"]["operation"] == "insert"

    crud_read = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "read",
            "fields": ["id", "name", "city"],
            "filters": {"id": records[0]["id"]},
            "limit": 5,
            "execute": False,
        },
    )
    assert crud_read.status_code == 200
    assert crud_read.json()["result"]["operation"] == "read"

    crud_update = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "update",
            "payload": {"city": "Chennai"},
            "filters": {"id": records[0]["id"]},
            "strategy": "advanced",
            "execute": False,
        },
    )
    assert crud_update.status_code == 200
    assert crud_update.json()["result"]["operation"] == "update"

    crud_delete = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "delete",
            "filters": {"id": records[0]["id"]},
            "strategy": "entity",
            "execute": False,
        },
    )
    assert crud_delete.status_code == 200
    assert crud_delete.json()["result"]["operation"] == "delete"

    auto_crud = client.post(
        "/crud_auto",
        json={
            "entity": "random_university_auto",
            "operation": "insert",
            "payload": records[1],
            "execute": False,
            "strategy": "simple",
        },
    )
    assert auto_crud.status_code == 200
    auto_payload = auto_crud.json()
    assert auto_payload["schema_id"] > 0
    assert auto_payload["auto_registration"]["status"] in {"registered", "existing"}

    metadata_data = json.loads(metadata_file.read_text(encoding="utf-8"))
    assert metadata_data
    first_meta = next(iter(metadata_data.values()))
    assert "placement_decision" in first_meta

    benchmark = performance_benchmark.run_benchmark(
        runs=3,
        execute=False,
        dataset_path=dataset_file,
        metadata_file=metadata_file,
        registry_db=registry_db,
    )
    assert benchmark["metadata"]["mode"] == "dry_run"
    assert benchmark["ingestion"]["runs"] == 3
    assert benchmark["logical_query"]["throughput_ops_per_sec"] >= 0.0

    comparison = comparative_evaluation.run_comparison(
        iterations=3,
        dataset=dataset_file,
        execute=False,
        registry_db=registry_db,
        metadata_file=metadata_file,
    )
    assert comparison["meta"]["mode"] == "dry_run"
    assert len(comparison["summary_table"]) == 3
    assert set(comparison["overhead"].keys()) == {"read", "nested_read", "update"}


@pytest.mark.skipif(
    os.getenv("RUN_LIVE_E2E", "0").strip().lower() not in {"1", "true", "yes", "on"},
    reason="Set RUN_LIVE_E2E=1 to enable live execute=True architecture verification",
)
def test_random_json_database_end_to_end_architecture_live_execute(tmp_path: Path, monkeypatch) -> None:
    registry_db = tmp_path / "random_registry_live.db"
    metadata_file = tmp_path / "random_metadata_live.json"
    dataset_file = tmp_path / "random_university_data_live.json"

    registry = SchemaRegistry(db_path=str(registry_db))
    query_engine = CRUDQueryEngine(registry=registry, metadata_file=str(metadata_file))
    executor = HybridCRUDExecutor(registry=registry, metadata_file=str(metadata_file))
    queue = BufferQueue(db_path=str(registry_db))

    _assert_live_backend_availability(executor)

    monkeypatch.setattr(api_module, "registry", registry)
    monkeypatch.setattr(api_module, "query_engine", query_engine)
    monkeypatch.setattr(api_module, "crud_executor", executor)
    monkeypatch.setattr(api_module, "buffer_queue", queue)

    records = _random_dataset(seed=2026, size=6)
    dataset_file.write_text(json.dumps(records, indent=2), encoding="utf-8")

    client = TestClient(api_module.app)
    register_response = client.post(
        "/register_schema",
        json={"entity": "random_university_live", "schema": records[0]},
    )
    assert register_response.status_code == 200
    schema_id = int(register_response.json()["schema"]["schema_id"])

    for record in records:
        ingest_response = client.post(
            f"/ingest/{schema_id}",
            json={"payload": record, "execute": True, "strategy": "simple"},
        )
        assert ingest_response.status_code == 200
        assert ingest_response.json()["crud"]["operation"] == "insert"

    live_read = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "read",
            "fields": ["id", "name", "university_name", "city"],
            "filters": {},
            "limit": 10,
            "execute": True,
        },
    )
    assert live_read.status_code == 200
    assert live_read.json()["result"]["operation"] == "read"

    live_update = client.post(
        f"/schemas/{schema_id}/crud",
        json={
            "operation": "update",
            "payload": {"city": "Mumbai"},
            "filters": {"id": records[0]["id"]},
            "strategy": "advanced",
            "execute": True,
        },
    )
    assert live_update.status_code == 200
    assert live_update.json()["result"]["operation"] == "update"

    benchmark = performance_benchmark.run_benchmark(
        runs=2,
        execute=True,
        dataset_path=dataset_file,
        metadata_file=metadata_file,
        registry_db=registry_db,
    )
    assert benchmark["metadata"]["mode"] == "execute"
    assert benchmark["ingestion"]["success"] == benchmark["ingestion"]["runs"]
    assert benchmark["logical_query"]["success"] == benchmark["logical_query"]["runs"]

    comparison = comparative_evaluation.run_comparison(
        iterations=2,
        dataset=dataset_file,
        execute=True,
        registry_db=registry_db,
        metadata_file=metadata_file,
    )
    assert comparison["meta"]["mode"] == "execute"
    assert comparison["direct"]["sql_read_avg_ms"] is not None
    assert comparison["direct"]["mongo_read_avg_ms"] is not None
    assert comparison["direct"]["mixed_update_avg_ms"] is not None
