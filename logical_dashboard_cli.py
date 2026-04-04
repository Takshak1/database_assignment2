"""Logical dashboard CLI for hybrid database system.

Displays logical sessions, entities, instances, and query results without exposing
backend-specific details (tables/collections/indices).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from crud_executor import HybridCRUDExecutor
from schema_registry import SchemaRegistry


@dataclass
class QueryRecord:
    query_input: Dict[str, Any]
    status: str
    logical_result: List[Dict[str, Any]]
    summary: Dict[str, Any]
    timestamp: str


@dataclass
class DashboardSession:
    session_id: str
    started_at: str
    registry_db: str
    metadata_file: str
    queries: List[QueryRecord] = field(default_factory=list)


def _now_iso() -> str:
    return datetime.utcnow().isoformat()


def _safe_json(data: Any) -> str:
    return json.dumps(data, indent=2, default=str, ensure_ascii=False)


def _parse_filters(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        if isinstance(data, dict):
            return data
    except json.JSONDecodeError:
        pass
    raise ValueError("Filters must be valid JSON object (e.g., {\"username\": \"alice\"}).")


def _default_execute() -> bool:
    return os.getenv("DASHBOARD_EXECUTE", "0").strip().lower() in {"1", "true", "yes", "on"}


def _sanitize_error() -> str:
    return "Execution failed due to backend unavailability or configuration. Run in dry-run mode to verify logical requests."


def _plan_summary(plan: Dict[str, Any]) -> Dict[str, Any]:
    field_locations = plan.get("field_locations") or []
    resolved = [loc for loc in field_locations if loc.get("status") == "resolved"]
    missing = [loc for loc in field_locations if loc.get("status") != "resolved"]
    sql_fields = [loc.get("requested") for loc in resolved if loc.get("storage") == "sql"]
    mongo_fields = [loc.get("requested") for loc in resolved if loc.get("storage") == "mongo"]
    sql_required = bool(plan.get("sql")) or bool(sql_fields)
    mongo_required = bool(plan.get("mongo")) or bool(mongo_fields)
    merge_key = None
    if isinstance(plan.get("merge"), dict):
        merge_key = plan.get("merge", {}).get("merge_key")
    return {
        "requested_fields": [loc.get("requested") for loc in field_locations],
        "resolved_fields": len(resolved),
        "missing_fields": len(missing),
        "uses_sql": sql_required,
        "uses_mongo": mongo_required,
        "merge_key_present": bool(merge_key),
        "merge_required": bool(merge_key) and bool(sql_fields) and bool(mongo_fields),
        "backend_flow": {
            "sql": {
                "enabled": sql_required,
                "logical_fields": sql_fields,
            },
            "mongo": {
                "enabled": mongo_required,
                "logical_fields": mongo_fields,
            },
        },
    }


def _summarize_entities(registry: SchemaRegistry) -> List[Dict[str, Any]]:
    return registry.list_schemas()


def _entity_fields(schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    fields = schema.get("fields", [])
    results: List[Dict[str, Any]] = []
    for field in fields:
        results.append(
            {
                "field": field.get("field_path") or field.get("field_name"),
                "type": field.get("data_type"),
                "nullable": field.get("is_nullable"),
                "primary_key": field.get("is_primary_key"),
                "unique": field.get("is_unique"),
            }
        )
    return results


def _run_logical_query(
    executor: HybridCRUDExecutor,
    session: DashboardSession,
    schema_id: int,
    fields: List[str],
    filters: Dict[str, Any],
    limit: Optional[int],
    execute: bool,
) -> QueryRecord:
    query_input = {
        "operation": "read",
        "fields": fields,
        "filters": filters,
        "limit": limit,
        "execute": execute,
    }
    logical_result: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {}
    status = "ok"
    try:
        result = executor.execute(
            schema_id,
            operation="read",
            fields=fields,
            filters=filters,
            limit=limit,
            execute=execute,
        )
        details = result.details
        plan_summary = _plan_summary(details)
        logical_result = details.get("results") or []
        summary = {
            "items": len(logical_result),
            "note": details.get("note") if not execute else None,
            "plan_summary": plan_summary,
        }
        if execute and not logical_result:
            summary["note"] = "No logical results returned."
    except Exception:
        status = "failed"
        summary = {"error": _sanitize_error()}

    record = QueryRecord(
        query_input=query_input,
        status=status,
        logical_result=logical_result,
        summary=summary,
        timestamp=_now_iso(),
    )
    session.queries.append(record)
    return record


def _print_session(session: DashboardSession, registry: SchemaRegistry) -> None:
    entities = _summarize_entities(registry)
    print("\nActive Session")
    print("-" * 60)
    print(_safe_json(
        {
            "session_id": session.session_id,
            "started_at": session.started_at,
            "registry_db": session.registry_db,
            "metadata_file": session.metadata_file,
            "schemas_loaded": len(entities),
            "queries_run": len(session.queries),
        }
    ))


def _print_entities(registry: SchemaRegistry) -> None:
    entities = _summarize_entities(registry)
    print("\nLogical Entities")
    print("-" * 60)
    if not entities:
        print("No schemas registered yet.")
        return
    sanitized = [
        {
            "schema_id": e.get("schema_id"),
            "entity_name": e.get("entity_name"),
            "field_count": e.get("field_count"),
            "created_at": e.get("created_at"),
        }
        for e in entities
    ]
    print(_safe_json(sanitized))


def _print_entity_details(
    registry: SchemaRegistry,
    executor: HybridCRUDExecutor,
    session: DashboardSession,
    schema_id: int,
) -> None:
    schema = registry.get_schema(schema_id)
    fields = _entity_fields(schema)
    print(f"\nEntity: {schema.get('entity_name')} (schema_id={schema_id})")
    print("-" * 60)
    print("Fields:")
    print(_safe_json(fields))

    preview_fields = [f["field"] for f in fields if f.get("field")][:6]
    if preview_fields:
        print("\nSample Instances (logical preview)")
        print("-" * 60)
        record = _run_logical_query(
            executor,
            session,
            schema_id,
            fields=preview_fields,
            filters={},
            limit=3,
            execute=_default_execute(),
        )
        _print_query_record(record)


def _print_query_record(record: QueryRecord) -> None:
    print("Query Input:")
    print(_safe_json(record.query_input))
    print("Execution Status:", record.status)
    if record.logical_result:
        print("Logical Result:")
        print(_safe_json(record.logical_result))
    else:
        print("Logical Result: []")
    if record.summary:
        print("Summary:")
        print(_safe_json(record.summary))


def _print_query_history(session: DashboardSession) -> None:
    print("\nQuery History")
    print("-" * 60)
    if not session.queries:
        print("No queries executed yet.")
        return
    for idx, record in enumerate(session.queries, start=1):
        print(f"\n#{idx} @ {record.timestamp}")
        _print_query_record(record)


def _interactive_dashboard(
    registry: SchemaRegistry,
    executor: HybridCRUDExecutor,
    session: DashboardSession,
) -> None:
    while True:
        print("\nLogical Dashboard")
        print("1) Active session info")
        print("2) List logical entities")
        print("3) Entity details + sample instances")
        print("4) Run logical query")
        print("5) Query history")
        print("0) Exit")
        choice = input("Select an option: ").strip()

        if choice == "1":
            _print_session(session, registry)
        elif choice == "2":
            _print_entities(registry)
        elif choice == "3":
            schema_id = input("Enter schema_id: ").strip()
            if schema_id.isdigit():
                _print_entity_details(registry, executor, session, int(schema_id))
            else:
                print("schema_id must be a number.")
        elif choice == "4":
            schema_id = input("Enter schema_id: ").strip()
            if not schema_id.isdigit():
                print("schema_id must be a number.")
                continue
            field_raw = input("Fields (comma-separated): ").strip()
            fields = [f.strip() for f in field_raw.split(",") if f.strip()]
            filter_raw = input("Filters JSON (optional): ").strip() or None
            limit_raw = input("Limit (optional): ").strip()
            limit = int(limit_raw) if limit_raw.isdigit() else None
            try:
                filters = _parse_filters(filter_raw)
            except ValueError as exc:
                print(str(exc))
                continue
            execute = input("Execute against live backends? (y/N): ").strip().lower() == "y"
            record = _run_logical_query(
                executor,
                session,
                int(schema_id),
                fields=fields,
                filters=filters,
                limit=limit,
                execute=execute,
            )
            _print_query_record(record)
        elif choice == "5":
            _print_query_history(session)
        elif choice == "0":
            break
        else:
            print("Unknown option.")


def main() -> int:
    parser = argparse.ArgumentParser(description="Logical Dashboard CLI")
    parser.add_argument("--registry", default=os.getenv("SCHEMA_REGISTRY_DB", "schema_registry.db"))
    parser.add_argument("--metadata", default=os.getenv("METADATA_FILE", "metadata.json"))
    parser.add_argument("--list-entities", action="store_true")
    parser.add_argument("--entity", type=int, help="Show details for a schema_id")
    parser.add_argument("--query", type=int, help="Run logical query for a schema_id")
    parser.add_argument("--fields", default="", help="Comma-separated fields for --query")
    parser.add_argument("--filters", default=None, help="JSON filters for --query")
    parser.add_argument("--limit", type=int, default=None, help="Result limit for --query")
    parser.add_argument("--execute", action="store_true", help="Execute against live backends")

    args = parser.parse_args()

    registry = SchemaRegistry(db_path=args.registry)
    executor = HybridCRUDExecutor(registry=registry, metadata_file=args.metadata)
    session = DashboardSession(
        session_id=str(uuid.uuid4()),
        started_at=_now_iso(),
        registry_db=args.registry,
        metadata_file=args.metadata,
    )

    if args.list_entities:
        _print_entities(registry)
        return 0
    if args.entity is not None:
        _print_entity_details(registry, executor, session, args.entity)
        return 0
    if args.query is not None:
        try:
            filters = _parse_filters(args.filters)
        except ValueError as exc:
            print(str(exc))
            return 1
        fields = [f.strip() for f in args.fields.split(",") if f.strip()]
        record = _run_logical_query(
            executor,
            session,
            args.query,
            fields=fields,
            filters=filters,
            limit=args.limit,
            execute=args.execute or _default_execute(),
        )
        _print_query_record(record)
        return 0

    _interactive_dashboard(registry, executor, session)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
