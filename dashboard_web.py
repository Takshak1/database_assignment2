"""Logical dashboard web UI (FastAPI).

Serves a local-hosted dashboard that presents logical entities, instances,
and query results without exposing backend-specific storage details.
"""

from __future__ import annotations

import json
import os
import re
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, Form
from fastapi.responses import HTMLResponse

from crud_executor import HybridCRUDExecutor
from schema_registry import SchemaRegistry

try:  # pragma: no cover - optional dependency
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover
    mysql_connector = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore

app = FastAPI(title="Logical Dashboard")


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


def _safe(value: Any) -> str:
    return str(value).replace("<", "&lt;").replace(">", "&gt;")


def _json_pretty(value: Any) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


def _parse_filters(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    raw = raw.strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        raise ValueError("Filters must be valid JSON object.")
    if not isinstance(data, dict):
        raise ValueError("Filters must be a JSON object.")
    return data


def _parse_payload(raw: Optional[str]) -> Dict[str, Any]:
    if not raw:
        return {}
    raw = raw.strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except Exception:
        raise ValueError("Payload must be valid JSON object.")
    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object.")
    return data


def _default_execute() -> bool:
    return os.getenv("DASHBOARD_EXECUTE", "0").strip().lower() in {"1", "true", "yes", "on"}


def _default_preview_execute() -> bool:
    # Entity preview should show actual sample rows by default.
    return os.getenv("DASHBOARD_PREVIEW_EXECUTE", "1").strip().lower() in {"1", "true", "yes", "on"}


def _sanitize_error() -> str:
    return "Execution failed due to backend unavailability or configuration. Try dry-run mode to validate logical queries."


def _test_connections() -> Dict[str, Any]:
    results: Dict[str, Any] = {"mysql": {}, "mongo": {}}

    if mysql_connector is None:
        results["mysql"] = {"ok": False, "error": "mysql-connector-python not installed"}
    else:
        try:
            conn = mysql_connector.connect(**executor.mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()
            cursor.close()
            conn.close()
            results["mysql"] = {"ok": True}
        except Exception as exc:  # pragma: no cover
            results["mysql"] = {"ok": False, "error": str(exc)}

    if MongoClient is None:
        results["mongo"] = {"ok": False, "error": "pymongo not installed"}
    else:
        try:
            client = MongoClient(
                host=os.getenv("MONGO_HOST", "localhost"),
                port=int(os.getenv("MONGO_PORT", "27017")),
                serverSelectionTimeoutMS=4000,
            )
            client.admin.command("ping")
            client.close()
            results["mongo"] = {"ok": True}
        except Exception as exc:  # pragma: no cover
            results["mongo"] = {"ok": False, "error": str(exc)}

    return results


def _sql_table_counts() -> Dict[str, Any]:
    if mysql_connector is None:
        return {"ok": False, "error": "mysql-connector-python not installed"}
    try:
        conn = mysql_connector.connect(**executor.mysql_config)
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [row[0] for row in cursor.fetchall()]
        counts: Dict[str, int] = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row = cursor.fetchone()
            counts[table] = int(row[0]) if row else 0
        cursor.close()
        conn.close()
        return {"ok": True, "counts": counts}
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}


def _sql_fk_violations() -> Dict[str, Any]:
    if mysql_connector is None:
        return {"ok": False, "error": "mysql-connector-python not installed"}
    try:
        conn = mysql_connector.connect(**executor.mysql_config)
        cursor = conn.cursor()
        violations: List[Dict[str, Any]] = []
        skipped_relations: List[Dict[str, Any]] = []
        cursor.execute("SHOW TABLES")
        existing_tables = {
            _normalize_table_lookup_name(row[0])
            for row in cursor.fetchall()
            if row and row[0] is not None
        }
        for schema in registry.list_schemas():
            detail = registry.get_schema(schema["schema_id"])
            blueprint = detail.get("sql_blueprint") or detail.get("analysis", {}).get("sql_blueprint")
            if not blueprint:
                continue
            for relation in blueprint.get("relationships", []):
                child = relation.get("from_table")
                parent = relation.get("to_table")
                child_col = relation.get("from_column")
                parent_col = relation.get("to_column")
                if not all([child, parent, child_col, parent_col]):
                    continue
                if not _is_sql_table_available(existing_tables, child) or not _is_sql_table_available(existing_tables, parent):
                    skipped_relations.append({
                        "child_table": child,
                        "parent_table": parent,
                        "child_column": child_col,
                        "parent_column": parent_col,
                        "reason": "skipped_missing_table",
                    })
                    continue
                statement = _build_fk_violation_statement(child, parent, child_col, parent_col)
                cursor.execute(statement)
                row = cursor.fetchone()
                count = int(row[0]) if row else 0
                violations.append({
                    "child_table": child,
                    "parent_table": parent,
                    "child_column": child_col,
                    "parent_column": parent_col,
                    "missing_parent": count,
                })
        cursor.close()
        conn.close()
        total_missing = sum(item["missing_parent"] for item in violations)
        return {
            "ok": True,
            "violations": violations,
            "total_missing": total_missing,
            "skipped_relations": skipped_relations,
        }
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}


def _normalize_table_lookup_name(name: Any) -> str:
    value = str(name or "").strip().strip("`")
    if "." in value:
        value = value.split(".")[-1]
    return value.lower()


def _is_sql_table_available(existing_tables: set[str], table_name: Any) -> bool:
    if not existing_tables:
        return False
    normalized = _normalize_table_lookup_name(table_name)
    return bool(normalized) and normalized in existing_tables


def _quote_mysql_identifier(name: Any) -> str:
    value = str(name or "").strip()
    if not value:
        return "``"
    return f"`{value.replace('`', '``')}`"


def _build_fk_violation_statement(child: Any, parent: Any, child_col: Any, parent_col: Any) -> str:
    child_table = _quote_mysql_identifier(child)
    parent_table = _quote_mysql_identifier(parent)
    child_column = _quote_mysql_identifier(child_col)
    parent_column = _quote_mysql_identifier(parent_col)
    child_alias = "child_tbl"
    parent_alias = "parent_tbl"
    return (
        f"SELECT COUNT(*) FROM {child_table} AS {child_alias} "
        f"LEFT JOIN {parent_table} AS {parent_alias} "
        f"ON {child_alias}.{child_column} = {parent_alias}.{parent_column} "
        f"WHERE {child_alias}.{child_column} IS NOT NULL AND {parent_alias}.{parent_column} IS NULL"
    )


def _mongo_collection_counts() -> Dict[str, Any]:
    if MongoClient is None:
        return {"ok": False, "error": "pymongo not installed"}
    try:
        client = MongoClient(
            host=os.getenv("MONGO_HOST", "localhost"),
            port=int(os.getenv("MONGO_PORT", "27017")),
            serverSelectionTimeoutMS=4000,
        )
        db = client[os.getenv("MONGO_DATABASE", "streaming_db")]
        counts = {name: db[name].count_documents({}) for name in db.list_collection_names()}
        client.close()
        return {"ok": True, "counts": counts}
    except Exception as exc:  # pragma: no cover
        return {"ok": False, "error": str(exc)}


def _acid_report() -> Dict[str, Any]:
    sql_counts = _sql_table_counts()
    mongo_counts = _mongo_collection_counts()
    fk_report = _sql_fk_violations()

    isolation_info: Dict[str, Any] = {"ok": False, "error": "not_checked"}
    if mysql_connector is not None:
        try:
            conn = mysql_connector.connect(**executor.mysql_config)
            cursor = conn.cursor()
            cursor.execute("SELECT @@transaction_isolation")
            row = cursor.fetchone()
            cursor.close()
            conn.close()
            isolation_info = {"ok": True, "level": row[0] if row else "unknown"}
        except Exception as exc:  # pragma: no cover
            isolation_info = {"ok": False, "error": str(exc)}

    atomicity_ok = os.getenv("TRANSACTION_COORDINATION", "1").strip().lower() in {"1", "true", "yes", "on"}
    consistency_ok = fk_report.get("ok") and fk_report.get("total_missing", 0) == 0

    return {
        "atomicity": {
            "enabled": atomicity_ok,
            "note": "SQL transaction + Mongo session/compensating rollback",
        },
        "consistency": {
            "fk_violations": fk_report,
            "status": "pass" if consistency_ok else "fail",
        },
        "isolation": isolation_info,
        "durability": {
            "sql_tables": sql_counts,
            "mongo_collections": mongo_counts,
        },
    }


def _summarize_write(details: Dict[str, Any]) -> Dict[str, Any]:
    plan = details.get("plan") if isinstance(details, dict) else None
    summary: Dict[str, Any] = {}

    if isinstance(plan, dict):
        summary["plan_summary"] = _plan_summary(plan)

    sql = details.get("sql") if isinstance(details, dict) else None
    mongo = details.get("mongo") if isinstance(details, dict) else None

    if isinstance(sql, dict):
        rows_inserted = sql.get("rows_inserted")
        if rows_inserted is not None:
            summary["sql_rows_inserted"] = rows_inserted

    if isinstance(mongo, dict):
        docs_inserted = mongo.get("documents_inserted")
        if docs_inserted is not None:
            summary["mongo_documents_inserted"] = docs_inserted

    if isinstance(details, dict) and details.get("strategy"):
        summary["strategy"] = details.get("strategy")

    return summary


def _plan_summary(plan: Dict[str, Any]) -> Dict[str, Any]:
    field_locations = plan.get("field_locations") or []
    resolved = [loc for loc in field_locations if _is_field_status_resolved(loc.get("status"))]
    missing = [loc for loc in field_locations if loc.get("status") == "missing"]
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


def _extract_plan_payload(details: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(details, dict):
        return None
    if isinstance(details.get("field_locations"), list):
        return details
    nested = details.get("plan")
    if isinstance(nested, dict):
        return nested
    return None


def _logical_plan_view(plan: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not plan:
        return {
            "requested_fields": [],
            "sql_resolved_fields": [],
            "mongo_resolved_fields": [],
            "buffer_resolved_fields": [],
            "missing_fields": [],
            "merge_key_used": None,
        }

    field_locations = plan.get("field_locations") or []
    requested = [loc.get("requested") for loc in field_locations if loc.get("requested")]
    sql_fields = [loc.get("requested") for loc in field_locations if loc.get("storage") == "sql" and loc.get("requested")]
    mongo_fields = [loc.get("requested") for loc in field_locations if loc.get("storage") == "mongo" and loc.get("requested")]
    buffer_fields = [loc.get("requested") for loc in field_locations if loc.get("storage") == "buffer" and loc.get("requested")]
    missing_fields = [
        loc.get("requested")
        for loc in field_locations
        if loc.get("status") == "missing" and loc.get("requested")
    ]

    merge_key = None
    merge_plan = plan.get("merge")
    if isinstance(merge_plan, dict):
        merge_key = merge_plan.get("merge_key")

    return {
        "requested_fields": requested,
        "sql_resolved_fields": sql_fields,
        "mongo_resolved_fields": mongo_fields,
        "buffer_resolved_fields": buffer_fields,
        "missing_fields": missing_fields,
        "merge_key_used": merge_key,
    }


def _explainability_badges(plan: Optional[Dict[str, Any]]) -> List[Dict[str, str]]:
    if not plan:
        return []

    note_map = {
        "exact_match": "Exact field mapping",
        "partial_match": "Partial field-path match",
        "table_scope": "Mapped by table scope",
        "metadata_hint": "Inferred from metadata hint",
        "field_not_found": "No mapping found",
    }

    badges: List[Dict[str, str]] = []
    for loc in plan.get("field_locations") or []:
        field_name = str(loc.get("requested") or "unknown")
        storage = str(loc.get("storage") or "unknown").lower()
        note = str(loc.get("notes") or "")
        reason = note_map.get(note, note or "routing rule")
        badges.append(
            {
                "field": field_name,
                "storage": storage,
                "reason": reason,
                "status": str(loc.get("status") or "unknown"),
            }
        )
    return badges


def _backend_operations(details: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(details, dict):
        return {}

    ops: Dict[str, Any] = {}
    for key in ("sql", "mongo", "merge"):
        if key in details:
            ops[key] = details.get(key)

    plan = details.get("plan")
    if isinstance(plan, dict):
        ops.setdefault("plan_sql", plan.get("sql"))
        ops.setdefault("plan_mongo", plan.get("mongo"))
        ops.setdefault("plan_merge", plan.get("merge"))

    return ops


def _is_field_status_resolved(status: Any) -> bool:
    return str(status or "").strip().lower() in {"resolved", "hint"}


def _describe_sql_zero_match_reason(details: Dict[str, Any]) -> Optional[str]:
    if mysql_connector is None or not isinstance(details, dict):
        return None

    result_summary = details.get("result_summary") if isinstance(details.get("result_summary"), dict) else {}
    sql_rows = int(result_summary.get("sql_rows", 0) or 0)
    if sql_rows != 0:
        return None

    sql_plan = details.get("sql") if isinstance(details.get("sql"), dict) else None
    if not sql_plan:
        return None

    where_clause = str(sql_plan.get("where") or "")
    parameters = sql_plan.get("parameters") or []
    if not where_clause or not parameters:
        return None

    match = re.search(r"([A-Za-z_][A-Za-z0-9_]*)\.([A-Za-z_][A-Za-z0-9_]*)\s*=\s*%s", where_clause)
    if not match:
        return None

    table_name, column_name = match.group(1), match.group(2)
    filter_value = parameters[0]
    if not isinstance(filter_value, str) or not any(ch.isalpha() for ch in filter_value):
        return None

    conn = None
    cursor = None
    try:
        conn = mysql_connector.connect(**executor.mysql_config)
        cursor = conn.cursor()
        cursor.execute(f"SHOW COLUMNS FROM {table_name} LIKE %s", (column_name,))
        column_info = cursor.fetchone()
        if not column_info or len(column_info) < 2:
            return None

        column_type = str(column_info[1]).lower()
        numeric_markers = ("int", "decimal", "double", "float", "numeric")
        if not any(marker in column_type for marker in numeric_markers):
            return None

        cursor.execute(f"SELECT {column_name} FROM {table_name} ORDER BY {column_name} LIMIT 5")
        sample_values = [str(row[0]) for row in cursor.fetchall() if row and row[0] is not None]
        sample_note = f" Sample {column_name} values: {', '.join(sample_values)}." if sample_values else ""
        return (
            f"No SQL rows matched: filter '{column_name}={filter_value}' appears to be an external ID, "
            f"but `{table_name}.{column_name}` stores numeric keys ({column_type})."
            f"{sample_note}"
        )
    except Exception:
        return None
    finally:
        if cursor is not None:
            cursor.close()
        if conn is not None:
            conn.close()


def _build_empty_read_reason(details: Dict[str, Any]) -> str:
    if not isinstance(details, dict):
        return "No logical results returned."

    plan_payload = _extract_plan_payload(details) or {}
    logical_plan = _logical_plan_view(plan_payload)
    missing_fields = logical_plan.get("missing_fields") or []
    if missing_fields:
        return (
            "Requested fields could not be resolved in metadata: "
            + ", ".join(str(field) for field in missing_fields)
            + "."
        )

    result_summary = details.get("result_summary") if isinstance(details.get("result_summary"), dict) else {}
    sql_rows = int(result_summary.get("sql_rows", 0) or 0)
    mongo_documents = int(result_summary.get("mongo_documents", 0) or 0)

    uses_sql = bool(plan_payload.get("sql"))
    uses_mongo = bool(plan_payload.get("mongo"))
    merge_info = plan_payload.get("merge") if isinstance(plan_payload.get("merge"), dict) else {}
    merge_key = merge_info.get("merge_key")
    merge_required = bool(uses_sql and uses_mongo and merge_key)

    if merge_required and sql_rows > 0 and mongo_documents == 0:
        return f"SQL returned rows, but no matching Mongo documents were found for merge key '{merge_key}'."
    if merge_required and mongo_documents > 0 and sql_rows == 0:
        return f"Mongo returned documents, but no matching SQL rows were found for merge key '{merge_key}'."

    if uses_sql and uses_mongo and sql_rows == 0 and mongo_documents == 0:
        return "No SQL rows or Mongo documents matched the current filters."
    if uses_sql and not uses_mongo and sql_rows == 0:
        sql_hint = _describe_sql_zero_match_reason(details)
        if sql_hint:
            return sql_hint
        return "No SQL rows matched the current filters."
    if uses_mongo and not uses_sql and mongo_documents == 0:
        return "No Mongo documents matched the current filters."

    if uses_sql and uses_mongo:
        return "Data was fetched from SQL/Mongo, but no merged logical records were produced."

    return "No logical results returned."


def _format_field_chips(items: List[str]) -> str:
    if not items:
        return "<span class='muted'>None</span>"
    return " ".join(f"<span class='chip'>{_safe(item)}</span>" for item in items)


def _render_query_explainability(record: QueryRecord) -> str:
    summary = record.summary or {}
    plan_view = summary.get("logical_plan") or {}
    badges = summary.get("explainability") or []
    backend_ops = summary.get("backend_operations") or {}

    badge_rows = []
    for badge in badges:
        storage = str(badge.get("storage") or "unknown").lower()
        css = {
            "sql": "storage-sql",
            "mongo": "storage-mongo",
            "buffer": "storage-buffer",
        }.get(storage, "storage-unknown")
        badge_rows.append(
            f"<tr><td>{_safe(badge.get('field'))}</td>"
            f"<td><span class='badge {css}'>{_safe(storage.upper())}</span></td>"
            f"<td>{_safe(badge.get('reason'))}</td>"
            f"<td>{_safe(badge.get('status'))}</td></tr>"
        )

    explainability_table = ""
    if badge_rows:
        explainability_table = f"""
<div class='card'>
  <h3>Explainability Badges</h3>
  <table>
    <thead><tr><th>Field</th><th>Storage</th><th>Why routed</th><th>Status</th></tr></thead>
    <tbody>{''.join(badge_rows)}</tbody>
  </table>
</div>
"""

    return f"""
<div class='card'>
  <h3>Logical Plan View</h3>
  <p><strong>Requested fields:</strong> {_format_field_chips(plan_view.get('requested_fields', []))}</p>
  <p><strong>SQL-resolved fields:</strong> {_format_field_chips(plan_view.get('sql_resolved_fields', []))}</p>
  <p><strong>Mongo-resolved fields:</strong> {_format_field_chips(plan_view.get('mongo_resolved_fields', []))}</p>
  <p><strong>Buffer-resolved fields:</strong> {_format_field_chips(plan_view.get('buffer_resolved_fields', []))}</p>
  <p><strong>Missing fields:</strong> {_format_field_chips(plan_view.get('missing_fields', []))}</p>
  <p><strong>Merge key used:</strong> {_safe(plan_view.get('merge_key_used') or 'None')}</p>
</div>
{explainability_table}
<div class='card'>
  <details>
    <summary><strong>Before/After Example (click to expand)</strong></summary>
    <p class='muted'>user query → logical result → backend operations</p>
    <h4>User query</h4>
    <pre>{_safe(_json_pretty(record.query_input))}</pre>
    <h4>Logical result</h4>
    <pre>{_safe(_json_pretty(record.logical_result))}</pre>
    <h4>Backend operations</h4>
    <pre>{_safe(_json_pretty(backend_ops))}</pre>
  </details>
</div>
"""


registry = SchemaRegistry(db_path=os.getenv("SCHEMA_REGISTRY_DB", "schema_registry.db"))
executor = HybridCRUDExecutor(registry=registry, metadata_file=os.getenv("METADATA_FILE", "metadata.json"))

session = DashboardSession(
    session_id=str(uuid.uuid4()),
    started_at=_now_iso(),
    registry_db=registry.db_path,
    metadata_file=os.getenv("METADATA_FILE", "metadata.json"),
)


def _summarize_entities() -> List[Dict[str, Any]]:
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


def _run_query(
    schema_id: int,
    fields: List[str],
    filters: Dict[str, Any],
    limit: Optional[int],
    execute: bool,
    *,
    record_history: bool = True,
) -> QueryRecord:
    query_input = {
        "operation": "read",
        "fields": fields,
        "filters": filters,
        "limit": limit,
        "execute": execute,
    }
    status = "ok"
    logical_result: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {}
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
        plan_payload = _extract_plan_payload(details)
        logical_result = details.get("results") or []
        summary = {
            "items": len(logical_result),
            "note": details.get("note") if not execute else None,
            "plan_summary": plan_summary,
            "logical_plan": _logical_plan_view(plan_payload),
            "explainability": _explainability_badges(plan_payload),
            "backend_operations": _backend_operations(details),
        }
        if execute and not logical_result:
            summary["note"] = _build_empty_read_reason(details)
    except Exception as exc:
        status = "failed"
        summary = {"error": _sanitize_error(), "detail": str(exc)}

    record = QueryRecord(
        query_input=query_input,
        status=status,
        logical_result=logical_result,
        summary=summary,
        timestamp=_now_iso(),
    )
    if record_history:
        session.queries.append(record)
    return record


def _run_crud(
    schema_id: Optional[int],
    entity: Optional[str],
    operation: str,
    fields: List[str],
    payload: Dict[str, Any],
    filters: Dict[str, Any],
    limit: Optional[int],
    strategy: str,
    execute: bool,
) -> QueryRecord:
    op = (operation or "read").lower()
    if schema_id is None:
        schema_id = _resolve_or_register_entity(entity, payload, op)
    if op == "read":
        return _run_query(
            schema_id,
            fields=fields,
            filters=filters,
            limit=limit,
            execute=execute,
            record_history=True,
        )

    query_input = {
        "operation": op,
        "fields": fields,
        "payload": payload,
        "filters": filters,
        "limit": limit,
        "strategy": strategy,
        "execute": execute,
    }

    status = "ok"
    summary: Dict[str, Any] = {}
    logical_result: List[Dict[str, Any]] = []

    try:
        result = executor.execute(
            schema_id,
            operation=op,
            payload=payload,
            fields=fields,
            filters=filters,
            limit=limit,
            strategy=strategy,
            execute=execute,
        )
        details = result.details
        plan_payload = _extract_plan_payload(details)
        summary = {
            "executed": execute,
            "operation": op,
            **_summarize_write(details),
            "logical_plan": _logical_plan_view(plan_payload),
            "explainability": _explainability_badges(plan_payload),
            "backend_operations": _backend_operations(details),
        }
    except Exception as exc:
        status = "failed"
        summary = {"error": _sanitize_error(), "detail": str(exc)}

    record = QueryRecord(
        query_input=query_input,
        status=status,
        logical_result=logical_result,
        summary=summary,
        timestamp=_now_iso(),
    )
    session.queries.append(record)
    return record


def _html_page(title: str, body: str) -> HTMLResponse:
    return HTMLResponse(
        f"""
<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <title>{_safe(title)}</title>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 2rem; background: #f8fafc; color: #1f2937; }}
    h1 {{ margin-bottom: 0.2rem; }}
    .card {{ background: #fff; border-radius: 8px; padding: 1rem 1.5rem; margin-bottom: 1rem; box-shadow: 0 2px 6px rgba(0,0,0,0.08); }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 1rem; }}
    .muted {{ color: #6b7280; font-size: 0.9rem; }}
    pre {{ background: #111827; color: #f9fafb; padding: 1rem; border-radius: 6px; overflow: auto; }}
    a {{ color: #2563eb; text-decoration: none; }}
    table {{ width: 100%; border-collapse: collapse; }}
    th, td {{ text-align: left; padding: 0.5rem; border-bottom: 1px solid #e5e7eb; }}
    .badge {{ display: inline-block; padding: 0.15rem 0.5rem; border-radius: 999px; background: #e5e7eb; font-size: 0.75rem; }}
        .chip {{ display: inline-block; margin: 0.1rem 0.25rem 0.1rem 0; padding: 0.2rem 0.5rem; border-radius: 999px; background: #eef2ff; font-size: 0.8rem; }}
        .storage-sql {{ background: #dbeafe; }}
        .storage-mongo {{ background: #dcfce7; }}
        .storage-buffer {{ background: #fef3c7; }}
        .storage-unknown {{ background: #f3f4f6; }}
        details summary {{ cursor: pointer; margin-bottom: 0.5rem; }}
    .success {{ background: #dcfce7; }}
    .failed {{ background: #fee2e2; }}
  </style>
</head>
<body>
  <h1>Logical Dashboard</h1>
  <p class='muted'>Local host UI for logical entities and queries (backend details hidden).</p>
  <div class='card'>
      <a href='/'>Home</a> · <a href='/entities'>Entities</a> · <a href='/crud'>CRUD</a> · <a href='/acid'>ACID Report</a> · <a href='/connections'>Test Connection</a> · <a href='/history'>Query History</a>
  </div>
  {body}
</body>
</html>
"""
    )


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    entities = _summarize_entities()
    body = f"""
<div class='card'>
  <h2>Active Session</h2>
  <div class='grid'>
    <div><strong>Session ID</strong><br />{_safe(session.session_id)}</div>
    <div><strong>Started</strong><br />{_safe(session.started_at)}</div>
    <div><strong>Registry DB</strong><br />{_safe(session.registry_db)}</div>
    <div><strong>Metadata File</strong><br />{_safe(session.metadata_file)}</div>
  </div>
  <p class='muted'>Schemas loaded: {len(entities)} · Queries run: {len(session.queries)}</p>
</div>
"""
    return _html_page("Logical Dashboard", body)


@app.get("/entities", response_class=HTMLResponse)
def list_entities() -> HTMLResponse:
    entities = _summarize_entities()
    if not entities:
        return _html_page("Logical Entities", "<div class='card'>No schemas registered yet.</div>")

    rows = """""".join(
        f"""
<tr>
  <td>{_safe(entity.get('schema_id'))}</td>
  <td>{_safe(entity.get('entity_name'))}</td>
  <td>{_safe(entity.get('field_count'))}</td>
  <td>{_safe(entity.get('created_at'))}</td>
  <td><a href='/entity/{entity.get('schema_id')}'>View</a></td>
</tr>
"""
        for entity in entities
    )
    body = f"""
<div class='card'>
  <h2>Logical Entities</h2>
  <table>
    <thead>
      <tr><th>ID</th><th>Name</th><th>Fields</th><th>Created</th><th></th></tr>
    </thead>
    <tbody>
      {rows}
    </tbody>
  </table>
</div>
"""
    return _html_page("Logical Entities", body)


@app.get("/entity/{schema_id}", response_class=HTMLResponse)
def entity_detail(schema_id: int) -> HTMLResponse:
    schema = registry.get_schema(schema_id)
    fields = _entity_fields(schema)
    field_rows = """""".join(
        f"""
<tr>
  <td>{_safe(item.get('field'))}</td>
  <td>{_safe(item.get('type'))}</td>
  <td>{_safe(item.get('nullable'))}</td>
  <td>{_safe(item.get('primary_key'))}</td>
  <td>{_safe(item.get('unique'))}</td>
</tr>
"""
        for item in fields
    )

    preview_fields = [item["field"] for item in fields if item.get("field")][:6]
    preview_html = "<p class='muted'>No fields available for preview.</p>"
    if preview_fields:
        preview_execute = _default_preview_execute()
        record = _run_query(
            schema_id,
            fields=preview_fields,
            filters={},
            limit=3,
            execute=preview_execute,
            record_history=False,
        )

        # If preview is configured as dry-run and returns no logical rows, retry once with execute=True.
        if not preview_execute and record.status == "ok" and not record.logical_result:
            record = _run_query(
                schema_id,
                fields=preview_fields,
                filters={},
                limit=3,
                execute=True,
                record_history=False,
            )

        preview_detail = ""
        if record.status != "ok":
            detail = (record.summary or {}).get("detail") or (record.summary or {}).get("error")
            if detail:
                preview_detail = f"<p class='muted'><strong>Reason:</strong> {_safe(detail)}</p>"
        elif not record.logical_result:
            note = (record.summary or {}).get("note") or "No rows exist yet for this entity in the configured backends."
            preview_detail = f"<p class='muted'><strong>Reason:</strong> {_safe(note)}</p>"

        result_block = _safe(_json_pretty(record.logical_result)) if record.logical_result else "No sample rows returned."
        preview_html = f"""
<div class='card'>
  <h3>Sample Instances (logical preview)</h3>
  <p>Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span></p>
  {preview_detail}
  <pre>{result_block}</pre>
</div>
"""

    body = f"""
<div class='card'>
  <h2>Entity: {_safe(schema.get('entity_name'))} (schema_id={schema_id})</h2>
  <table>
    <thead>
      <tr><th>Field</th><th>Type</th><th>Nullable</th><th>Primary Key</th><th>Unique</th></tr>
    </thead>
    <tbody>
      {field_rows}
    </tbody>
  </table>
</div>
{preview_html}
"""
    return _html_page("Entity Details", body)




@app.get("/crud", response_class=HTMLResponse)
def crud_form() -> HTMLResponse:
        entities = _summarize_entities()
        options = """""".join(
                f"<option value='{entity.get('schema_id')}'>{_safe(entity.get('entity_name'))} (id={entity.get('schema_id')})</option>"
                for entity in entities
        )
        body = f"""
<div class='card'>
    <h2>CRUD Operation</h2>
    <form method='post'>
        <label>Schema ID (optional)</label><br />
        <select name='schema_id'>
            <option value=''>-- Auto-register using entity + payload --</option>
            {options}
        </select><br /><br />

        <label>Entity Name (used when schema_id is blank)</label><br />
        <input type='text' name='entity' placeholder='user_activity' style='width: 100%' /><br /><br />

        <label>Operation</label><br />
        <select name='operation'>
            <option value='read'>read</option>
            <option value='insert'>insert</option>
            <option value='update'>update</option>
            <option value='delete'>delete</option>
        </select><br /><br />

        <label>Fields (comma-separated) — for read</label><br />
        <input type='text' name='fields' placeholder='username,comments' style='width: 100%' /><br /><br />

        <label>Payload (JSON object) — for insert/update</label><br />
        <textarea name='payload' rows='4' style='width: 100%'></textarea><br /><br />

        <label>Filters (JSON object) — for read/update/delete</label><br />
        <textarea name='filters' rows='3' style='width: 100%'></textarea><br /><br />

        <label>Limit (optional, read)</label><br />
        <input type='number' name='limit' min='1' /><br /><br />

        <label>Strategy (update/delete)</label><br />
        <input type='text' name='strategy' placeholder='simple or entity/sub-entity' style='width: 100%' /><br /><br />

        <label><input type='checkbox' name='execute' /> Execute against live backends</label><br /><br />
        <button type='submit'>Run Operation</button>
    </form>
</div>
"""
        return _html_page("CRUD Operations", body)




@app.post("/crud", response_class=HTMLResponse)
def crud_submit(
    schema_id: Optional[str] = Form(None),
    entity: str = Form(""),
    operation: str = Form("read"),
    fields: str = Form(""),
    payload: str = Form(""),
    filters: str = Form(""),
    limit: Optional[int] = Form(None),
    strategy: str = Form("simple"),
    execute: Optional[str] = Form(None),
) -> HTMLResponse:
    parsed_schema_id: Optional[int] = None
    if schema_id and schema_id.strip():
        parsed_schema_id = int(schema_id)
    parsed_fields = [value.strip() for value in fields.split(",") if value.strip()]
    try:
        parsed_payload = _parse_payload(payload)
        parsed_filters = _parse_filters(filters)
    except ValueError as exc:
        return _html_page("CRUD Result", f"<div class='card'><strong>Error:</strong> { _safe(str(exc)) }</div>")

    record = _run_crud(
        parsed_schema_id,
        entity,
        operation=operation,
        fields=parsed_fields,
        payload=parsed_payload,
        filters=parsed_filters,
        limit=limit,
        strategy=strategy or "simple",
        execute=bool(execute),
    )

    body = f"""
<div class='card'>
  <h2>CRUD Result</h2>
  <p>Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span></p>
    <h3>Input</h3>
    <pre>{_safe(_json_pretty(record.query_input))}</pre>
    <h3>Logical Result</h3>
    <pre>{_safe(_json_pretty(record.logical_result))}</pre>
  <h3>Summary</h3>
    <pre>{_safe(_json_pretty(record.summary))}</pre>
</div>
{_render_query_explainability(record)}
"""
    return _html_page("CRUD Result", body)


@app.get("/history", response_class=HTMLResponse)
def query_history() -> HTMLResponse:
    if not session.queries:
        return _html_page("Query History", "<div class='card'>No queries executed yet.</div>")

    cards = []
    for record in session.queries[::-1]:
        cards.append(
            f"""
<div class='card'>
  <p class='muted'>{_safe(record.timestamp)} · Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span></p>
        <strong>Input</strong>
        <pre>{_safe(_json_pretty(record.query_input))}</pre>
        <strong>Logical Result</strong>
        <pre>{_safe(_json_pretty(record.logical_result))}</pre>
    <strong>Summary</strong>
    <pre>{_safe(_json_pretty(record.summary))}</pre>
</div>
{_render_query_explainability(record)}
"""
        )

    return _html_page("Query History", "".join(cards))


@app.get("/acid", response_class=HTMLResponse)
def acid_report() -> HTMLResponse:
        report = _acid_report()
        atomicity = report.get("atomicity", {})
        consistency = report.get("consistency", {})
        isolation = report.get("isolation", {})
        durability = report.get("durability", {})

        sql_counts = durability.get("sql_tables", {})
        mongo_counts = durability.get("mongo_collections", {})
        fk_report = consistency.get("fk_violations", {})

        body = f"""
<div class='card'>
    <h2>ACID Validation Report</h2>
    <p class='muted'>Snapshot based on current data in SQL/Mongo backends.</p>
</div>

<div class='card'>
    <h3>Atomicity</h3>
    <p>Status: <span class='badge {'success' if atomicity.get('enabled') else 'failed'}'>
        {_safe('enabled' if atomicity.get('enabled') else 'disabled')}</span></p>
    <p class='muted'>{_safe(atomicity.get('note', ''))}</p>
</div>

<div class='card'>
    <h3>Consistency</h3>
    <p>Status: <span class='badge {'success' if consistency.get('status') == 'pass' else 'failed'}'>
        {_safe(consistency.get('status', 'unknown'))}</span></p>
    <pre>{_safe(fk_report)}</pre>
</div>

<div class='card'>
    <h3>Isolation</h3>
    <pre>{_safe(isolation)}</pre>
</div>

<div class='card'>
    <h3>Durability (Current Data)</h3>
    <h4>SQL Table Counts</h4>
    <pre>{_safe(sql_counts)}</pre>
    <h4>Mongo Collection Counts</h4>
    <pre>{_safe(mongo_counts)}</pre>
</div>
"""

        return _html_page("ACID Report", body)


@app.get("/connections", response_class=HTMLResponse)
def connection_page() -> HTMLResponse:
        results = _test_connections()
        mysql = results.get("mysql", {})
        mongo = results.get("mongo", {})
        body = f"""
<div class='card'>
    <h2>Connection Test</h2>
    <p>Status:</p>
    <ul>
        <li>MySQL: <strong>{'OK' if mysql.get('ok') else 'FAILED'}</strong> { _safe(mysql.get('error', '')) }</li>
        <li>MongoDB: <strong>{'OK' if mongo.get('ok') else 'FAILED'}</strong> { _safe(mongo.get('error', '')) }</li>
    </ul>
    <p class='muted'>Refresh this page to re-run the checks.</p>
</div>
"""
        return _html_page("Connection Test", body)


def _resolve_or_register_entity(entity: Optional[str], payload: Dict[str, Any], operation: str) -> int:
    entity = (entity or "").strip()
    if not entity:
        raise ValueError("Entity name is required when schema_id is not provided")
    existing = registry.list_schemas(entity)
    if existing:
        return int(existing[0]["schema_id"])
    if operation == "read":
        raise ValueError("Schema not found for entity; provide schema_id or run an insert with payload first")
    if not payload:
        raise ValueError("Payload is required to register a new entity")
    stored = registry.register_schema(entity, payload)
    return int(stored["schema_id"])


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8003)
