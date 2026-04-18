"""Logical dashboard web UI (FastAPI).

Serves a local-hosted dashboard that presents logical entities, instances,
and query results without exposing backend-specific storage details.
"""

from __future__ import annotations

import json
import os
import re
import uuid
import base64
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
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
    duration_ms: float = 0.0


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


def _utc_now() -> datetime:
    return datetime.utcnow()


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


def _perf_artifact_dir() -> Path:
    return Path(__file__).resolve().parent / "docs" / "perf_artifacts"


def _read_json_artifact(path: Path) -> Optional[Dict[str, Any]]:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
            return data if isinstance(data, dict) else None
    except Exception:
        return None


def _perf_summary_artifact() -> Optional[Dict[str, Any]]:
    return _read_json_artifact(_perf_artifact_dir() / "assignment4_perf_summary.json")


def _normalize_storage_backend(raw_value: Any) -> str:
    token = str(raw_value or "").strip().lower()
    if token in {"sql", "mysql", "relational", "rdbms"}:
        return "sql"
    if token in {"mongo", "mongodb", "document", "nosql", "embed", "embedded", "reference"}:
        return "mongo"
    if token in {"buffer", "cache", "queue"}:
        return "buffer"
    return "unknown"


def _distribution_from_schema(schema: Dict[str, Any]) -> Dict[str, int]:
    counts = {"sql": 0, "mongo": 0, "buffer": 0, "unknown": 0}
    storage_strategy = schema.get("storage_strategy") or {}
    field_mappings = ((storage_strategy.get("mappings") or {}).get("fields") or []) if isinstance(storage_strategy, dict) else []

    if field_mappings:
        for mapping in field_mappings:
            backend = _normalize_storage_backend(mapping.get("decision") or mapping.get("storage"))
            counts[backend] += 1
        return counts

    for field in schema.get("fields", []):
        backend = _normalize_storage_backend(field.get("storage_strategy") or field.get("decision") or field.get("storage"))
        counts[backend] += 1
    return counts


def _live_distribution_from_registry(schema_ids: Optional[List[int]] = None) -> Dict[str, Any]:
    all_schemas = registry.list_schemas()
    if not all_schemas:
        return {
            "source": "none",
            "distribution": {"sql": 0, "mongo": 0, "buffer": 0, "unknown": 0},
            "entities": [],
        }

    selected: List[Dict[str, Any]] = []
    selected_ids = {int(sid) for sid in (schema_ids or []) if isinstance(sid, int) and sid > 0}
    if selected_ids:
        for item in all_schemas:
            try:
                if int(item.get("schema_id")) in selected_ids:
                    selected.append(item)
            except Exception:
                continue

    if not selected:
        selected = list(all_schemas)

    totals = {"sql": 0, "mongo": 0, "buffer": 0, "unknown": 0}
    entities: List[str] = []
    for item in selected:
        try:
            schema = registry.get_schema(int(item["schema_id"]))
        except Exception:
            continue
        entity_name = str(item.get("entity_name") or f"schema_{item.get('schema_id')}")
        entities.append(entity_name)
        counts = _distribution_from_schema(schema)
        for key in totals:
            totals[key] += int(counts.get(key, 0) or 0)

    source_label = "registry:queried_entities" if selected_ids else "registry:all_entities"
    return {
        "source": source_label,
        "distribution": totals,
        "entities": sorted(set(entities)),
    }


def _image_data_uri(path: Path) -> Optional[str]:
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        encoded = base64.b64encode(raw).decode("ascii")
        return f"data:image/png;base64,{encoded}"
    except Exception:
        return None


def _sanitize_error() -> str:
    return "Execution failed due to system unavailability or configuration. Try dry-run mode to validate logical queries."


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
        seen_relations: set[tuple[str, str, str, str]] = set()
        cursor.execute("SHOW TABLES")
        existing_tables = {
            _normalize_table_lookup_name(row[0])
            for row in cursor.fetchall()
            if row and row[0] is not None
        }
        for schema in registry.list_schemas():
            detail = registry.get_schema(schema["schema_id"])
            entity_name = str(detail.get("entity_name") or "").strip().lower()
            blueprint = detail.get("sql_blueprint") or detail.get("analysis", {}).get("sql_blueprint")
            if entity_name != "university" or not blueprint:
                continue
            for relation in blueprint.get("relationships", []):
                child = relation.get("from_table")
                parent = relation.get("to_table")
                child_col = relation.get("from_column")
                parent_col = relation.get("to_column")
                if not all([child, parent, child_col, parent_col]):
                    continue
                signature = (
                    _normalize_table_lookup_name(child),
                    _normalize_table_lookup_name(parent),
                    str(child_col).lower(),
                    str(parent_col).lower(),
                )
                if signature in seen_relations:
                    continue
                seen_relations.add(signature)
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
            actual_level_raw = (row[0] if row else "unknown")
            actual_level = str(actual_level_raw).strip().replace("_", "-").upper()
            required_level = os.getenv("REQUIRED_ISOLATION_LEVEL", "REPEATABLE-READ").strip().replace("_", "-").upper()
            isolation_rank = {
                "READ-UNCOMMITTED": 1,
                "READ-COMMITTED": 2,
                "REPEATABLE-READ": 3,
                "SERIALIZABLE": 4,
            }
            actual_rank = isolation_rank.get(actual_level)
            required_rank = isolation_rank.get(required_level)
            if actual_rank is None or required_rank is None:
                isolation_info = {
                    "ok": False,
                    "level": actual_level_raw,
                    "required_level": required_level,
                    "error": "unknown isolation level comparison",
                }
            elif actual_rank >= required_rank:
                isolation_info = {
                    "ok": True,
                    "level": actual_level_raw,
                    "required_level": required_level,
                }
            else:
                isolation_info = {
                    "ok": False,
                    "level": actual_level_raw,
                    "required_level": required_level,
                    "error": "isolation level below required policy",
                }
        except Exception as exc:  # pragma: no cover
            isolation_info = {"ok": False, "error": str(exc)}

    tx_coord_enabled = os.getenv("TRANSACTION_COORDINATION", "1").strip().lower() in {"1", "true", "yes", "on"}
    tx_prereq_ok = bool(sql_counts.get("ok")) and bool(mongo_counts.get("ok")) and bool(isolation_info.get("ok"))
    atomicity_ok = tx_coord_enabled and tx_prereq_ok
    atomicity_note = "SQL transaction + Mongo session/compensating rollback"
    if not tx_coord_enabled:
        atomicity_note += " (disabled by TRANSACTION_COORDINATION)"
    elif not tx_prereq_ok:
        atomicity_note += " (backend transaction prerequisites unavailable)"
    consistency_ok = fk_report.get("ok") and fk_report.get("total_missing", 0) == 0

    return {
        "atomicity": {
            "enabled": atomicity_ok,
            "note": atomicity_note,
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

    inserted_total = 0
    if isinstance(sql, dict):
        inserted_total += int(sql.get("rows_inserted") or 0)

    if isinstance(mongo, dict):
        inserted_total += int(mongo.get("documents_inserted") or 0)

    if inserted_total:
        summary["inserted_items"] = inserted_total

    if isinstance(details, dict) and details.get("strategy"):
        summary["strategy"] = details.get("strategy")

    return summary


def _plan_summary(plan: Dict[str, Any]) -> Dict[str, Any]:
    field_locations = plan.get("field_locations") or []
    resolved = [loc for loc in field_locations if _is_field_status_resolved(loc.get("status"))]
    missing = [loc for loc in field_locations if loc.get("status") == "missing"]
    merge_key = None
    if isinstance(plan.get("merge"), dict):
        merge_key = plan.get("merge", {}).get("merge_key")
    return {
        "requested_fields": [loc.get("requested") for loc in field_locations],
        "resolved_fields": len(resolved),
        "missing_fields": len(missing),
        "merge_key_present": bool(merge_key),
        "merge_required": bool(merge_key),
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
            "resolved_fields": [],
            "missing_fields": [],
        }

    field_locations = plan.get("field_locations") or []
    requested = [loc.get("requested") for loc in field_locations if loc.get("requested")]
    resolved_fields = [
        loc.get("requested")
        for loc in field_locations
        if _is_field_status_resolved(loc.get("status")) and loc.get("requested")
    ]
    missing_fields = [
        loc.get("requested")
        for loc in field_locations
        if loc.get("status") == "missing" and loc.get("requested")
    ]

    return {
        "requested_fields": requested,
        "resolved_fields": resolved_fields,
        "missing_fields": missing_fields,
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
        note = str(loc.get("notes") or "")
        reason = note_map.get(note, note or "routing rule")
        badges.append(
            {
                "field": field_name,
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
    if isinstance(parameters, dict):
        parameters = list(parameters.values())
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
        return f"Partial logical fragments were found, but merge key '{merge_key}' did not produce complete records."
    if merge_required and mongo_documents > 0 and sql_rows == 0:
        return f"Partial logical fragments were found, but merge key '{merge_key}' did not produce complete records."

    if uses_sql and uses_mongo and sql_rows == 0 and mongo_documents == 0:
        return "No records matched the current filters."
    if uses_sql and not uses_mongo and sql_rows == 0:
        sql_hint = _describe_sql_zero_match_reason(details)
        if sql_hint:
            return sql_hint
        return "No records matched the current filters."
    if uses_mongo and not uses_sql and mongo_documents == 0:
        return "No records matched the current filters."

    if uses_sql and uses_mongo:
        return "Data was fetched, but no merged logical records were produced."

    return "No logical results returned."


def _format_field_chips(items: List[str]) -> str:
    if not items:
        return "<span class='muted'>None</span>"
    return " ".join(f"<span class='chip'>{_safe(item)}</span>" for item in items)


def _default_read_fields(schema_id: int) -> List[str]:
    try:
        schema = registry.get_schema(schema_id)
    except Exception:
        return []

    fields: List[str] = []
    seen: set[str] = set()
    for field in schema.get("fields", []):
        field_name = field.get("field_path") or field.get("field_name")
        if field_name:
            normalized = str(field_name)
            if normalized not in seen:
                seen.add(normalized)
                fields.append(normalized)

    for entry in (schema.get("mongo_strategy") or {}).get("entries", []):
        field_name = entry.get("field_path")
        if field_name:
            normalized = str(field_name)
            if normalized not in seen:
                seen.add(normalized)
                fields.append(normalized)

    return fields


def _render_logical_result_table(
    rows: List[Dict[str, Any]],
    empty_message: str = "No logical results returned.",
    preferred_columns: Optional[List[str]] = None,
) -> str:
    if not rows:
        return f"<p class='muted'>{_safe(empty_message)}</p>"

    visible_rows: List[Dict[str, Any]] = []
    for row in rows:
        if isinstance(row, dict):
            visible_rows.append(row)
        else:
            visible_rows.append({"value": row})

    columns: List[str] = []
    for column in preferred_columns or []:
        if column and column not in columns:
            columns.append(str(column))
    for row in visible_rows:
        for key in row.keys():
            if key not in columns:
                columns.append(str(key))

    cells = []
    for row in visible_rows:
        row_cells = []
        for column in columns:
            value = row.get(column, "")
            if isinstance(value, (dict, list)):
                cell_text = _json_pretty(value)
            else:
                cell_text = str(value)
            row_cells.append(f"<td><pre class='cell-pre'>{_safe(cell_text)}</pre></td>")
        cells.append(f"<tr>{''.join(row_cells)}</tr>")

    header = "".join(f"<th>{_safe(column)}</th>" for column in columns)
    return f"""
<table class='result-table'>
  <thead><tr>{header}</tr></thead>
  <tbody>{''.join(cells)}</tbody>
</table>
"""


def _render_logical_result(record: QueryRecord) -> str:
    operation = _query_operation(record)
    if operation == "read":
        empty_message = str((record.summary or {}).get("note") or "No logical results returned.")
        plan_view = (record.summary or {}).get("logical_plan") or {}
        preferred_columns = plan_view.get("requested_fields") or (record.query_input or {}).get("fields") or []
        return _render_logical_result_table(
            record.logical_result,
            empty_message=empty_message,
            preferred_columns=[str(field) for field in preferred_columns if str(field).strip()],
        )
    return f"<pre>{_safe(_json_pretty(record.logical_result))}</pre>"


def _extract_read_results(details: Dict[str, Any]) -> List[Dict[str, Any]]:
    if not isinstance(details, dict):
        return []

    candidates: List[Any] = [
        details.get("results"),
        details.get("logical_result"),
        details.get("rows"),
        details.get("merged_results"),
    ]
    nested_data = details.get("data")
    if isinstance(nested_data, dict):
        candidates.append(nested_data.get("results"))

    for candidate in candidates:
        if isinstance(candidate, list):
            normalized: List[Dict[str, Any]] = []
            for item in candidate:
                if isinstance(item, dict):
                    normalized.append(item)
                else:
                    normalized.append({"value": item})
            return normalized

    return []


def _render_query_explainability(record: QueryRecord) -> str:
    summary = record.summary or {}
    plan_view = summary.get("logical_plan") or {}
    badges = summary.get("explainability") or []
    before_after_logical_result = _render_logical_result(record)

    badge_rows = []
    for badge in badges:
        badge_rows.append(
            f"<tr><td>{_safe(badge.get('field'))}</td>"
            f"<td>{_safe(badge.get('reason'))}</td>"
            f"<td>{_safe(badge.get('status'))}</td></tr>"
        )

    explainability_table = ""
    if badge_rows:
        explainability_table = f"""
<div class='card'>
  <h3>Explainability Badges</h3>
  <table>
        <thead><tr><th>Field</th><th>Why routed</th><th>Status</th></tr></thead>
    <tbody>{''.join(badge_rows)}</tbody>
  </table>
</div>
"""

    return f"""
<div class='card'>
    <h3>Logical Plan View</h3>
    <p><strong>Requested fields:</strong> {_format_field_chips(plan_view.get('requested_fields', []))}</p>
    <p><strong>Resolved fields:</strong> {_format_field_chips(plan_view.get('resolved_fields', []))}</p>
    <p><strong>Missing fields:</strong> {_format_field_chips(plan_view.get('missing_fields', []))}</p>
</div>
{explainability_table}
<div class='card'>
    <details>
        <summary><strong>Before/After Example (click to expand)</strong></summary>
        <p class='muted'>user query -> logical result</p>
        <h4>User query</h4>
        <pre>{_safe(_json_pretty(record.query_input))}</pre>
        <h4>Logical result</h4>
        {before_after_logical_result}
    </details>
</div>
"""


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(value)
    except Exception:
        return default


def _query_operation(record: QueryRecord) -> str:
    return str((record.query_input or {}).get("operation") or "read").lower()


def _aggregate_query_metrics(records: List[QueryRecord]) -> Dict[str, Any]:
    if not records:
        return {
            "total_queries": 0,
            "success_queries": 0,
            "failed_queries": 0,
            "avg_latency_ms": 0.0,
            "p95_latency_ms": 0.0,
            "throughput_qps": 0.0,
            "operation_breakdown": {},
        }

    latencies = sorted(float(r.duration_ms or 0.0) for r in records)
    total = len(records)
    success = sum(1 for r in records if r.status == "ok")
    failed = total - success
    avg_latency = sum(latencies) / total if total else 0.0
    p95_idx = max(0, min(total - 1, int(0.95 * total) - 1))
    p95_latency = latencies[p95_idx]

    parsed_times: List[datetime] = []
    for record in records:
        try:
            parsed_times.append(datetime.fromisoformat(record.timestamp))
        except Exception:
            continue

    throughput_qps = 0.0
    if len(parsed_times) >= 2:
        parsed_times.sort()
        window = (parsed_times[-1] - parsed_times[0]).total_seconds()
        if window > 0:
            throughput_qps = total / window

    breakdown: Dict[str, Dict[str, int]] = {}
    for record in records:
        op = _query_operation(record)
        status_bucket = breakdown.setdefault(op, {"ok": 0, "failed": 0, "total": 0})
        status_bucket["total"] += 1
        if record.status == "ok":
            status_bucket["ok"] += 1
        else:
            status_bucket["failed"] += 1

    return {
        "total_queries": total,
        "success_queries": success,
        "failed_queries": failed,
        "avg_latency_ms": round(avg_latency, 3),
        "p95_latency_ms": round(p95_latency, 3),
        "throughput_qps": round(throughput_qps, 3),
        "operation_breakdown": breakdown,
    }


def _filter_query_records(
    records: List[QueryRecord],
    status: Optional[str],
    operation: Optional[str],
    limit: Optional[int],
) -> List[QueryRecord]:
    status_filter = (status or "all").strip().lower()
    operation_filter = (operation or "all").strip().lower()

    filtered = records
    if status_filter in {"ok", "failed"}:
        filtered = [record for record in filtered if record.status == status_filter]
    if operation_filter in {"read", "insert", "update", "delete"}:
        filtered = [record for record in filtered if _query_operation(record) == operation_filter]

    if limit is not None and limit > 0:
        filtered = filtered[:limit]
    return filtered


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


def _resolve_entity_context(schema_id: int) -> Dict[str, Any]:
    try:
        schema = registry.get_schema(schema_id)
    except Exception:
        return {
            "schema_id": schema_id,
            "logical_entity": "unknown",
        }

    entity_name = schema.get("entity_name") or "unknown"
    return {
        "schema_id": schema_id,
        "logical_entity": entity_name,
    }


def _run_query(
    schema_id: int,
    fields: List[str],
    filters: Dict[str, Any],
    limit: Optional[int],
    execute: bool,
    *,
    record_history: bool = True,
) -> QueryRecord:
    started_at = _utc_now()
    entity_context = _resolve_entity_context(schema_id)
    query_input = {
        "operation": "read",
        **entity_context,
        "fields": fields,
        "filters": filters,
        "limit": limit,
        "execute": execute,
    }
    effective_fields = list(fields)
    if not effective_fields:
        effective_fields = _default_read_fields(schema_id)
    status = "ok"
    logical_result: List[Dict[str, Any]] = []
    summary: Dict[str, Any] = {}
    try:
        result = executor.execute(
            schema_id,
            operation="read",
            fields=effective_fields,
            filters=filters,
            limit=limit,
            execute=execute,
        )
        details = result.details
        plan_summary = _plan_summary(details)
        plan_payload = _extract_plan_payload(details)
        logical_result = _extract_read_results(details)
        summary = {
            "items": len(logical_result),
            "note": details.get("note") if not execute else None,
            "plan_summary": plan_summary,
            "logical_plan": _logical_plan_view(plan_payload),
            "explainability": _explainability_badges(plan_payload),
            "effective_fields": effective_fields,
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
        duration_ms=(
            (_utc_now() - started_at).total_seconds() * 1000.0
        ),
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
    started_at = _utc_now()
    op = (operation or "read").lower()
    if schema_id is None:
        schema_id = _resolve_or_register_entity(entity, payload, op)
    if op == "read":
        effective_fields = fields or _default_read_fields(schema_id)
        return _run_query(
            schema_id,
            fields=effective_fields,
            filters=filters,
            limit=limit,
            execute=execute,
            record_history=True,
        )

    query_input = {
        "operation": op,
        **_resolve_entity_context(schema_id),
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
        duration_ms=(
            (_utc_now() - started_at).total_seconds() * 1000.0
        ),
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
    .result-table th, .result-table td {{ vertical-align: top; }}
    .cell-pre {{ background: #f8fafc; color: #111827; margin: 0; padding: 0; white-space: pre-wrap; word-break: break-word; }}
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
    <p class='muted'>Logical schema interface for sessions, entities, instances, queries, and history.</p>
        <div class='card'>
                                                <a href='/'>Active Session</a> · <a href='/entities'>Logical Entities</a> · <a href='/crud'>Logical Query Result</a> · <a href='/monitor'>Query Monitor</a> · <a href='/history'>Query History</a> · <a href='/comparison'>Performance Comparison</a> · <a href='/acid'>Validation Report</a> · <a href='/connections'>Service Check</a>
  </div>
  {body}
</body>
</html>
"""
    )


@app.get("/", response_class=HTMLResponse)
def home() -> HTMLResponse:
    entities = _summarize_entities()
    metrics = _aggregate_query_metrics(session.queries)
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

<div class='card'>
    <h2>Logical Query Metrics</h2>
    <div class='grid'>
        <div><strong>Total Queries</strong><br />{_safe(metrics.get('total_queries'))}</div>
        <div><strong>Avg Latency (ms)</strong><br />{_safe(metrics.get('avg_latency_ms'))}</div>
        <div><strong>P95 Latency (ms)</strong><br />{_safe(metrics.get('p95_latency_ms'))}</div>
        <div><strong>Throughput (QPS)</strong><br />{_safe(metrics.get('throughput_qps'))}</div>
    </div>
    <p class='muted'>Monitoring is computed at logical query level and does not expose backend internals.</p>
</div>

<div class='card'>
    <h2>System Checks</h2>
    <p class='muted'>Use these dashboard pages to validate transactional guarantees and logical service availability.</p>
    <p><a href='/acid'>Open Validation Report</a> · <a href='/connections'>Run Service Check</a></p>
</div>

<div class='card'>
    <h2>Performance Comparison</h2>
    <p class='muted'>View framework vs direct SQL vs direct MongoDB latency/throughput comparison with charts.</p>
    <p><a href='/comparison'>Open Performance Comparison</a></p>
</div>

<div class='card'>
    <h2>Capability Map</h2>
    <p class='muted'>This dashboard supports all required logical operations:</p>
    <ul>
        <li><a href='/'>Viewing active sessions</a></li>
        <li><a href='/entities'>Listing logical entities within a session</a></li>
        <li><a href='/entities'>Viewing instances of each entity</a></li>
        <li><a href='/entities'>Inspecting field names and values of logical objects</a></li>
        <li><a href='/crud'>Displaying results of executed logical queries</a></li>
        <li><a href='/history'>Viewing query execution history</a></li>
    </ul>
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
    <p class='muted'>Browse the registered logical models and their visible fields.</p>
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
            note = (record.summary or {}).get("note") or "No rows exist yet for this entity in the configured data stores."
            preview_detail = f"<p class='muted'><strong>Reason:</strong> {_safe(note)}</p>"

        result_block = _render_logical_result_table(record.logical_result, "No sample rows returned.")
        preview_html = f"""
<div class='card'>
  <h3>Sample Instances (logical preview)</h3>
  <p>Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span></p>
  {preview_detail}
  {result_block}
</div>
"""

    body = f"""
<div class='card'>
  <h2>Entity: {_safe(schema.get('entity_name'))} (schema_id={schema_id})</h2>
    <p class='muted'>Inspect field names, types, and sample instances for this logical model.</p>
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

        <label><input type='checkbox' name='execute' /> Execute against live system</label><br /><br />
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
    <p>Operation: <span class='badge'>{_safe((record.query_input or {}).get('operation'))}</span> · Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span></p>
    <h3>Logical Result</h3>
    {_render_logical_result(record)}
</div>
"""
    return _html_page("CRUD Result", body)


@app.get("/history", response_class=HTMLResponse)
def query_history(
    status: str = "all",
    operation: str = "all",
    limit: int = 100,
) -> HTMLResponse:
    records = _filter_query_records(
        session.queries[::-1],
        status=status,
        operation=operation,
        limit=_safe_int(limit, 100),
    )

    if not records:
        return _html_page("Query History", "<div class='card'>No queries executed yet.</div>")

    cards = []
    for record in records:
        query_input = record.query_input or {}
        logical_entity = query_input.get("logical_entity")
        schema_id = query_input.get("schema_id")
        fields = query_input.get("fields") if isinstance(query_input.get("fields"), list) else []
        filters = query_input.get("filters") if isinstance(query_input.get("filters"), dict) else {}
        limit_value = query_input.get("limit")
        note = (record.summary or {}).get("note") if isinstance(record.summary, dict) else None
        cards.append(
            f"""
<div class='card'>
  <p class='muted'>{_safe(record.timestamp)} · Operation: {_safe(_query_operation(record))} · Status: <span class='badge {'success' if record.status == 'ok' else 'failed'}'>{_safe(record.status)}</span> · Duration: {_safe(round(record.duration_ms, 3))} ms</p>
    {f"<p class='muted'><strong>Logical Entity:</strong> {_safe(logical_entity)} (schema_id={_safe(schema_id)})</p>" if logical_entity else ''}
        {f"<p class='muted'><strong>Fields:</strong> {_safe(', '.join(str(f) for f in fields))}</p>" if fields else ''}
        {f"<p class='muted'><strong>Filters:</strong> {_safe(_json_pretty(filters))}</p>" if filters else ''}
        {f"<p class='muted'><strong>Limit:</strong> {_safe(limit_value)}</p>" if limit_value not in (None, '') else ''}
        {f"<p class='muted'><strong>Note:</strong> {_safe(note)}</p>" if note else ''}
        <strong>Logical Result</strong>
        {_render_logical_result(record)}
</div>
"""
        )

    return _html_page("Query History", "".join(cards))


@app.get("/monitor", response_class=HTMLResponse)
def query_monitor(
        status: str = "all",
        operation: str = "all",
        limit: int = 100,
) -> HTMLResponse:
        recent_records = _filter_query_records(
                session.queries[::-1],
                status=status,
                operation=operation,
                limit=_safe_int(limit, 100),
        )
        metrics = _aggregate_query_metrics(recent_records)

        op_rows = "".join(
                f"<tr><td>{_safe(op)}</td><td>{_safe(data.get('total'))}</td><td>{_safe(data.get('ok'))}</td><td>{_safe(data.get('failed'))}</td></tr>"
                for op, data in metrics.get("operation_breakdown", {}).items()
        )
        if not op_rows:
                op_rows = "<tr><td colspan='4' class='muted'>No data</td></tr>"

        recent_rows = "".join(
                f"<tr><td>{_safe(record.timestamp)}</td><td>{_safe(_query_operation(record))}</td><td>{_safe(record.status)}</td><td>{_safe(round(record.duration_ms, 3))}</td></tr>"
                for record in recent_records[:20]
        )
        if not recent_rows:
                recent_rows = "<tr><td colspan='4' class='muted'>No query history yet.</td></tr>"

        perf_summary = _perf_summary_artifact() or {}
        recent_schema_ids = sorted(
            {
                int((record.query_input or {}).get("schema_id"))
                for record in recent_records
                if isinstance((record.query_input or {}).get("schema_id"), int)
            }
        )
        live_distribution_payload = _live_distribution_from_registry(recent_schema_ids)
        distribution = live_distribution_payload.get("distribution") if isinstance(live_distribution_payload, dict) else {}
        if not isinstance(distribution, dict) or not any(int(distribution.get(k, 0) or 0) for k in ("sql", "mongo", "buffer", "unknown")):
            artifact_distribution = perf_summary.get("distribution") if isinstance(perf_summary, dict) else {}
            distribution = artifact_distribution if isinstance(artifact_distribution, dict) else {"sql": 0, "mongo": 0, "buffer": 0, "unknown": 0}
            distribution_source = "artifact:assignment4_perf_summary.json"
            distribution_entities = []
        else:
            distribution_source = str(live_distribution_payload.get("source") or "registry")
            distribution_entities = live_distribution_payload.get("entities") if isinstance(live_distribution_payload.get("entities"), list) else []

        def _metric_row(label: str, key: str) -> str:
            source = perf_summary.get(key) if isinstance(perf_summary, dict) else None
            if not isinstance(source, dict):
                return (
                    f"<tr><td>{_safe(label)}</td><td colspan='3' class='muted'>"
                    "No benchmark artifact data found for this metric.</td></tr>"
                )
            return (
                f"<tr><td>{_safe(label)}</td>"
                f"<td>{_safe(source.get('avg_latency_ms'))}</td>"
                f"<td>{_safe(source.get('throughput_ops_per_sec'))}</td>"
                f"<td>{_safe(source.get('p95_latency_ms'))}</td></tr>"
            )

        benchmark_rows = "".join(
            [
                _metric_row("Data ingestion latency", "ingestion"),
                _metric_row("Logical query response time", "logical_query"),
                _metric_row("Metadata lookup overhead", "metadata_lookup"),
                _metric_row("Transaction coordination overhead (SQL + MongoDB)", "transaction_coordination_overhead"),
            ]
        )

        distribution_rows = "".join(
            [
                f"<tr><td>SQL</td><td>{_safe(distribution.get('sql', 0))}</td></tr>",
                f"<tr><td>MongoDB</td><td>{_safe(distribution.get('mongo', 0))}</td></tr>",
                f"<tr><td>Buffer</td><td>{_safe(distribution.get('buffer', 0))}</td></tr>",
                f"<tr><td>Unknown</td><td>{_safe(distribution.get('unknown', 0))}</td></tr>",
            ]
        )

        body = f"""
<div class='card'>
    <h2>Query Monitor</h2>
    <p class='muted'>Scope: status={_safe(status)} · operation={_safe(operation)} · limit={_safe(limit)}</p>
    <div class='grid'>
        <div><strong>Total Queries</strong><br />{_safe(metrics.get('total_queries'))}</div>
        <div><strong>Successful</strong><br />{_safe(metrics.get('success_queries'))}</div>
        <div><strong>Failed</strong><br />{_safe(metrics.get('failed_queries'))}</div>
        <div><strong>Average Query Latency (ms)</strong><br />{_safe(metrics.get('avg_latency_ms'))}</div>
        <div><strong>P95 Latency (ms)</strong><br />{_safe(metrics.get('p95_latency_ms'))}</div>
        <div><strong>Throughput (operations per second)</strong><br />{_safe(metrics.get('throughput_qps'))}</div>
    </div>
</div>

<div class='card'>
    <h3>Performance Metrics (Assignment Experiments)</h3>
    <p class='muted'>Based on benchmark artifact summary for ingestion, logical query, metadata lookup, and SQL+MongoDB transaction coordination overhead.</p>
    <table>
        <thead><tr><th>Metric Source</th><th>Average Latency (ms)</th><th>Throughput (ops/sec)</th><th>P95 (ms)</th></tr></thead>
        <tbody>{benchmark_rows}</tbody>
    </table>
</div>

<div class='card'>
    <h3>Distribution of Data Across Storage Backends</h3>
    <p class='muted'>Source: {_safe(distribution_source)}</p>
    <p class='muted'>Entities in scope: {_safe(', '.join(str(entity) for entity in distribution_entities) if distribution_entities else 'all available entities')}</p>
    <table>
        <thead><tr><th>Backend</th><th>Mapped Fields</th></tr></thead>
        <tbody>{distribution_rows}</tbody>
    </table>
</div>

<div class='card'>
    <h3>Operation Breakdown</h3>
    <table>
        <thead><tr><th>Operation</th><th>Total</th><th>OK</th><th>Failed</th></tr></thead>
        <tbody>{op_rows}</tbody>
    </table>
</div>

<div class='card'>
    <h3>Recent Queries</h3>
    <table>
        <thead><tr><th>Timestamp</th><th>Operation</th><th>Status</th><th>Duration (ms)</th></tr></thead>
        <tbody>{recent_rows}</tbody>
    </table>
</div>
"""
        return _html_page("Query Monitor", body)


@app.get("/comparison", response_class=HTMLResponse)
def performance_comparison() -> HTMLResponse:
    artifact_dir = _perf_artifact_dir()
    comparison_path = artifact_dir / "assignment4_comparison_comparison.json"
    comparison = _read_json_artifact(comparison_path)

    if not comparison:
        body = """
<div class='card'>
    <h2>Performance Comparison</h2>
    <p class='muted'>Comparison artifact not found yet.</p>
    <p class='muted'>Run: <strong>python comparative_evaluation.py --iterations 20 --execute --output-prefix assignment4_comparison</strong></p>
</div>
"""
        return _html_page("Performance Comparison", body)

    logical = comparison.get("logical", {})
    direct = comparison.get("direct", {})
    overhead = comparison.get("overhead", {})
    throughput_curve = comparison.get("throughput_curve", {})

    bar_img = _image_data_uri(artifact_dir / "assignment4_comparison_latency_bar.png")
    line_img = _image_data_uri(artifact_dir / "assignment4_comparison_throughput_line.png")

    workload_rows = []
    workloads = throughput_curve.get("workloads") or []
    logical_series = throughput_curve.get("logical_ops_per_sec") or []
    direct_sql_series = throughput_curve.get("direct_sql_ops_per_sec") or []
    direct_mongo_series = throughput_curve.get("direct_mongo_ops_per_sec") or []

    def _avg_series(values: List[Any]) -> Optional[float]:
        cleaned = [float(v) for v in values if isinstance(v, (int, float))]
        if not cleaned:
            return None
        return round(sum(cleaned) / len(cleaned), 3)

    logical_tp_overall = logical.get("throughput_ops_per_sec")
    direct_sql_tp_overall = direct.get("sql_throughput_ops_per_sec")
    direct_mongo_tp_overall = direct.get("mongo_throughput_ops_per_sec")
    if direct_sql_tp_overall is None:
        direct_sql_tp_overall = _avg_series(direct_sql_series)
    if direct_mongo_tp_overall is None:
        direct_mongo_tp_overall = _avg_series(direct_mongo_series)

    read_overhead_pct = (overhead.get("read") or {}).get("relative_percent")
    nested_overhead_pct = (overhead.get("nested_read") or {}).get("relative_percent")
    update_overhead_pct = (overhead.get("update") or {}).get("relative_percent")

    read_metrics_rows = f"""
            <tr>
                <td>Query latency (ms)</td>
                <td>{_safe(logical.get('read_avg_ms'))}</td>
                <td>{_safe(direct.get('sql_read_avg_ms'))}</td>
                <td>-</td>
                <td>{_safe(read_overhead_pct)}</td>
            </tr>
            <tr>
                <td>Update latency (ms)</td>
                <td>-</td><td>-</td><td>-</td><td>-</td>
            </tr>
            <tr>
                <td>System throughput (ops/sec)</td>
                <td>{_safe(logical_tp_overall)}</td>
                <td>{_safe(direct_sql_tp_overall)}</td>
                <td>{_safe(direct_mongo_tp_overall)}</td>
                <td>-</td>
            </tr>
            <tr>
                <td>Query processing overhead introduced by framework (%)</td>
                <td>{_safe(read_overhead_pct)}</td>
                <td>-</td><td>-</td><td>-</td>
            </tr>
    """

    nested_metrics_rows = f"""
            <tr>
                <td>Query latency (ms)</td>
                <td>{_safe(logical.get('nested_read_avg_ms'))}</td>
                <td>-</td>
                <td>{_safe(direct.get('mongo_read_avg_ms'))}</td>
                <td>{_safe(nested_overhead_pct)}</td>
            </tr>
            <tr>
                <td>Update latency (ms)</td>
                <td>-</td><td>-</td><td>-</td><td>-</td>
            </tr>
            <tr>
                <td>System throughput (ops/sec)</td>
                <td>{_safe(logical_tp_overall)}</td>
                <td>{_safe(direct_sql_tp_overall)}</td>
                <td>{_safe(direct_mongo_tp_overall)}</td>
                <td>-</td>
            </tr>
            <tr>
                <td>Query processing overhead introduced by framework (%)</td>
                <td>{_safe(nested_overhead_pct)}</td>
                <td>-</td><td>-</td><td>-</td>
            </tr>
    """

    update_metrics_rows = f"""
            <tr>
                <td>Query latency (ms)</td>
                <td>-</td><td>-</td><td>-</td><td>-</td>
            </tr>
            <tr>
                <td>Update latency (ms)</td>
                <td>{_safe(logical.get('update_avg_ms'))}</td>
                <td>{_safe(direct.get('sql_update_avg_ms'))}</td>
                <td>{_safe(direct.get('mongo_update_avg_ms'))}</td>
                <td>{_safe(update_overhead_pct)}</td>
            </tr>
            <tr>
                <td>System throughput (ops/sec)</td>
                <td>{_safe(logical_tp_overall)}</td>
                <td>{_safe(direct_sql_tp_overall)}</td>
                <td>{_safe(direct_mongo_tp_overall)}</td>
                <td>-</td>
            </tr>
            <tr>
                <td>Query processing overhead introduced by framework (%)</td>
                <td>{_safe(update_overhead_pct)}</td>
                <td>-</td><td>-</td><td>-</td>
            </tr>
    """

    def _num(value: Any) -> float:
        try:
            return float(value)
        except Exception:
            return 0.0

    def _bar_rows(items: List[Dict[str, Any]], color: str) -> str:
        max_value = max((_num(item.get("value")) for item in items), default=0.0)
        if max_value <= 0:
            max_value = 1.0
        rows: List[str] = []
        for item in items:
            label = _safe(item.get("label", "-"))
            value = _num(item.get("value"))
            width_pct = round((value / max_value) * 100, 2) if value > 0 else 0.0
            rows.append(
                "<div style='display:grid;grid-template-columns:280px 1fr 90px;gap:10px;align-items:center;margin:8px 0;'>"
                f"<div>{label}</div>"
                "<div style='background:#e5e7eb;height:14px;border-radius:8px;overflow:hidden;'>"
                f"<div style='height:100%;width:{width_pct}%;background:{color};'></div>"
                "</div>"
                f"<div>{_safe(round(value, 3))} ms</div>"
                "</div>"
            )
        return "".join(rows)

    query_latency_bars = _bar_rows(
        [
            {"label": "User Retrieval - Framework", "value": logical.get("read_avg_ms")},
            {"label": "User Retrieval - Direct SQL", "value": direct.get("sql_read_avg_ms")},
            {"label": "Nested Access - Framework", "value": logical.get("nested_read_avg_ms")},
            {"label": "Nested Access - Direct MongoDB", "value": direct.get("mongo_read_avg_ms")},
        ],
        color="#0369a1",
    )

    update_latency_bars = _bar_rows(
        [
            {"label": "Cross-Entity Update - Framework", "value": logical.get("update_avg_ms")},
            {"label": "Cross-Entity Update - Direct SQL", "value": direct.get("sql_update_avg_ms")},
            {"label": "Cross-Entity Update - Direct MongoDB", "value": direct.get("mongo_update_avg_ms")},
        ],
        color="#0f766e",
    )

    for idx, point in enumerate(workloads):
        logical_tp = logical_series[idx] if idx < len(logical_series) else "-"
        direct_sql_tp = direct_sql_series[idx] if idx < len(direct_sql_series) else "-"
        direct_mongo_tp = direct_mongo_series[idx] if idx < len(direct_mongo_series) else "-"
        workload_rows.append(
            f"<tr><td>{_safe(point)}</td><td>{_safe(logical_tp)}</td><td>{_safe(direct_sql_tp)}</td><td>{_safe(direct_mongo_tp)}</td></tr>"
        )

    chart_block = ""
    if bar_img:
        chart_block += f"<div class='card'><h3>Latency Comparison (Bar Chart)</h3><img alt='Latency bar chart' src='{bar_img}' style='max-width:100%;height:auto;' /></div>"
    if line_img:
        chart_block += f"<div class='card'><h3>Throughput Under Workload (Line Graph)</h3><img alt='Throughput line chart' src='{line_img}' style='max-width:100%;height:auto;' /></div>"

    body = f"""
<div class='card'>
    <h2>Hybrid Framework vs Direct SQL vs Direct MongoDB</h2>
    <p class='muted'>Separate scenario tables compare framework against Direct SQL and Direct MongoDB using query latency, update latency, system throughput, and framework query-processing overhead.</p>
</div>

<div class='card'>
    <h3>Retrieving user records through the logical query interface vs direct SQL queries</h3>
    <table>
        <thead>
            <tr>
                <th>Metric</th>
                <th>Framework</th>
                <th>Direct SQL</th>
                <th>Direct MongoDB</th>
                <th>Framework Overhead vs Direct (%)</th>
            </tr>
        </thead>
        <tbody>{read_metrics_rows}</tbody>
    </table>
</div>

<div class='card'>
    <h3>Accessing nested documents using the framework vs direct MongoDB queries</h3>
    <table>
        <thead>
            <tr>
                <th>Metric</th>
                <th>Framework</th>
                <th>Direct SQL</th>
                <th>Direct MongoDB</th>
                <th>Framework Overhead vs Direct (%)</th>
            </tr>
        </thead>
        <tbody>{nested_metrics_rows}</tbody>
    </table>
</div>

<div class='card'>
    <h3>Updating records across multiple entities</h3>
    <table>
        <thead>
            <tr>
                <th>Metric</th>
                <th>Framework</th>
                <th>Direct SQL</th>
                <th>Direct MongoDB</th>
                <th>Framework Overhead vs Direct (%)</th>
            </tr>
        </thead>
        <tbody>{update_metrics_rows}</tbody>
    </table>
    <p class='muted'>Direct update latency is now tracked separately for SQL and MongoDB execution paths.</p>
</div>

<div class='card'>
    <h3>Bar Charts Comparing Query Latency</h3>
    <p class='muted'>Visual comparison for user retrieval and nested-document scenarios.</p>
    {query_latency_bars}
</div>

<div class='card'>
    <h3>Bar Chart Comparing Update Latency</h3>
    <p class='muted'>Visual comparison for cross-entity update scenario.</p>
    {update_latency_bars}
</div>

<div class='card'>
    <h3>Throughput Table (Increasing Workload)</h3>
    <table>
        <thead><tr><th>Workload Point</th><th>Framework (ops/sec)</th><th>Direct SQL (ops/sec)</th><th>Direct MongoDB (ops/sec)</th></tr></thead>
        <tbody>{''.join(workload_rows) if workload_rows else "<tr><td colspan='4' class='muted'>No throughput points found.</td></tr>"}</tbody>
    </table>
    <p class='muted'>Overall throughput: framework={_safe(logical.get('throughput_ops_per_sec'))} ops/sec, direct SQL={_safe(direct_sql_tp_overall)} ops/sec, direct MongoDB={_safe(direct_mongo_tp_overall)} ops/sec.</p>
</div>

{chart_block}
"""
    return _html_page("Performance Comparison", body)


@app.get("/acid", response_class=HTMLResponse)
def acid_report() -> HTMLResponse:
    report = _acid_report()
    atomicity = report.get("atomicity", {})
    consistency = report.get("consistency", {})
    isolation = report.get("isolation", {})
    durability = report.get("durability", {})

    fk_report = consistency.get("fk_violations", {})
    consistency_missing = int(fk_report.get("total_missing", 0) or 0)
    isolation_ok = bool(isolation.get("ok"))
    durability_ok = bool(durability.get("sql_tables", {}).get("ok")) and bool(durability.get("mongo_collections", {}).get("ok"))
    isolation_note = "Meets the required policy." if isolation_ok else "Does not meet the required policy."
    consistency_note = "All logical relationships passed validation." if consistency.get("status") == "pass" else f"Validation found {consistency_missing} issue(s)."
    durability_note = "Logical data snapshot is available." if durability_ok else "One or more logical data sources are unavailable."

    body = f"""
<div class='card'>
    <h2>Validation Report</h2>
    <p class='muted'>Snapshot based on the current logical data state.</p>
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
    <p class='muted'>{_safe(consistency_note)}</p>
</div>

<div class='card'>
    <h3>Isolation</h3>
    <p>Status: <span class='badge {'success' if isolation_ok else 'failed'}'>
        {_safe('pass' if isolation_ok else 'fail')}</span></p>
    <p class='muted'>{_safe(isolation_note)}</p>
</div>

<div class='card'>
    <h3>Durability</h3>
    <p>Status: <span class='badge {'success' if durability_ok else 'failed'}'>
        {_safe('pass' if durability_ok else 'fail')}</span></p>
    <p class='muted'>{_safe(durability_note)}</p>
</div>
"""
    return _html_page("ACID Report", body)


@app.get("/connections", response_class=HTMLResponse)
def connection_page() -> HTMLResponse:
    results = _test_connections()
    mysql = results.get("mysql", {})
    mongo = results.get("mongo", {})
    primary_ok = bool(mysql.get("ok"))
    secondary_ok = bool(mongo.get("ok"))
    body = f"""
<div class='card'>
    <h2>Service Check</h2>
    <p>Status:</p>
    <ul>
        <li>Logical service A: <strong>{'OK' if primary_ok else 'FAILED'}</strong>{' connection unavailable' if not primary_ok else ''}</li>
        <li>Logical service B: <strong>{'OK' if secondary_ok else 'FAILED'}</strong>{' connection unavailable' if not secondary_ok else ''}</li>
    </ul>
    <p class='muted'>Refresh this page to re-run the checks.</p>
</div>
"""
    return _html_page("Service Check", body)


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
