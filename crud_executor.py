"""Hybrid CRUD executor that operates across SQL and Mongo backends (Step 10)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:  
    from dotenv import load_dotenv
except Exception:  
    def load_dotenv() -> bool:
        return False

from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from result_aggregator import ResultAggregator
from metadata_manager import MetadataManager

load_dotenv()

DEFAULT_MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", ""),
    "database": os.getenv("MYSQL_DATABASE", "streaming_db"),
}

DEFAULT_MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "database": os.getenv("MONGO_DATABASE", "streaming_db"),
    "collection": os.getenv("MONGO_COLLECTION", "logs"),
}

try:  
    import mysql.connector as mysql_connector
except Exception:  
    mysql_connector = None  

try:  
    from pymongo import MongoClient
except Exception:  
    MongoClient = None  


@dataclass
class CRUDResult:
    """Container describing the outcome of a CRUD operation."""

    operation: str
    executed: bool
    details: Dict[str, Any]

    def to_dict(self) -> Dict[str, Any]:
        return {
            "operation": self.operation,
            "executed": self.executed,
            "details": self.details,
        }


class HybridCRUDExecutor:
    """Routes CRUD requests to SQL/Mongo using registry + metadata intelligence."""

    def __init__(
        self,
        *,
        registry: Optional[SchemaRegistry] = None,
        metadata_file: str = "metadata.json",
        mysql_config: Optional[Dict[str, Any]] = None,
        mongo_config: Optional[Dict[str, Any]] = None,
        aggregator: Optional[ResultAggregator] = None,
    ) -> None:
        self.registry = registry or SchemaRegistry()
        self.metadata_manager = MetadataManager(metadata_file)
        self.query_engine = CRUDQueryEngine(
            registry=self.registry,
            metadata_file=metadata_file,
        )
        self.aggregator = aggregator or ResultAggregator(registry=self.registry)
        self.mysql_config = mysql_config or DEFAULT_MYSQL_CONFIG
        self.mongo_config = mongo_config or DEFAULT_MONGO_CONFIG

    
    def execute(
        self,
        schema_id: int,
        *,
        operation: str,
        payload: Optional[Dict[str, Any]] = None,
        fields: Optional[List[str]] = None,
        filters: Optional[Dict[str, Any]] = None,
        strategy: str = "simple",
        execute: bool = False,
        limit: Optional[int] = None,
    ) -> CRUDResult:
        self._sync_metadata_from_schema(schema_id)

        op = (operation or "read").lower()
        if op == "insert":
            return CRUDResult(
                operation="insert",
                executed=execute,
                details=self._handle_insert(schema_id, payload or {}, execute=execute),
            )
        if op == "read":
            return CRUDResult(
                operation="read",
                executed=execute,
                details=self._handle_read(
                    schema_id,
                    fields=fields or [],
                    filters=filters or {},
                    limit=limit,
                    execute=execute,
                ),
            )
        if op == "update":
            return CRUDResult(
                operation="update",
                executed=execute,
                details=self._handle_update(
                    schema_id,
                    payload or {},
                    filters or {},
                    strategy=strategy,
                    execute=execute,
                ),
            )
        if op == "delete":
            return CRUDResult(
                operation="delete",
                executed=execute,
                details=self._handle_delete(
                    schema_id,
                    filters or {},
                    strategy=strategy,
                    execute=execute,
                ),
            )
        raise ValueError(f"Unsupported CRUD operation: {operation}")

    def _sync_metadata_from_schema(self, schema_id: int) -> None:
        """Populate metadata.json from schema registry entries used by CRUD flows."""
        try:
            schema = self.registry.get_schema(schema_id)
        except Exception:
            return

        fields = schema.get("fields") or []
        analysis_entries = (schema.get("analysis") or {}).get("entries") or []
        analysis_by_path = {
            (entry.get("field_path") or "").replace("[]", ""): entry
            for entry in analysis_entries
            if entry.get("field_path")
        }

        analyzer_stats = {"total": len(fields) or 1}

        for field in fields:
            field_name = field.get("field_name")
            if not field_name:
                continue

            parent = (field.get("parent_field") or "").replace("[]", "")
            field_path = f"{parent}.{field_name}" if parent else field_name
            entry = analysis_by_path.get(field_path) or analysis_by_path.get(field_name) or {}

            dtype = str(field.get("data_type") or "string").lower()
            if dtype.startswith("array"):
                type_token = "list"
            elif dtype in {"integer", "int", "bigint"}:
                type_token = "int"
            elif dtype in {"number", "float", "double", "decimal"}:
                type_token = "float"
            elif dtype in {"boolean", "bool"}:
                type_token = "bool"
            elif dtype in {"object", "json", "jsonb"}:
                type_token = "dict"
            else:
                type_token = "str"

            stats = {
                "freq": 1.0,
                "types": {type_token},
                "unique_count": 1 if field.get("is_unique") else 0,
                "uniqueness_ratio": 1.0 if field.get("is_unique") else 0.0,
                "is_unique_field": bool(field.get("is_unique")),
                "nested": bool(field.get("nesting_level", 0) > 0),
                "composite_score": 0.8,
                "stability": 1.0,
                "semantic_info": {
                    "detected_kind": "unknown",
                    "semantic_weight": 0.0,
                    "avg_length": 0,
                    "max_length": 0,
                    "is_long_text": False,
                },
                "ambiguity_info": {"ambiguity_score": 0.0},
                "has_type_ambiguity": False,
                "drift_analysis": {"drift_score": 0.0, "drift_history": []},
                "should_quarantine": False,
                "quarantine_reason": "stable",
                "nesting_level": int(field.get("nesting_level") or 0),
                "parent_field": parent or None,
                "is_array": bool(field.get("is_array")),
            }

            placement = (entry.get("pipeline") or "sql").lower()
            placement_info = {
                "decision": placement,
                "reason": entry.get("pipeline_reason") or "schema_registry_sync",
                "confidence": float(entry.get("pipeline_confidence") or 1.0),
                "signals": {
                    "source": "schema_registry",
                    "schema_id": schema_id,
                },
            }

            self.metadata_manager.update_field_metadata(field_path, stats, placement_info, analyzer_stats)

        self.metadata_manager.save_metadata()

    # ------------------------------------------------------------------
    # Insert flow
    # ------------------------------------------------------------------
    def _handle_insert(self, schema_id: int, payload: Dict[str, Any], *, execute: bool) -> Dict[str, Any]:
        if os.getenv("AUTO_EXTEND_SCHEMA", "1").strip().lower() in {"1", "true", "yes", "on"}:
            self.registry.refresh_schema_with_sample(schema_id, payload)
        plan = self.query_engine.plan_query(
            schema_id,
            {
                "operation": "insert",
                "payload": payload,
            },
        )
        sql_plan = plan.get("sql") or {"order": [], "rows": {}, "foreign_keys": {}}
        mongo_plan = plan.get("mongo") or {"collections": {}}

        auto_create_hint = self._auto_create_sql_tables(schema_id)

        if not execute:
            return {
                "plan": plan,
                "sql": sql_plan,
                "mongo": mongo_plan,
                "auto_create_sql": auto_create_hint,
                "note": "Set execute=true to run inserts against live databases",
            }

        if auto_create_hint.get("attempted") and auto_create_hint.get("errors"):
            return {
                "plan": plan,
                "sql": sql_plan,
                "mongo": mongo_plan,
                "auto_create_sql": auto_create_hint,
                "error": "SQL table auto-creation failed; insert aborted.",
            }

        if self._transaction_enabled():
            return self._execute_transactional_insert(sql_plan, mongo_plan)

        mysql_result = self._execute_sql_inserts(sql_plan)
        mongo_result = self._execute_mongo_inserts(mongo_plan)
        return {
            "sql": mysql_result,
            "mongo": mongo_result,
        }

    def _auto_create_sql_tables(self, schema_id: int) -> Dict[str, Any]:
        enabled = os.getenv("AUTO_CREATE_SQL_ON_INSERT", "1").strip().lower() in {"1", "true", "yes", "on"}
        if not enabled:
            return {"attempted": False, "created": 0, "altered": 0, "errors": []}

        if mysql_connector is None:
            return {
                "attempted": True,
                "created": 0,
                "altered": 0,
                "errors": ["mysql-connector-python is not available"],
            }

        schema = self.registry.get_schema(schema_id)
        sql_section = (schema.get("storage_strategy") or {}).get("sql", {})
        commands = sql_section.get("commands", [])
        tables = sql_section.get("tables", [])
        if not commands or not tables:
            return {
                "attempted": True,
                "created": 0,
                "altered": 0,
                "errors": ["No SQL DDL commands available"],
            }

        table_names = [table.get("name") for table in tables if table.get("name")]
        conn = mysql_connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        created = 0
        altered = 0
        errors: List[str] = []
        try:
            existing = set()
            cursor.execute("SHOW TABLES")
            for row in cursor.fetchall():
                existing.add(row[0])

            for table_name, command in zip(table_names, commands):
                if table_name in existing:
                    altered += self._ensure_sql_columns(cursor, table_name, tables, errors)
                    continue
                try:
                    cursor.execute(command)
                    created += 1
                except Exception as exc:  # pragma: no cover
                    errors.append(str(exc))
            conn.commit()
        finally:
            cursor.close()
            conn.close()

        return {"attempted": True, "created": created, "altered": altered, "errors": errors}

    def _ensure_sql_columns(
        self,
        cursor: Any,
        table_name: str,
        tables_meta: List[Dict[str, Any]],
        errors: List[str],
    ) -> int:
        auto_alter = os.getenv("AUTO_ALTER_SQL", "1").strip().lower() in {"1", "true", "yes", "on"}
        if not auto_alter:
            return 0

        table_meta = next((table for table in tables_meta if table.get("name") == table_name), None)
        if not table_meta:
            return 0

        try:
            cursor.execute(f"DESCRIBE `{table_name}`")
            existing_cols = {row[0] for row in cursor.fetchall()}
        except Exception as exc:  # pragma: no cover
            errors.append(str(exc))
            return 0

        altered = 0
        for column in table_meta.get("columns", []):
            column_name = column.get("name")
            if not column_name or column_name in existing_cols:
                continue
            definition = self._column_definition_sql(column)
            if not definition:
                continue
            statement = f"ALTER TABLE `{table_name}` ADD COLUMN {definition}"
            try:
                cursor.execute(statement)
                altered += 1
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))
        return altered

    def _column_definition_sql(self, column: Dict[str, Any]) -> Optional[str]:
        name = column.get("name")
        col_type = column.get("type")
        if not name or not col_type:
            return None
        parts = [f"`{name}`", str(col_type)]
        if not column.get("nullable", True):
            parts.append("NOT NULL")
        for constraint in column.get("constraints", []) or []:
            if constraint.upper() == "PRIMARY KEY":
                continue
            parts.append(constraint)
        return " ".join(parts)

    def _plan_sql_inserts(
        self,
        payload: Dict[str, Any],
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not blueprint:
            return {"tables": [], "rows": {}}

        table_map = {table["name"]: table for table in blueprint.get("tables", [])}
        relationships = blueprint.get("relationships", [])
        table_order = self._table_insertion_order(blueprint)
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        grouped = self._group_mappings_by_table(mappings)

        rows: Dict[str, List[Dict[str, Any]]] = {}
        for table_name in table_order:
            table = table_map.get(table_name)
            table_mappings = grouped.get(table_name, [])
            anchor_path = self._find_anchor_path(table)
            source_records = self._source_records(payload, anchor_path)
            table_rows: List[Dict[str, Any]] = []
            for record in source_records:
                row: Dict[str, Any] = {}
                for mapping in table_mappings:
                    column = mapping.get("column")
                    if not column:
                        continue
                    field_path = mapping.get("field_path")
                    value = self._resolve_relative_value(record, field_path, anchor_path)
                    if value is not None:
                        row[column] = value
                table_rows.append(row)
            rows[table_name] = table_rows or [{}]

        fk_hints = self._build_fk_hints(relationships)
        return {
            "order": table_order,
            "rows": rows,
            "foreign_keys": fk_hints,
        }

    def _plan_mongo_docs(self, payload: Dict[str, Any], storage_strategy: Dict[str, Any]) -> Dict[str, Any]:
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        docs: Dict[str, Dict[str, Any]] = {}
        for mapping in mappings:
            if (mapping.get("decision") or "sql").lower() != "mongo":
                continue
            collection = mapping.get("collection") or mapping.get("target_collection")
            field_path = mapping.get("field_path")
            if not collection or not field_path:
                continue
            value = self._resolve_value(payload, field_path)
            if value is None:
                continue
            docs.setdefault(collection, {})
            self._assign_nested_value(docs[collection], field_path.split("."), value)
        return {"collections": docs}

    def _execute_sql_inserts(
        self,
        plan: Dict[str, Any],
        *,
        conn: Optional[Any] = None,
        cursor: Optional[Any] = None,
        commit: bool = True,
    ) -> Dict[str, Any]:
        if not plan.get("order"):
            return {"rows_inserted": 0, "details": []}
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        created_conn = False
        created_cursor = False
        if conn is None:
            conn = mysql_connector.connect(**self.mysql_config)
            created_conn = True
        if cursor is None:
            cursor = conn.cursor()
            created_cursor = True
        inserted_keys: Dict[str, List[int]] = {}
        details: List[Dict[str, Any]] = []
        try:
            for table in plan["order"]:
                table_rows = plan["rows"].get(table, [])
                fk_hint = plan["foreign_keys"].get(table)
                inserted_keys.setdefault(table, [])
                for row in table_rows:
                    row_to_insert = dict(row)
                    if fk_hint:
                        parent_table = fk_hint["parent_table"]
                        parent_column = fk_hint["from_column"]
                        parent_keys = inserted_keys.get(parent_table) or []
                        parent_key = parent_keys[-1] if parent_keys else None
                        if parent_key is not None:
                            row_to_insert[parent_column] = parent_key
                    columns = list(row_to_insert.keys())
                    if not columns:
                        statement = f"INSERT INTO {table} () VALUES ()"
                        values: List[Any] = []
                        cursor.execute(statement)
                    else:
                        values = [row_to_insert[col] for col in columns]
                        placeholders = ", ".join(["%s"] * len(columns))
                        statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                        cursor.execute(statement, values)
                    inserted_id = cursor.lastrowid
                    inserted_keys[table].append(inserted_id)
                    details.append({
                        "table": table,
                        "statement": statement,
                        "values": values,
                        "inserted_id": inserted_id,
                    })
            if commit:
                conn.commit()
        finally:
            if created_cursor:
                cursor.close()
            if created_conn:
                conn.close()
        total_rows = sum(len(v) for v in inserted_keys.values())
        return {"rows_inserted": total_rows, "details": details}

    def _execute_mongo_inserts(
        self,
        plan: Dict[str, Any],
        *,
        client: Optional[Any] = None,
        session: Optional[Any] = None,
    ) -> Dict[str, Any]:
        collections = plan.get("collections") or {}
        if not collections:
            return {"documents_inserted": 0, "details": []}
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        created_client = False
        if client is None:
            uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
            client = MongoClient(uri)
            created_client = True
        db = client[self.mongo_config['database']]
        details: List[Dict[str, Any]] = []
        try:
            for collection, document in collections.items():
                result = db[collection].insert_one(document, session=session)
                details.append({
                    "collection": collection,
                    "document": document,
                    "inserted_id": str(result.inserted_id),
                    "raw_id": result.inserted_id,
                })
        finally:
            if created_client:
                client.close()
        return {"documents_inserted": len(details), "details": details}

 
    def _handle_read(
        self,
        schema_id: int,
        *,
        fields: List[str],
        filters: Dict[str, Any],
        limit: Optional[int],
        execute: bool,
    ) -> Dict[str, Any]:
        plan = self.query_engine.plan_query(
            schema_id,
            {
                "operation": "read",
                "fields": fields,
                "filters": filters,
                "limit": limit,
            },
        )
        if not execute:
            plan["note"] = "Set execute=true to fetch live data"
            return plan

        sql_rows = self._execute_sql_select(plan["sql"]) if plan.get("sql") else []
        mongo_rows = self._execute_mongo_reads(plan.get("mongo", [])) if plan.get("mongo") else []
        merged = self.aggregator.aggregate(
            schema_id,
            sql_rows=sql_rows,
            mongo_rows=mongo_rows,
            merge_plan=plan.get("merge"),
        )
        plan["results"] = merged
        plan["result_summary"] = {
            "sql_rows": len(sql_rows),
            "mongo_documents": len(mongo_rows),
            "merged_items": len(merged),
        }
        return plan

    def _execute_sql_select(self, sql_plan: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not sql_plan:
            return []
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        conn = mysql_connector.connect(**self.mysql_config)
        cursor = conn.cursor(dictionary=True)
        try:
            statement = sql_plan.get("statement")
            params = sql_plan.get("parameters", [])
            cursor.execute(statement, params)
            rows = cursor.fetchall()
        finally:
            cursor.close()
            conn.close()
        return rows

    def _execute_mongo_reads(self, mongo_plan: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not mongo_plan:
            return []
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
        client = MongoClient(uri)
        db = client[self.mongo_config['database']]
        results: List[Dict[str, Any]] = []
        try:
            for item in mongo_plan:
                collection = item.get("collection")
                projection = item.get("projection") or None
                filter_doc = item.get("filter") or {}
                docs = list(db[collection].find(filter_doc, projection))
                for doc in docs:
                    doc["_collection"] = collection
                    results.append(doc)
        finally:
            client.close()
        return results


    def _handle_update(
        self,
        schema_id: int,
        payload: Dict[str, Any],
        filters: Dict[str, Any],
        *,
        strategy: str,
        execute: bool,
    ) -> Dict[str, Any]:
        if os.getenv("AUTO_EXTEND_SCHEMA", "1").strip().lower() in {"1", "true", "yes", "on"}:
            self.registry.refresh_schema_with_sample(schema_id, payload)
        auto_create_hint = self._auto_create_sql_tables(schema_id)
        update_plan = self.query_engine.plan_query(
            schema_id,
            {
                "operation": "update",
                "payload": payload,
                "filters": filters,
                "strategy": strategy,
            },
        )

        if not execute:
            update_plan["auto_create_sql"] = auto_create_hint

        if execute and auto_create_hint.get("attempted") and auto_create_hint.get("errors"):
            return {
                "strategy": strategy,
                "plan": update_plan,
                "auto_create_sql": auto_create_hint,
                "error": "SQL table auto-creation failed; update aborted.",
            }

        if strategy == "simple":
            delete_plan = update_plan.get("delete", {})
            insert_plan = update_plan.get("insert", {})
            if execute and self._transaction_enabled():
                return self._execute_transactional_simple_update(delete_plan, insert_plan, filters)
            delete_result = self._handle_delete(
                schema_id,
                delete_plan.get("filters") or filters,
                strategy="entity",
                execute=execute,
            )
            insert_result = self._handle_insert(
                schema_id,
                payload,
                execute=execute,
            )
            return {
                "strategy": "simple",
                "plan": update_plan,
                "auto_create_sql": auto_create_hint,
                "delete": delete_result,
                "insert": insert_result,
            }

        sql_updates = update_plan.get("sql") or []
        mongo_updates = update_plan.get("mongo") or []
        if not execute:
            return {
                "strategy": "advanced",
                "plan": update_plan,
                "sql": sql_updates,
                "mongo": mongo_updates,
            }
        if self._transaction_enabled():
            return self._execute_transactional_update(sql_updates, mongo_updates, filters)
        sql_result = self._execute_sql_updates(sql_updates, filters)
        mongo_result = self._execute_mongo_updates(mongo_updates, filters)
        return {
            "strategy": "advanced",
            "sql": sql_result,
            "mongo": mongo_result,
        }

    def _plan_advanced_updates(
        self,
        payload: Dict[str, Any],
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        sql_updates: Dict[str, Dict[str, Any]] = {}
        mongo_updates: Dict[str, Dict[str, Any]] = {}
        for mapping in mappings:
            decision = (mapping.get("decision") or "sql").lower()
            field_path = mapping.get("field_path")
            if not field_path:
                continue
            value = self._resolve_value(payload, field_path)
            if value is None:
                continue
            if decision == "sql":
                table = mapping.get("table")
                column = mapping.get("column")
                if not table or not column:
                    continue
                sql_updates.setdefault(table, {})[column] = value
            elif decision == "mongo":
                collection = mapping.get("collection")
                if not collection:
                    continue
                mongo_updates.setdefault(collection, {})[field_path] = value
        sql_plan = [
            {
                "table": table,
                "set": columns,
            }
            for table, columns in sql_updates.items()
        ]
        mongo_plan = [
            {
                "collection": collection,
                "set": fields,
            }
            for collection, fields in mongo_updates.items()
        ]
        return sql_plan, mongo_plan

    def _execute_sql_updates(
        self,
        plan: List[Dict[str, Any]],
        filters: Dict[str, Any],
        *,
        conn: Optional[Any] = None,
        cursor: Optional[Any] = None,
        commit: bool = True,
    ) -> List[Dict[str, Any]]:
        if not plan:
            return []
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        created_conn = False
        created_cursor = False
        if conn is None:
            conn = mysql_connector.connect(**self.mysql_config)
            created_conn = True
        if cursor is None:
            cursor = conn.cursor()
            created_cursor = True
        results: List[Dict[str, Any]] = []
        try:
            for item in plan:
                table = item["table"]
                set_clause = ", ".join([f"{col} = %s" for col in item["set"]])
                values = list(item["set"].values())
                where_clause = self._build_simple_where(filters)
                statement = f"UPDATE {table} SET {set_clause} {where_clause['clause']}"
                cursor.execute(statement, values + where_clause["values"])
                results.append({
                    "table": table,
                    "statement": statement,
                    "affected": cursor.rowcount,
                })
            if commit:
                conn.commit()
        finally:
            if created_cursor:
                cursor.close()
            if created_conn:
                conn.close()
        return results

    def _execute_mongo_updates(self, plan: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not plan:
            return []
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
        client = MongoClient(uri)
        db = client[self.mongo_config['database']]
        results: List[Dict[str, Any]] = []
        try:
            for item in plan:
                collection = item["collection"]
                update_doc = {"$set": {}}  
                for field_path, value in item["set"].items():
                    update_doc["$set"][field_path] = value
                result = db[collection].update_many(filters or {}, update_doc)
                results.append({
                    "collection": collection,
                    "matched": result.matched_count,
                    "modified": result.modified_count,
                })
        finally:
            client.close()
        return results

    def _handle_delete(
        self,
        schema_id: int,
        filters: Dict[str, Any],
        *,
        strategy: str,
        execute: bool,
    ) -> Dict[str, Any]:
        delete_plan = self.query_engine.plan_query(
            schema_id,
            {
                "operation": "delete",
                "filters": filters,
                "strategy": strategy,
            },
        )
        effective_filters = delete_plan.get("filters") or {}
        sql_plan = delete_plan.get("sql") or {"tables": []}
        mongo_plan = delete_plan.get("mongo") or {"collections": []}

        if not execute:
            return {
                "strategy": strategy,
                "plan": delete_plan,
                "sql": sql_plan,
                "mongo": mongo_plan,
                "note": "Set execute=true to run deletes",
            }

        if self._transaction_enabled():
            return self._execute_transactional_delete(sql_plan, mongo_plan, effective_filters, strategy)

        sql_result = self._execute_sql_deletes(sql_plan, effective_filters)
        mongo_result = self._execute_mongo_deletes(mongo_plan, effective_filters)
        return {
            "strategy": strategy,
            "sql": sql_result,
            "mongo": mongo_result,
        }

    def _plan_entity_delete(self, blueprint: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not blueprint:
            return {"tables": []}
        order = list(reversed(self._table_insertion_order(blueprint)))
        return {"tables": order}

    def _plan_subentity_delete(self, target: Optional[str], blueprint: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not target or not blueprint:
            return {"tables": []}
        table_map = {table["name"]: table for table in blueprint.get("tables", [])}
        if target not in table_map:
            return {"tables": []}
        return {"tables": [target]}

    def _execute_sql_deletes(
        self,
        plan: Dict[str, Any],
        filters: Dict[str, Any],
        *,
        conn: Optional[Any] = None,
        cursor: Optional[Any] = None,
        commit: bool = True,
    ) -> List[Dict[str, Any]]:
        tables = plan.get("tables") or []
        if not tables:
            return []
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        created_conn = False
        created_cursor = False
        if conn is None:
            conn = mysql_connector.connect(**self.mysql_config)
            created_conn = True
        if cursor is None:
            cursor = conn.cursor()
            created_cursor = True
        results: List[Dict[str, Any]] = []
        where = self._build_simple_where(filters)
        try:
            for table in tables:
                statement = f"DELETE FROM {table} {where['clause']}"
                cursor.execute(statement, where["values"])
                results.append({
                    "table": table,
                    "statement": statement,
                    "deleted": cursor.rowcount,
                })
            if commit:
                conn.commit()
        finally:
            if created_cursor:
                cursor.close()
            if created_conn:
                conn.close()
        return results

    def _plan_entity_mongo_delete(self, storage_strategy: Dict[str, Any]) -> Dict[str, Any]:
        collections = set()
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            if (mapping.get("decision") or "sql").lower() == "mongo":
                collections.add(mapping.get("collection") or mapping.get("target_collection"))
        return {"collections": sorted(c for c in collections if c)}

    def _plan_subentity_mongo_delete(self, target: Optional[str], storage_strategy: Dict[str, Any]) -> Dict[str, Any]:
        if not target:
            return {"collections": []}
        return {"collections": [target]}

    def _execute_mongo_deletes(self, plan: Dict[str, Any], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        collections = plan.get("collections") or []
        if not collections:
            return []
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
        client = MongoClient(uri)
        db = client[self.mongo_config['database']]
        results: List[Dict[str, Any]] = []
        try:
            for collection in collections:
                result = db[collection].delete_many(filters or {})
                results.append({
                    "collection": collection,
                    "deleted": result.deleted_count,
                })
        finally:
            client.close()
        return results

    # ------------------------------------------------------------------
    # Transaction coordination
    # ------------------------------------------------------------------
    def _transaction_enabled(self) -> bool:
        return os.getenv("TRANSACTION_COORDINATION", "1").strip().lower() in {"1", "true", "yes", "on"}

    def _execute_transactional_insert(self, sql_plan: Dict[str, Any], mongo_plan: Dict[str, Any]) -> Dict[str, Any]:
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        sql_conn = mysql_connector.connect(**self.mysql_config)
        sql_cursor = sql_conn.cursor()
        sql_conn.start_transaction()

        mongo_client = None
        mongo_session = None
        mongo_in_txn = False
        mongo_result: Dict[str, Any] = {"documents_inserted": 0, "details": []}

        try:
            if mongo_plan.get("collections"):
                mongo_client = self._create_mongo_client()
                mongo_session, mongo_in_txn = self._start_mongo_transaction(mongo_client)

            sql_result = self._execute_sql_inserts(sql_plan, conn=sql_conn, cursor=sql_cursor, commit=False)
            if mongo_plan.get("collections"):
                mongo_result = self._execute_mongo_inserts(
                    mongo_plan,
                    client=mongo_client,
                    session=mongo_session,
                )

            if mongo_in_txn and mongo_session is not None:
                mongo_session.commit_transaction()
            sql_conn.commit()
        except Exception:
            sql_conn.rollback()
            if mongo_in_txn and mongo_session is not None:
                self._safe_abort_mongo_transaction(mongo_session)
            else:
                self._rollback_mongo_inserts(mongo_client, mongo_result)
            raise
        finally:
            if mongo_session is not None:
                mongo_session.end_session()
            if mongo_client is not None:
                mongo_client.close()
            sql_cursor.close()
            sql_conn.close()

        return {"sql": sql_result, "mongo": mongo_result}

    def _execute_transactional_update(
        self,
        sql_plan: List[Dict[str, Any]],
        mongo_plan: List[Dict[str, Any]],
        filters: Dict[str, Any],
    ) -> Dict[str, Any]:
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        sql_conn = mysql_connector.connect(**self.mysql_config)
        sql_cursor = sql_conn.cursor()
        sql_conn.start_transaction()

        mongo_client = None
        mongo_session = None
        mongo_in_txn = False
        mongo_backups: List[Dict[str, Any]] = []
        mongo_result: List[Dict[str, Any]] = []

        try:
            if mongo_plan:
                mongo_client = self._create_mongo_client()
                mongo_session, mongo_in_txn = self._start_mongo_transaction(mongo_client)

            sql_result = self._execute_sql_updates(sql_plan, filters, conn=sql_conn, cursor=sql_cursor, commit=False)
            if mongo_plan:
                mongo_result, mongo_backups = self._execute_mongo_updates_transactional(
                    mongo_client,
                    mongo_plan,
                    filters,
                    mongo_session,
                    mongo_in_txn,
                )

            if mongo_in_txn and mongo_session is not None:
                mongo_session.commit_transaction()
            sql_conn.commit()
        except Exception:
            sql_conn.rollback()
            if mongo_in_txn and mongo_session is not None:
                self._safe_abort_mongo_transaction(mongo_session)
            else:
                self._rollback_mongo_updates(mongo_client, mongo_backups)
            raise
        finally:
            if mongo_session is not None:
                mongo_session.end_session()
            if mongo_client is not None:
                mongo_client.close()
            sql_cursor.close()
            sql_conn.close()

        return {
            "strategy": "advanced",
            "sql": sql_result,
            "mongo": mongo_result,
        }

    def _execute_transactional_delete(
        self,
        sql_plan: Dict[str, Any],
        mongo_plan: Dict[str, Any],
        filters: Dict[str, Any],
        strategy: str,
    ) -> Dict[str, Any]:
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        sql_conn = mysql_connector.connect(**self.mysql_config)
        sql_cursor = sql_conn.cursor()
        sql_conn.start_transaction()

        mongo_client = None
        mongo_session = None
        mongo_in_txn = False
        mongo_backups: List[Dict[str, Any]] = []
        mongo_result: List[Dict[str, Any]] = []

        try:
            if mongo_plan.get("collections"):
                mongo_client = self._create_mongo_client()
                mongo_session, mongo_in_txn = self._start_mongo_transaction(mongo_client)

            sql_result = self._execute_sql_deletes(sql_plan, filters, conn=sql_conn, cursor=sql_cursor, commit=False)
            if mongo_plan.get("collections"):
                mongo_result, mongo_backups = self._execute_mongo_deletes_transactional(
                    mongo_client,
                    mongo_plan,
                    filters,
                    mongo_session,
                    mongo_in_txn,
                )

            if mongo_in_txn and mongo_session is not None:
                mongo_session.commit_transaction()
            sql_conn.commit()
        except Exception:
            sql_conn.rollback()
            if mongo_in_txn and mongo_session is not None:
                self._safe_abort_mongo_transaction(mongo_session)
            else:
                self._rollback_mongo_deletes(mongo_client, mongo_backups)
            raise
        finally:
            if mongo_session is not None:
                mongo_session.end_session()
            if mongo_client is not None:
                mongo_client.close()
            sql_cursor.close()
            sql_conn.close()

        return {
            "strategy": strategy,
            "sql": sql_result,
            "mongo": mongo_result,
        }

    def _execute_transactional_simple_update(
        self,
        delete_plan: Dict[str, Any],
        insert_plan: Dict[str, Any],
        filters: Dict[str, Any],
    ) -> Dict[str, Any]:
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        sql_conn = mysql_connector.connect(**self.mysql_config)
        sql_cursor = sql_conn.cursor()
        sql_conn.start_transaction()

        mongo_client = None
        mongo_session = None
        mongo_in_txn = False
        mongo_backups: List[Dict[str, Any]] = []
        mongo_delete_result: List[Dict[str, Any]] = []
        mongo_insert_result: Dict[str, Any] = {"documents_inserted": 0, "details": []}

        sql_delete_plan = delete_plan.get("sql") or {"tables": []}
        mongo_delete_plan = delete_plan.get("mongo") or {"collections": []}
        sql_insert_plan = insert_plan.get("sql") or {"order": [], "rows": {}, "foreign_keys": {}}
        mongo_insert_plan = insert_plan.get("mongo") or {"collections": {}}
        effective_filters = delete_plan.get("filters") or filters

        try:
            if mongo_delete_plan.get("collections") or mongo_insert_plan.get("collections"):
                mongo_client = self._create_mongo_client()
                mongo_session, mongo_in_txn = self._start_mongo_transaction(mongo_client)

            sql_delete_result = self._execute_sql_deletes(
                sql_delete_plan,
                effective_filters,
                conn=sql_conn,
                cursor=sql_cursor,
                commit=False,
            )
            if mongo_delete_plan.get("collections"):
                mongo_delete_result, mongo_backups = self._execute_mongo_deletes_transactional(
                    mongo_client,
                    mongo_delete_plan,
                    effective_filters,
                    mongo_session,
                    mongo_in_txn,
                )

            sql_insert_result = self._execute_sql_inserts(
                sql_insert_plan,
                conn=sql_conn,
                cursor=sql_cursor,
                commit=False,
            )
            if mongo_insert_plan.get("collections"):
                mongo_insert_result = self._execute_mongo_inserts(
                    mongo_insert_plan,
                    client=mongo_client,
                    session=mongo_session,
                )

            if mongo_in_txn and mongo_session is not None:
                mongo_session.commit_transaction()
            sql_conn.commit()
        except Exception:
            sql_conn.rollback()
            if mongo_in_txn and mongo_session is not None:
                self._safe_abort_mongo_transaction(mongo_session)
            else:
                self._rollback_mongo_inserts(mongo_client, mongo_insert_result)
                self._rollback_mongo_deletes(mongo_client, mongo_backups)
            raise
        finally:
            if mongo_session is not None:
                mongo_session.end_session()
            if mongo_client is not None:
                mongo_client.close()
            sql_cursor.close()
            sql_conn.close()

        return {
            "strategy": "simple",
            "delete": {"sql": sql_delete_result, "mongo": mongo_delete_result},
            "insert": {"sql": sql_insert_result, "mongo": mongo_insert_result},
        }

    def _create_mongo_client(self) -> Any:
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
        return MongoClient(uri)

    def _start_mongo_transaction(self, client: Any) -> Tuple[Optional[Any], bool]:
        try:
            session = client.start_session()
        except Exception:  # pragma: no cover - depends on runtime
            return None, False
        try:
            session.start_transaction()
            return session, True
        except Exception:  # pragma: no cover - depends on runtime
            session.end_session()
            return None, False

    def _safe_abort_mongo_transaction(self, session: Any) -> None:
        try:
            session.abort_transaction()
        except Exception:  # pragma: no cover - best effort
            pass

    def _execute_mongo_updates_transactional(
        self,
        client: Any,
        plan: List[Dict[str, Any]],
        filters: Dict[str, Any],
        session: Optional[Any],
        in_transaction: bool,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        db = client[self.mongo_config["database"]]
        results: List[Dict[str, Any]] = []
        backups: List[Dict[str, Any]] = []
        for item in plan:
            collection = item["collection"]
            update_doc = {"$set": dict(item.get("set", {}))}
            if not in_transaction:
                snapshot = list(db[collection].find(filters or {}))
                backups.append({"collection": collection, "filters": filters, "docs": snapshot})
            result = db[collection].update_many(filters or {}, update_doc, session=session)
            results.append({
                "collection": collection,
                "matched": result.matched_count,
                "modified": result.modified_count,
            })
        return results, backups

    def _execute_mongo_deletes_transactional(
        self,
        client: Any,
        plan: Dict[str, Any],
        filters: Dict[str, Any],
        session: Optional[Any],
        in_transaction: bool,
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        db = client[self.mongo_config["database"]]
        results: List[Dict[str, Any]] = []
        backups: List[Dict[str, Any]] = []
        for collection in plan.get("collections") or []:
            if not in_transaction:
                snapshot = list(db[collection].find(filters or {}))
                backups.append({"collection": collection, "filters": filters, "docs": snapshot})
            result = db[collection].delete_many(filters or {}, session=session)
            results.append({
                "collection": collection,
                "deleted": result.deleted_count,
            })
        return results, backups

    def _rollback_mongo_inserts(self, client: Optional[Any], result: Dict[str, Any]) -> None:
        if client is None:
            return
        details = result.get("details") or []
        if not details:
            return
        db = client[self.mongo_config["database"]]
        try:
            from bson import ObjectId  # type: ignore
        except Exception:  # pragma: no cover - optional dependency
            ObjectId = None  # type: ignore
        for item in details:
            collection = item.get("collection")
            raw_id = item.get("raw_id") or item.get("inserted_id")
            if not collection or raw_id is None:
                continue
            identifier = raw_id
            if ObjectId is not None and isinstance(raw_id, str):
                try:
                    identifier = ObjectId(raw_id)
                except Exception:
                    identifier = raw_id
            db[collection].delete_one({"_id": identifier})

    def _rollback_mongo_updates(self, client: Optional[Any], backups: List[Dict[str, Any]]) -> None:
        if client is None or not backups:
            return
        db = client[self.mongo_config["database"]]
        for backup in backups:
            collection = backup.get("collection")
            docs = backup.get("docs") or []
            filters = backup.get("filters") or {}
            if not collection:
                continue
            db[collection].delete_many(filters)
            if docs:
                db[collection].insert_many(docs)

    def _rollback_mongo_deletes(self, client: Optional[Any], backups: List[Dict[str, Any]]) -> None:
        if client is None or not backups:
            return
        db = client[self.mongo_config["database"]]
        for backup in backups:
            collection = backup.get("collection")
            docs = backup.get("docs") or []
            if not collection or not docs:
                continue
            db[collection].insert_many(docs)

    # ------------------------------------------------------------------
    # Helper utilities
    def _group_mappings_by_table(self, mappings: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for mapping in mappings:
            table = mapping.get("table")
            if not table:
                continue
            grouped.setdefault(table, []).append(mapping)
        return grouped

    def _find_anchor_path(self, table: Optional[Dict[str, Any]]) -> Optional[str]:
        if not table:
            return None
        table_name = table.get("name")
        sources = table.get("sources", [])
        if table_name and table_name in sources:
            return table_name
        for source in sources:
            if source and "." not in source:
                return source
        return None

    def _source_records(self, payload: Dict[str, Any], anchor: Optional[str]) -> List[Any]:
        if not anchor:
            return [payload]
        value = self._resolve_value(payload, anchor)
        if value is None:
            return [payload]
        if isinstance(value, list):
            return value
        return [value]

    def _resolve_relative_value(
        self,
        source: Any,
        field_path: Optional[str],
        anchor_path: Optional[str],
    ) -> Any:
        if not field_path:
            return None
        relative = field_path
        if anchor_path and field_path.startswith(anchor_path + "."):
            relative = field_path[len(anchor_path) + 1 :]
        return self._resolve_value(source, relative)

    def _resolve_value(self, data: Any, path: Optional[str]) -> Any:
        if path is None or path == "":
            return data
        tokens = [token for token in path.split(".") if token]
        return self._resolve_tokens(data, tokens)

    def _resolve_tokens(self, current: Any, tokens: List[str]) -> Any:
        if not tokens:
            return current
        if current is None:
            return None
        token = tokens[0]
        rest = tokens[1:]
        if isinstance(current, list):
            aggregated: List[Any] = []
            for item in current:
                value = self._resolve_tokens(item, tokens)
                if isinstance(value, list):
                    aggregated.extend(value)
                elif value is not None:
                    aggregated.append(value)
            return aggregated
        if isinstance(current, dict):
            return self._resolve_tokens(current.get(token), rest)
        return None

    def _assign_nested_value(self, document: Dict[str, Any], path_tokens: List[str], value: Any) -> None:
        target = document
        for token in path_tokens[:-1]:
            target = target.setdefault(token, {})
        target[path_tokens[-1]] = value

    def _table_insertion_order(self, blueprint: Dict[str, Any]) -> List[str]:
        tables = [table["name"] for table in blueprint.get("tables", [])]
        root = blueprint.get("root_table") or (tables[0] if tables else None)
        relationships = blueprint.get("relationships", [])
        order: List[str] = []
        visited = set()

        def visit(table: str) -> None:
            if table in visited:
                return
            parent = self._parent_table(table, relationships)
            if parent:
                visit(parent)
            visited.add(table)
            order.append(table)

        for table in tables:
            visit(table)
        if root and root in order:
            order.remove(root)
            order.insert(0, root)
        return order

    def _parent_table(self, table: str, relationships: List[Dict[str, Any]]) -> Optional[str]:
        for relation in relationships:
            if relation.get("from_table") == table:
                return relation.get("to_table")
        return None

    def _build_fk_hints(self, relationships: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        hints: Dict[str, Dict[str, Any]] = {}
        for relation in relationships:
            hints[relation["from_table"]] = {
                "parent_table": relation["to_table"],
                "from_column": relation["from_column"],
                "to_column": relation["to_column"],
            }
        return hints

    def _build_simple_where(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        if not filters:
            return {"clause": "", "values": []}
        clauses: List[str] = []
        values: List[Any] = []
        for column, value in filters.items():
            if column in {"target", "criteria"}:
                continue
            clauses.append(f"{column} = %s")
            values.append(value)
        clause = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        return {"clause": clause, "values": values}


__all__ = ["HybridCRUDExecutor", "CRUDResult"]