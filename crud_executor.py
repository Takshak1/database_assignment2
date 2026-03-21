"""Hybrid CRUD executor that operates across SQL and Mongo backends (Step 10)."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:  # pragma: no cover - optional dependency
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    def load_dotenv() -> bool:
        return False

from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from result_aggregator import ResultAggregator

load_dotenv()

DEFAULT_MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST", "localhost"),
    "user": os.getenv("MYSQL_USER", "root"),
    "password": os.getenv("MYSQL_PASSWORD", "devil"),
    "database": os.getenv("MYSQL_DATABASE", "streaming_db"),
}

DEFAULT_MONGO_CONFIG = {
    "host": os.getenv("MONGO_HOST", "localhost"),
    "port": int(os.getenv("MONGO_PORT", 27017)),
    "database": os.getenv("MONGO_DATABASE", "streaming_db"),
    "collection": os.getenv("MONGO_COLLECTION", "logs"),
}

try:  # pragma: no cover - optional dependency at runtime
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover
    mysql_connector = None  # type: ignore

try:  # pragma: no cover - optional dependency at runtime
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore


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
        self.query_engine = CRUDQueryEngine(
            registry=self.registry,
            metadata_file=metadata_file,
        )
        self.aggregator = aggregator or ResultAggregator(registry=self.registry)
        self.mysql_config = mysql_config or DEFAULT_MYSQL_CONFIG
        self.mongo_config = mongo_config or DEFAULT_MONGO_CONFIG

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
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

    # ------------------------------------------------------------------
    # Insert flow
    # ------------------------------------------------------------------
    def _handle_insert(self, schema_id: int, payload: Dict[str, Any], *, execute: bool) -> Dict[str, Any]:
        schema = self.registry.get_schema(schema_id)
        storage_strategy = schema.get("storage_strategy") or {}
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        sql_plan = self._plan_sql_inserts(payload, storage_strategy, blueprint)
        mongo_plan = self._plan_mongo_docs(payload, storage_strategy)

        if not execute:
            return {
                "sql": sql_plan,
                "mongo": mongo_plan,
                "note": "Set execute=true to run inserts against live databases",
            }

        mysql_result = self._execute_sql_inserts(sql_plan)
        mongo_result = self._execute_mongo_inserts(mongo_plan)
        return {
            "sql": mysql_result,
            "mongo": mongo_result,
        }

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

    def _execute_sql_inserts(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        if not plan.get("order"):
            return {"rows_inserted": 0, "details": []}
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        conn = mysql_connector.connect(**self.mysql_config)
        cursor = conn.cursor()
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
                        parent_key = inserted_keys.get(parent_table, [None])[-1]
                        if parent_key is not None:
                            row_to_insert[parent_column] = parent_key
                    columns = list(row_to_insert.keys())
                    if not columns:
                        continue
                    values = [row_to_insert[col] for col in columns]
                    placeholders = ", ".join(["%s"] * len(columns))
                    statement = f"INSERT INTO {table} ({', '.join(columns)}) VALUES ({placeholders})"
                    cursor.execute(statement, values)
                    conn.commit()
                    inserted_id = cursor.lastrowid
                    inserted_keys[table].append(inserted_id)
                    details.append({
                        "table": table,
                        "statement": statement,
                        "values": values,
                        "inserted_id": inserted_id,
                    })
        finally:
            cursor.close()
            conn.close()
        total_rows = sum(len(v) for v in inserted_keys.values())
        return {"rows_inserted": total_rows, "details": details}

    def _execute_mongo_inserts(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        collections = plan.get("collections") or {}
        if not collections:
            return {"documents_inserted": 0, "details": []}
        if MongoClient is None:
            raise RuntimeError("pymongo is not available")
        uri = f"mongodb://{self.mongo_config['host']}:{self.mongo_config['port']}/"
        client = MongoClient(uri)
        db = client[self.mongo_config['database']]
        details: List[Dict[str, Any]] = []
        try:
            for collection, document in collections.items():
                result = db[collection].insert_one(document)
                details.append({
                    "collection": collection,
                    "document": document,
                    "inserted_id": str(result.inserted_id),
                })
        finally:
            client.close()
        return {"documents_inserted": len(details), "details": details}

    # ------------------------------------------------------------------
    # Read flow
    # ------------------------------------------------------------------
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
            params = tuple(sql_plan.get("parameters", {}).values())
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


    # ------------------------------------------------------------------
    # Update / delete
    # ------------------------------------------------------------------
    def _handle_update(
        self,
        schema_id: int,
        payload: Dict[str, Any],
        filters: Dict[str, Any],
        *,
        strategy: str,
        execute: bool,
    ) -> Dict[str, Any]:
        if strategy == "simple":
            delete_result = self._handle_delete(schema_id, filters, strategy="entity", execute=execute)
            insert_result = self._handle_insert(schema_id, payload, execute=execute)
            return {
                "strategy": "simple",
                "delete": delete_result,
                "insert": insert_result,
            }

        # Advanced: generate targeted updates
        schema = self.registry.get_schema(schema_id)
        storage_strategy = schema.get("storage_strategy") or {}
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        sql_updates, mongo_updates = self._plan_advanced_updates(payload, storage_strategy, blueprint)
        if not execute:
            return {
                "strategy": "advanced",
                "sql": sql_updates,
                "mongo": mongo_updates,
            }
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

    def _execute_sql_updates(self, plan: List[Dict[str, Any]], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        if not plan:
            return []
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        conn = mysql_connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        results: List[Dict[str, Any]] = []
        try:
            for item in plan:
                table = item["table"]
                set_clause = ", ".join([f"{col} = %s" for col in item["set"]])
                values = list(item["set"].values())
                where_clause = self._build_simple_where(filters)
                statement = f"UPDATE {table} SET {set_clause} {where_clause['clause']}"
                cursor.execute(statement, values + where_clause["values"])
                conn.commit()
                results.append({
                    "table": table,
                    "statement": statement,
                    "affected": cursor.rowcount,
                })
        finally:
            cursor.close()
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
                update_doc = {"$set": {}}  # type: ignore
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
        schema = self.registry.get_schema(schema_id)
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        storage_strategy = schema.get("storage_strategy") or {}
        effective_filters = filters if strategy != "sub-entity" else filters.get("criteria", {})
        if strategy == "sub-entity":
            target = filters.get("target")
            sql_plan = self._plan_subentity_delete(target, blueprint)
            mongo_plan = self._plan_subentity_mongo_delete(target, storage_strategy)
        else:
            sql_plan = self._plan_entity_delete(blueprint)
            mongo_plan = self._plan_entity_mongo_delete(storage_strategy)

        if not execute:
            return {
                "strategy": strategy,
                "sql": sql_plan,
                "mongo": mongo_plan,
                "note": "Set execute=true to run deletes",
            }

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

    def _execute_sql_deletes(self, plan: Dict[str, Any], filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        tables = plan.get("tables") or []
        if not tables:
            return []
        if mysql_connector is None:
            raise RuntimeError("mysql-connector-python is not available")
        conn = mysql_connector.connect(**self.mysql_config)
        cursor = conn.cursor()
        results: List[Dict[str, Any]] = []
        where = self._build_simple_where(filters)
        try:
            for table in tables:
                statement = f"DELETE FROM {table} {where['clause']}"
                cursor.execute(statement, where["values"])
                conn.commit()
                results.append({
                    "table": table,
                    "statement": statement,
                    "deleted": cursor.rowcount,
                })
        finally:
            cursor.close()
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
    # Helper utilities
    # ------------------------------------------------------------------
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
        for source in table.get("sources", []):
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