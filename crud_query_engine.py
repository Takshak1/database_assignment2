"""Step 9: Automatic CRUD query planning utilities."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple, Set

from schema_registry import SchemaRegistry
from metadata_manager import MetadataManager


@dataclass
class FieldLocation:
    """Represents how a logical field maps to physical storage."""

    requested: str
    resolved: Optional[str]
    storage: str
    table: Optional[str]
    column: Optional[str]
    collection: Optional[str]
    status: str
    notes: str
    related_columns: Optional[List[str]] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "requested": self.requested,
            "resolved": self.resolved,
            "storage": self.storage,
            "table": self.table,
            "column": self.column,
            "collection": self.collection,
            "status": self.status,
            "notes": self.notes,
            "related_columns": self.related_columns,
        }


class ParameterList(list):
    def values(self) -> List[Any]:
        return list(self)


class CRUDQueryEngine:
    """Plans CRUD operations based on registry metadata."""

    SUPPORTED_OPERATIONS = {"read", "insert", "update", "delete", "create"}

    def __init__(
        self,
        *,
        registry: Optional[SchemaRegistry] = None,
        metadata_file: str = "metadata.json",
    ) -> None:
        self.registry = registry or SchemaRegistry()
        self.metadata_manager = MetadataManager(metadata_file)
        self.structural_index = self._build_structural_index()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def plan_query(self, schema_id: int, request: Dict[str, Any]) -> Dict[str, Any]:
        operation = (request.get("operation") or "read").lower()
        if operation == "create":
            operation = "insert"
        if operation not in self.SUPPORTED_OPERATIONS:
            raise ValueError(f"Operation '{operation}' is not supported yet")

        schema = self.registry.get_schema(schema_id)
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        storage_strategy = schema.get("storage_strategy") or {}
        mongo_strategy = schema.get("mongo_strategy") or {}
        field_map = self._build_field_map(storage_strategy, mongo_strategy)
        table_map = self._build_table_map(blueprint)

        if operation == "insert":
            payload = request.get("payload") or {}
            return self._plan_insert(
                schema_id=schema_id,
                payload=payload,
                storage_strategy=storage_strategy,
                blueprint=blueprint,
            )

        if operation == "update":
            payload = request.get("payload") or {}
            filters = self._normalize_filters(request.get("filters") or {}, schema)
            strategy = (request.get("strategy") or "simple").lower()
            return self._plan_update(
                schema_id=schema_id,
                payload=payload,
                filters=filters,
                strategy=strategy,
                storage_strategy=storage_strategy,
                blueprint=blueprint,
            )

        if operation == "delete":
            filters = self._normalize_filters(request.get("filters") or {}, schema)
            strategy = (request.get("strategy") or "entity").lower()
            return self._plan_delete(
                schema_id=schema_id,
                filters=filters,
                strategy=strategy,
                storage_strategy=storage_strategy,
                blueprint=blueprint,
            )

        fields = request.get("fields") or []
        filters = self._normalize_filters(request.get("filters") or {}, schema)
        limit = request.get("limit")

        field_locations: List[FieldLocation] = []
        sql_requirements: Dict[str, Set[str]] = {}
        mongo_requirements: Dict[str, Set[str]] = {}

        for field in fields:
            location = self._locate_field(field, field_map, table_map)
            if location.storage == "sql" and (not location.table or (not location.column and not location.related_columns)):
                fallback_table, fallback_column = self._resolve_sql_column_from_table_map(field, table_map)
                if fallback_table:
                    if (not location.table) or (location.table not in table_map) or (not location.column):
                        location.table = fallback_table
                    if not location.column:
                        location.column = fallback_column
            field_locations.append(location)
            if location.storage == "sql" and location.table:
                columns = location.related_columns or ([location.column] if location.column else [])
                if columns:
                    sql_requirements.setdefault(location.table, set()).update(
                        col for col in columns if col
                    )
            elif location.storage == "mongo" and location.collection:
                mongo_requirements.setdefault(location.collection, set()).add(location.resolved or field)

        sql_plan = self._build_sql_plan(
            blueprint=blueprint,
            tables_to_columns=sql_requirements,
            filters=filters,
            field_map=field_map,
            limit=limit,
        ) if sql_requirements else None

        mongo_plan = self._build_mongo_plan(mongo_requirements, filters, field_map)

        merge_plan = self._build_merge_plan(fields, field_locations, blueprint)

        return {
            "schema_id": schema_id,
            "operation": operation,
            "field_locations": [loc.to_dict() for loc in field_locations],
            "sql": sql_plan,
            "mongo": mongo_plan,
            "merge": merge_plan,
        }

    def _normalize_filters(self, filters: Dict[str, Any], schema: Dict[str, Any]) -> Dict[str, Any]:
        if not isinstance(filters, dict) or not filters:
            return {}
        normalized = dict(filters)
        entity_name = str(schema.get("entity_name") or "").strip().lower()
        if entity_name:
            entity_scoped_id = f"{entity_name}_id"
            if entity_scoped_id in normalized and "id" not in normalized:
                normalized["id"] = normalized[entity_scoped_id]
        return normalized

    # ------------------------------------------------------------------
    # Field location helpers
    # ------------------------------------------------------------------
    def _build_field_map(
        self,
        storage_strategy: Dict[str, Any],
        mongo_strategy: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, List[Dict[str, Any]]]:
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        index: Dict[str, List[Dict[str, Any]]] = {}
        for entry in mappings:
            field_path = entry.get("field_path")
            if not field_path:
                continue
            keys = {field_path.lower(), field_path.split(".")[-1].lower()}
            for key in keys:
                index.setdefault(key, []).append(entry)

        for entry in (mongo_strategy or {}).get("entries", []):
            field_path = entry.get("field_path")
            if not field_path:
                continue
            mongo_entry = dict(entry)
            mongo_entry.setdefault("decision", "mongo")
            mongo_entry.setdefault("collection", entry.get("target_collection"))
            keys = {field_path.lower(), field_path.split(".")[-1].lower()}
            for key in keys:
                index.setdefault(key, []).append(mongo_entry)
        return index

    def _build_table_map(self, blueprint: Optional[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        if not blueprint:
            return {}
        return {table["name"]: table for table in blueprint.get("tables", [])}

    def _locate_field(
        self,
        field: str,
        field_map: Dict[str, List[Dict[str, Any]]],
        table_map: Dict[str, Dict[str, Any]],
    ) -> FieldLocation:
        key = (field or "").lower()
        entries = field_map.get(key)
        if not entries:
            entries = self._find_partial_field(field, field_map)

        if entries:
            chosen = self._prefer_exact(field, entries)
            decision = (chosen.get("decision") or "sql").lower()
            if decision in {"embed", "reference"}:
                decision = "mongo"
            table = chosen.get("table")
            column = chosen.get("column")
            related_columns: Optional[List[str]] = None
            notes = "exact_match" if chosen.get("field_path", "").lower() == key else "partial_match"

            if not column and table and table in table_map:
                related_columns = self._non_pk_columns(table_map[table])
                notes = "table_scope"

            return FieldLocation(
                requested=field,
                resolved=chosen.get("field_path"),
                storage=decision,
                table=table,
                column=column,
                collection=chosen.get("collection") or chosen.get("target_collection"),
                status="resolved",
                notes=notes,
                related_columns=related_columns,
            )

        table_name, table_info = self._match_table(field, table_map)
        if table_info:
            return FieldLocation(
                requested=field,
                resolved=table_name,
                storage="sql",
                table=table_name,
                column=None,
                collection=None,
                status="resolved",
                notes="table_scope",
                related_columns=self._non_pk_columns(table_info),
            )

        metadata_hint = self._metadata_hint(field)
        if metadata_hint:
            storage = metadata_hint.get("storage_engine", "unknown").lower()
            table = metadata_hint.get("table_or_collection")
            columns = metadata_hint.get("related_columns")
            return FieldLocation(
                requested=field,
                resolved=metadata_hint.get("field_path") or field,
                storage=storage,
                table=table if storage == "sql" else None,
                column=None,
                collection=table if storage == "mongo" else None,
                status="hint",
                notes="metadata_hint",
                related_columns=columns,
            )

        return FieldLocation(
            requested=field,
            resolved=None,
            storage="unknown",
            table=None,
            column=None,
            collection=None,
            status="missing",
            notes="field_not_found",
        )

    def _find_partial_field(
        self,
        field: str,
        field_map: Dict[str, List[Dict[str, Any]]],
    ) -> Optional[List[Dict[str, Any]]]:
        suffix = f".{field.lower()}"
        matches: List[Dict[str, Any]] = []
        for path, entries in field_map.items():
            for entry in entries:
                field_path = (entry.get("field_path") or "").lower()
                if field_path.endswith(suffix) or field_path == suffix.strip('.'):
                    matches.append(entry)
        return matches or None

    def _prefer_exact(self, field: str, entries: List[Dict[str, Any]]):
        key = field.lower()
        exact = [entry for entry in entries if (entry.get("field_path") or "").lower() == key]
        if exact:
            mongo_like = [
                entry
                for entry in exact
                if (entry.get("decision") or "").lower() in {"mongo", "embed", "reference"}
            ]
            if mongo_like:
                return mongo_like[0]
            return exact[0]
        for entry in entries:
            if (entry.get("field_path") or "").lower() == key:
                return entry
        return entries[0]

    def _non_pk_columns(self, table: Dict[str, Any]) -> List[str]:
        pk = table.get("primary_key")
        columns = []
        for column in table.get("columns", []):
            name = column.get("name")
            if name and name != pk:
                columns.append(name)
        return columns

    def _metadata_hint(self, field: str) -> Optional[Dict[str, Any]]:
        if not self.structural_index:
            return None
        key = field.lower()
        direct = self.structural_index.get(key)
        if direct:
            return direct
        for path, entry in self.structural_index.items():
            if path.endswith(f".{key}"):
                return entry
        return None

    def _match_table(
        self,
        field: str,
        table_map: Dict[str, Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[Dict[str, Any]]]:
        field_lower = field.lower()
        for name, table in table_map.items():
            if name.lower() == field_lower:
                return name, table
        return None, None

    def _resolve_sql_column_from_table_map(
        self,
        field: str,
        table_map: Dict[str, Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        lookup = (field or "").strip().lower()
        if not lookup:
            return None, None

        candidates: List[Tuple[str, str]] = []
        for table_name, table in table_map.items():
            for column in table.get("columns", []):
                col_name = column.get("name")
                if col_name and str(col_name).lower() == lookup:
                    candidates.append((table_name, col_name))

        if not candidates:
            return None, None
        return candidates[0]

    def _build_structural_index(self) -> Dict[str, Dict[str, Any]]:
        registry = self.metadata_manager.get_structural_registry()
        index = {}
        for entry in registry:
            field_path = (entry.get("field_path") or entry.get("field") or "").lower()
            if field_path:
                index[field_path] = entry
        return index

    # ------------------------------------------------------------------
    # SQL planning helpers
    # ------------------------------------------------------------------
    def _build_sql_plan(
        self,
        *,
        blueprint: Optional[Dict[str, Any]],
        tables_to_columns: Dict[str, Set[str]],
        filters: Dict[str, Any],
        field_map: Dict[str, List[Dict[str, Any]]],
        limit: Optional[int],
    ) -> Optional[Dict[str, Any]]:
        if not blueprint or not tables_to_columns:
            return None

        relationships = blueprint.get("relationships", [])
        root_table = blueprint.get("root_table")
        tables_needed = set(tables_to_columns.keys())

        base_table = root_table if root_table in tables_needed else next(iter(tables_needed))
        select_clauses = self._build_select_list(tables_to_columns)
        joins = self._generate_joins(base_table, tables_needed, relationships)
        where_clause, parameters = self._build_where_clause(filters, field_map, blueprint)

        query = f"SELECT {', '.join(select_clauses)} FROM {base_table}"
        if joins:
            query += " " + " ".join(join["clause"] for join in joins)
        if where_clause:
            query += f" WHERE {where_clause}"
        if limit:
            query += f" LIMIT {int(limit)}"

        return {
            "tables": sorted(tables_needed),
            "select": select_clauses,
            "base_table": base_table,
            "joins": joins,
            "where": where_clause,
            "parameters": parameters,
            "limit": limit,
            "statement": query,
        }

    def _build_select_list(self, tables_to_columns: Dict[str, Set[str]]) -> List[str]:
        select_clauses: List[str] = []
        for table, columns in tables_to_columns.items():
            for column in sorted(columns):
                alias = f"{table}_{column}"
                select_clauses.append(f"{table}.{column} AS {alias}")
        return select_clauses or ["*"]

    def _generate_joins(
        self,
        base_table: str,
        tables_needed: Set[str],
        relationships: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        if len(tables_needed) <= 1:
            return []

        included = {base_table}
        remaining = set(tables_needed) - included
        joins: List[Dict[str, Any]] = []

        while remaining:
            progress = False
            for relation in relationships:
                child = relation.get("from_table")
                parent = relation.get("to_table")
                if child in remaining and parent in included:
                    joins.append(
                        {
                            "type": "inner",
                            "clause": (
                                f"JOIN {child} ON {child}.{relation['from_column']} = "
                                f"{parent}.{relation['to_column']}"
                            ),
                            "relationship": relation,
                        }
                    )
                    included.add(child)
                    remaining.remove(child)
                    progress = True
                elif parent in remaining and child in included:
                    joins.append(
                        {
                            "type": "inner",
                            "clause": (
                                f"JOIN {parent} ON {parent}.{relation['to_column']} = "
                                f"{child}.{relation['from_column']}"
                            ),
                            "relationship": relation,
                        }
                    )
                    included.add(parent)
                    remaining.remove(parent)
                    progress = True
            if not progress:
                # Cannot resolve all joins with available relationships
                break
        return joins

    def _build_where_clause(
        self,
        filters: Dict[str, Any],
        field_map: Dict[str, List[Dict[str, Any]]],
        blueprint: Optional[Dict[str, Any]] = None,
    ) -> Tuple[Optional[str], ParameterList]:
        if not filters:
            return None, ParameterList()

        clauses: List[str] = []
        parameters: ParameterList = ParameterList()
        seen_filters: Set[Tuple[str, str, Any]] = set()
        for raw_field, value in filters.items():
            entries = field_map.get(raw_field.lower())
            table = None
            column = None
            if entries:
                chosen = self._prefer_exact(raw_field, entries)
                table = chosen.get("table")
                column = chosen.get("column")
            else:
                table, column = self._resolve_filter_from_blueprint(raw_field, blueprint)
            if not table or not column:
                continue
            signature = (str(table), str(column), value)
            if signature in seen_filters:
                continue
            seen_filters.add(signature)
            clauses.append(f"{table}.{column} = %s")
            parameters.append(value)

        return (" AND ".join(clauses) if clauses else None, parameters)

    def _resolve_filter_from_blueprint(
        self,
        raw_field: str,
        blueprint: Optional[Dict[str, Any]],
    ) -> Tuple[Optional[str], Optional[str]]:
        if not blueprint:
            return None, None

        lookup = (raw_field or "").strip().lower()
        if not lookup:
            return None, None

        candidates: List[Tuple[str, str]] = []
        for table in blueprint.get("tables", []):
            table_name = table.get("name")
            if not table_name:
                continue
            for column in table.get("columns", []):
                col_name = column.get("name")
                if col_name and str(col_name).lower() == lookup:
                    candidates.append((table_name, col_name))

        if not candidates:
            return None, None
        if len(candidates) == 1:
            return candidates[0]

        root_table = blueprint.get("root_table")
        for table_name, column_name in candidates:
            if table_name == root_table:
                return table_name, column_name
        return candidates[0]

    # ------------------------------------------------------------------
    # Mongo planning helpers
    # ------------------------------------------------------------------
    def _build_mongo_plan(
        self,
        collection_map: Dict[str, Set[str]],
        filters: Dict[str, Any],
        field_map: Dict[str, List[Dict[str, Any]]],
    ) -> List[Dict[str, Any]]:
        if not collection_map:
            return []

        plan: List[Dict[str, Any]] = []
        for collection, fields in collection_map.items():
            projection = {field: 1 for field in sorted(fields)}
            filter_doc = self._build_mongo_filter(filters, field_map, collection)
            plan.append(
                {
                    "collection": collection,
                    "filter": filter_doc,
                    "projection": projection,
                    "statement": f"db.{collection}.find({filter_doc or {}}, {projection})",
                }
            )
        return plan

    def _build_mongo_filter(
        self,
        filters: Dict[str, Any],
        field_map: Dict[str, List[Dict[str, Any]]],
        target_collection: str,
    ) -> Optional[Dict[str, Any]]:
        if not filters:
            return None
        filter_doc = {}
        for raw_field, value in filters.items():
            entries = field_map.get(raw_field.lower())
            if not entries:
                continue
            for entry in entries:
                if entry.get("collection") == target_collection:
                    filter_doc[entry.get("field_path", raw_field)] = value
                    break
        return filter_doc or None

    # ------------------------------------------------------------------
    # Merge planning
    # ------------------------------------------------------------------
    def _build_merge_plan(
        self,
        requested_fields: List[str],
        field_locations: List[FieldLocation],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        sql_fields = [loc for loc in field_locations if loc.storage == "sql" and loc.status != "missing"]
        mongo_fields = [loc for loc in field_locations if loc.storage == "mongo" and loc.status != "missing"]

        merge_key = self._infer_merge_key(sql_fields, mongo_fields, blueprint)
        response_shape = self._example_response_shape(requested_fields, sql_fields, mongo_fields)

        return {
            "strategy": "client_side_join" if mongo_fields else "sql_result",
            "merge_key": merge_key,
            "notes": (
                "Align SQL rows with Mongo documents using merge key before returning JSON"
                if mongo_fields
                else "Result emitted directly from SQL SELECT output"
            ),
            "response_shape": response_shape,
        }

    def _infer_merge_key(
        self,
        sql_fields: List[FieldLocation],
        mongo_fields: List[FieldLocation],
        blueprint: Optional[Dict[str, Any]],
    ) -> Optional[str]:
        preferred = {"username", "user_id", "_id"}
        for loc in sql_fields + mongo_fields:
            if (loc.resolved or loc.requested).split(".")[-1] in preferred:
                return loc.resolved or loc.requested
        if blueprint:
            root_table = blueprint.get("root_table")
            for table in blueprint.get("tables", []):
                if table.get("name") == root_table:
                    return f"{root_table}.{table.get('primary_key')}"
        return None

    def _example_response_shape(
        self,
        requested_fields: List[str],
        sql_fields: List[FieldLocation],
        mongo_fields: List[FieldLocation],
    ) -> Dict[str, Any]:
        sample = {}
        for loc in sql_fields:
            key = loc.requested
            sample[key] = f"{loc.table}.{loc.column or 'record'}"
        for loc in mongo_fields:
            key = loc.requested
            sample[key] = f"mongo:{loc.collection}.{loc.resolved}"
        return {
            "type": "object" if sample else "list",
            "example": sample,
            "requested_fields": requested_fields,
        }

    # ------------------------------------------------------------------
    # Write planning (insert / update / delete)
    # ------------------------------------------------------------------
    def _plan_insert(
        self,
        *,
        schema_id: int,
        payload: Dict[str, Any],
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        sql_plan = self._plan_sql_inserts(payload, storage_strategy, blueprint)
        mongo_plan = self._plan_mongo_docs(payload, storage_strategy)
        return {
            "schema_id": schema_id,
            "operation": "insert",
            "sql": sql_plan,
            "mongo": mongo_plan,
            "consistency": {
                "join_keys": sql_plan.get("foreign_keys", {}),
                "metadata_source": "schema_storage_strategies",
            },
        }

    def _plan_update(
        self,
        *,
        schema_id: int,
        payload: Dict[str, Any],
        filters: Dict[str, Any],
        strategy: str,
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if strategy == "simple":
            delete_plan = self._plan_delete(
                schema_id=schema_id,
                filters=filters,
                strategy="entity",
                storage_strategy=storage_strategy,
                blueprint=blueprint,
            )
            insert_plan = self._plan_insert(
                schema_id=schema_id,
                payload=payload,
                storage_strategy=storage_strategy,
                blueprint=blueprint,
            )
            return {
                "schema_id": schema_id,
                "operation": "update",
                "strategy": "simple",
                "delete": delete_plan,
                "insert": insert_plan,
                "consistency": {
                    "mode": "delete_then_insert",
                    "notes": "Maintains schema consistency by replaying normalized insert flow",
                },
            }

        sql_updates, mongo_updates = self._plan_advanced_updates(payload, storage_strategy)
        return {
            "schema_id": schema_id,
            "operation": "update",
            "strategy": "advanced",
            "filters": filters,
            "sql": sql_updates,
            "mongo": mongo_updates,
            "consistency": {
                "mode": "targeted_update",
                "notes": "Applies field-level updates using metadata mappings",
            },
        }

    def _plan_delete(
        self,
        *,
        schema_id: int,
        filters: Dict[str, Any],
        strategy: str,
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        effective_filters = filters if strategy != "sub-entity" else (filters.get("criteria") or {})
        if strategy == "sub-entity":
            target = filters.get("target")
            sql_plan = self._plan_subentity_delete(target, blueprint)
            mongo_plan = self._plan_subentity_mongo_delete(target, storage_strategy)
        else:
            sql_plan = self._plan_entity_delete(blueprint)
            mongo_plan = self._plan_entity_mongo_delete(storage_strategy)

        return {
            "schema_id": schema_id,
            "operation": "delete",
            "strategy": strategy,
            "filters": effective_filters,
            "sql": sql_plan,
            "mongo": mongo_plan,
            "consistency": {
                "cascade": strategy != "sub-entity",
                "notes": "Delete order is child-to-parent for SQL and mapped collections for Mongo",
            },
        }

    def _plan_sql_inserts(
        self,
        payload: Dict[str, Any],
        storage_strategy: Dict[str, Any],
        blueprint: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if not blueprint:
            return {"tables": [], "rows": {}, "foreign_keys": {}}

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
                    field_path = mapping.get("field_path")
                    if not column:
                        continue
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
            decision = (mapping.get("decision") or "sql").lower()
            if decision in {"embed", "reference"}:
                decision = "mongo"
            if decision != "mongo":
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

    def _plan_advanced_updates(
        self,
        payload: Dict[str, Any],
        storage_strategy: Dict[str, Any],
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        sql_updates: Dict[str, Dict[str, Any]] = {}
        mongo_updates: Dict[str, Dict[str, Any]] = {}
        for mapping in mappings:
            decision = (mapping.get("decision") or "sql").lower()
            if decision in {"embed", "reference"}:
                decision = "mongo"
            field_path = mapping.get("field_path")
            if not field_path:
                continue
            value = self._resolve_value(payload, field_path)
            if value is None:
                continue
            if decision == "sql":
                table = mapping.get("table")
                column = mapping.get("column")
                if table and column:
                    sql_updates.setdefault(table, {})[column] = value
            elif decision == "mongo":
                collection = mapping.get("collection") or mapping.get("target_collection")
                if collection:
                    mongo_updates.setdefault(collection, {})[field_path] = value

        return (
            [{"table": table, "set": columns} for table, columns in sql_updates.items()],
            [{"collection": collection, "set": fields} for collection, fields in mongo_updates.items()],
        )

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

    def _plan_entity_mongo_delete(self, storage_strategy: Dict[str, Any]) -> Dict[str, Any]:
        collections = set()
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            decision = (mapping.get("decision") or "sql").lower()
            if decision in {"embed", "reference"}:
                decision = "mongo"
            if decision == "mongo":
                collections.add(mapping.get("collection") or mapping.get("target_collection"))
        return {"collections": sorted(c for c in collections if c)}

    def _plan_subentity_mongo_delete(self, target: Optional[str], storage_strategy: Dict[str, Any]) -> Dict[str, Any]:
        if not target:
            return {"collections": []}
        collections = set()
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            collection = mapping.get("collection") or mapping.get("target_collection")
            table = mapping.get("table")
            if target in {collection, table} and collection:
                collections.add(collection)
        if collections:
            return {"collections": sorted(collections)}
        return {"collections": [target]}

    # ------------------------------------------------------------------
    # Shared write helpers
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
        visiting = set()

        def visit(table: str) -> None:
            if table in visited:
                return
            if table in visiting:
                # Break cyclic FK chains by stopping the current DFS branch.
                return
            visiting.add(table)
            parent = self._parent_table(table, relationships)
            if parent and parent != table:
                visit(parent)
            visiting.remove(table)
            visited.add(table)
            order.append(table)

        for table in tables:
            visit(table)
        if root and root in order and not self._parent_table(root, relationships):
            order.remove(root)
            order.insert(0, root)
        return order

    def _parent_table(self, table: str, relationships: List[Dict[str, Any]]) -> Optional[str]:
        for relation in relationships:
            if relation.get("from_table") == table:
                parent = relation.get("to_table")
                if parent == table:
                    continue
                return parent
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


__all__ = ["CRUDQueryEngine"]
