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


class CRUDQueryEngine:
    """Plans CRUD (starting with READ) operations based on registry metadata."""

    SUPPORTED_OPERATIONS = {"read"}

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
        if operation not in self.SUPPORTED_OPERATIONS:
            raise ValueError(f"Operation '{operation}' is not supported yet")

        fields = request.get("fields") or []
        filters = request.get("filters") or {}
        limit = request.get("limit")

        schema = self.registry.get_schema(schema_id)
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        storage_strategy = schema.get("storage_strategy") or {}
        field_map = self._build_field_map(storage_strategy)
        table_map = self._build_table_map(blueprint)

        field_locations: List[FieldLocation] = []
        sql_requirements: Dict[str, Set[str]] = {}
        mongo_requirements: Dict[str, Set[str]] = {}

        for field in fields:
            location = self._locate_field(field, field_map, table_map)
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

    # ------------------------------------------------------------------
    # Field location helpers
    # ------------------------------------------------------------------
    def _build_field_map(self, storage_strategy: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
        mappings = storage_strategy.get("mappings", {}).get("fields", [])
        index: Dict[str, List[Dict[str, Any]]] = {}
        for entry in mappings:
            field_path = entry.get("field_path")
            if not field_path:
                continue
            keys = {field_path.lower(), field_path.split(".")[-1].lower()}
            for key in keys:
                index.setdefault(key, []).append(entry)
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
                collection=chosen.get("collection"),
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
        where_clause, parameters = self._build_where_clause(filters, field_map)

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
    ) -> Tuple[Optional[str], Dict[str, Any]]:
        if not filters:
            return None, {}

        clauses: List[str] = []
        parameters: Dict[str, Any] = {}
        for raw_field, value in filters.items():
            entries = field_map.get(raw_field.lower())
            if not entries:
                continue
            chosen = self._prefer_exact(raw_field, entries)
            table = chosen.get("table")
            column = chosen.get("column")
            if not table or not column:
                continue
            param_name = f"param_{len(parameters) + 1}"
            clauses.append(f"{table}.{column} = :{param_name}")
            parameters[param_name] = value

        return (" AND ".join(clauses) if clauses else None, parameters)

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


__all__ = ["CRUDQueryEngine"]
