"""Step 5 SQL Normalization Engine.

Takes analyzer entries and emits relational table blueprints.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Tuple


class SQLNormalizationEngine:
    """Generate relational table definitions from schema analysis entries."""

    def __init__(self) -> None:
        pass

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def generate_blueprint(
        self,
        *,
        entity_name: str,
        entries: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Return a relational blueprint derived from analyzer entries."""

        sanitized_root = self._tableize(entity_name or "entity")
        tables: Dict[str, Dict[str, Any]] = {}
        container_to_table: Dict[Optional[str], str] = {None: sanitized_root, "": sanitized_root}
        relationships: List[Dict[str, str]] = []
        rules_applied: Dict[str, List[str]] = {
            "arrays": [],
            "nested_objects": [],
            "root_entities": [sanitized_root],
        }

        # Ensure root table exists with PK
        self._ensure_table(
            tables=tables,
            table_name=sanitized_root,
            source_path=entity_name,
            reason="root_entity",
        )

        # Sort by nesting to ensure parents registered before children
        ordered_entries = sorted(entries, key=lambda e: e.get("nesting_level", 0))

        for entry in ordered_entries:
            field_path = entry.get("field_path")
            classification = (entry.get("classification") or "").lower()
            parent_path = entry.get("parent")
            nesting_level = int(entry.get("nesting_level", 0) or 0)
            pattern = (entry.get("pattern") or "").lower()

            if not field_path:
                continue

            if classification in {"repeating_entity"}:
                table_name = self._tableize(field_path.split(".")[-1])
                container_to_table[field_path] = table_name
                self._ensure_table(
                    tables=tables,
                    table_name=table_name,
                    source_path=field_path,
                    reason="array_of_objects",
                )
                relationships.append(
                    self._attach_foreign_key(
                        tables=tables,
                        child_table=table_name,
                        parent_table=self._resolve_parent_table(container_to_table, parent_path, sanitized_root),
                    )
                )
                rules_applied["arrays"].append(field_path)
                continue

            if classification in {"root_object", "nested_object"}:
                table_name = self._tableize(field_path.split(".")[-1])
                container_to_table[field_path] = table_name
                self._ensure_table(
                    tables=tables,
                    table_name=table_name,
                    source_path=field_path,
                    reason="nested_object",
                )
                relationships.append(
                    self._attach_foreign_key(
                        tables=tables,
                        child_table=table_name,
                        parent_table=self._resolve_parent_table(container_to_table, parent_path, sanitized_root),
                    )
                )
                rules_applied["nested_objects"].append(field_path)
                continue

            owning_table = self._resolve_parent_table(container_to_table, parent_path, sanitized_root)
            column_name = self._column_name(field_path)
            sql_type = self._map_sql_type(entry.get("data_type"))
            self._add_column(
                tables,
                table_name=owning_table,
                column_name=column_name,
                column_type=sql_type,
                source_path=field_path,
            )

        # Remove None placeholders from relationships
        relationships = [rel for rel in relationships if rel]

        blueprint_tables = [self._table_to_dict(name, info) for name, info in tables.items()]
        return {
            "root_table": sanitized_root,
            "tables": blueprint_tables,
            "relationships": relationships,
            "rules": rules_applied,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _ensure_table(
        self,
        *,
        tables: Dict[str, Dict[str, Any]],
        table_name: str,
        source_path: str,
        reason: str,
    ) -> None:
        if table_name in tables:
            tables[table_name]["sources"].add(source_path)
            tables[table_name]["reasons"].add(reason)
            return

        pk = self._primary_key_name(table_name)
        tables[table_name] = {
            "table": table_name,
            "primary_key": pk,
            "columns": {
                pk: {
                    "name": pk,
                    "type": "SERIAL",
                    "nullable": False,
                    "constraints": ["PRIMARY KEY"],
                    "source": None,
                }
            },
            "foreign_keys": [],
            "sources": {source_path},
            "reasons": {reason},
        }

    def _add_column(
        self,
        tables: Dict[str, Dict[str, Any]],
        *,
        table_name: str,
        column_name: str,
        column_type: str,
        source_path: str,
    ) -> None:
        table = tables.get(table_name)
        if not table:
            return
        if column_name in table["columns"]:
            return
        table["columns"][column_name] = {
            "name": column_name,
            "type": column_type,
            "nullable": True,
            "constraints": [],
            "source": source_path,
        }
        table["sources"].add(source_path)

    def _attach_foreign_key(
        self,
        *,
        tables: Dict[str, Dict[str, Any]],
        child_table: str,
        parent_table: str,
    ) -> Optional[Dict[str, str]]:
        if not child_table or not parent_table:
            return None
        child = tables.get(child_table)
        parent = tables.get(parent_table)
        if not child or not parent:
            return None

        parent_pk = parent["primary_key"]
        fk_name = self._foreign_key_name(parent_table)
        if fk_name not in child["columns"]:
            child["columns"][fk_name] = {
                "name": fk_name,
                "type": "BIGINT",
                "nullable": False,
                "constraints": [],
                "source": None,
            }
        relation = {
            "from_table": child_table,
            "from_column": fk_name,
            "to_table": parent_table,
            "to_column": parent_pk,
        }
        # Avoid duplicates
        if relation not in child["foreign_keys"]:
            child["foreign_keys"].append(relation)
        return relation

    def _resolve_parent_table(
        self,
        container_map: Dict[Optional[str], str],
        parent_path: Optional[str],
        default: str,
    ) -> str:
        if parent_path in container_map:
            return container_map[parent_path]
        if parent_path:
            # Try progressively removing segments
            segments = parent_path.split(".")
            while segments:
                candidate = ".".join(segments)
                if candidate in container_map:
                    return container_map[candidate]
                segments.pop()
        return container_map.get(None, default)

    def _table_to_dict(self, name: str, info: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "name": name,
            "primary_key": info["primary_key"],
            "columns": list(info["columns"].values()),
            "foreign_keys": info["foreign_keys"],
            "sources": sorted(info["sources"]),
            "reasons": sorted(info["reasons"]),
        }

    def _tableize(self, value: str) -> str:
        cleaned = re.sub(r"[^0-9a-zA-Z]+", "_", value or "table")
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        cleaned = cleaned.lower() or "table"
        return cleaned

    def _column_name(self, field_path: str) -> str:
        token = field_path.split(".")[-1]
        token = token.replace("[]", "")
        return self._tableize(token)

    def _primary_key_name(self, table_name: str) -> str:
        base = table_name.rstrip("s") if table_name.endswith("s") and len(table_name) > 1 else table_name
        return f"{base}_id"

    def _foreign_key_name(self, parent_table: str) -> str:
        base = parent_table.rstrip("s") if parent_table.endswith("s") and len(parent_table) > 1 else parent_table
        return f"{base}_id"

    def _map_sql_type(self, data_type: Optional[str]) -> str:
        if not data_type:
            return "TEXT"
        normalized = data_type.lower()
        if normalized.startswith("array"):
            return "JSONB"
        if normalized in {"string", "text"}:
            return "TEXT"
        if normalized in {"integer", "int", "bigint"}:
            return "BIGINT"
        if normalized in {"number", "float", "double"}:
            return "DOUBLE PRECISION"
        if normalized in {"boolean", "bool"}:
            return "BOOLEAN"
        if normalized in {"object", "json", "jsonb"}:
            return "JSONB"
        return "TEXT"


__all__ = ["SQLNormalizationEngine"]
