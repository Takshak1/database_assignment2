"""Step 7 storage strategy generator: emit concrete SQL/Mongo commands and field mappings."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class StorageStrategyGenerator:
    """Translate blueprints and strategies into executable commands + mappings."""

    def generate(
        self,
        *,
        entity_name: str,
        sql_blueprint: Optional[Dict[str, Any]],
        mongo_strategy: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        sql_section = self._generate_sql(sql_blueprint)
        mongo_section = self._generate_mongo(mongo_strategy)
        mappings = self._build_mappings(sql_section, mongo_section)
        return {
            "entity": entity_name,
            "sql": sql_section,
            "mongo": mongo_section,
            "mappings": mappings,
        }

    # ------------------------------------------------------------------
    # SQL helpers
    # ------------------------------------------------------------------
    def _generate_sql(self, blueprint: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not blueprint:
            return {"tables": [], "commands": []}

        commands: List[str] = []
        tables_meta: List[Dict[str, Any]] = []
        for table in blueprint.get("tables", []):
            ddl = self._table_to_ddl(table)
            commands.append(ddl)
            tables_meta.append({
                "name": table["name"],
                "columns": table.get("columns", []),
                "foreign_keys": table.get("foreign_keys", []),
            })
        return {"tables": tables_meta, "commands": commands}

    def _table_to_ddl(self, table: Dict[str, Any]) -> str:
        column_lines: List[str] = []
        for column in table.get("columns", []):
            parts = [column["name"], column["type"]]
            if not column.get("nullable", True):
                parts.append("NOT NULL")
            if column.get("constraints"):
                parts.extend(column["constraints"])
            column_lines.append(" ".join(parts))
        for fk in table.get("foreign_keys", []):
            column_lines.append(
                f"FOREIGN KEY ({fk['from_column']}) REFERENCES {fk['to_table']}({fk['to_column']})"
            )
        columns_sql = ",\n    ".join(column_lines)
        return f"CREATE TABLE {table['name']} (\n    {columns_sql}\n);"

    # ------------------------------------------------------------------
    # Mongo helpers
    # ------------------------------------------------------------------
    def _generate_mongo(self, strategy: Optional[Dict[str, Any]]) -> Dict[str, Any]:
        if not strategy:
            return {"collections": [], "commands": []}

        collections = [doc["collection"] for doc in strategy.get("documents", [])]
        commands = [f"db.createCollection(\"{name}\")" for name in collections]
        documents = strategy.get("documents", [])
        entries = strategy.get("entries", [])
        return {
            "collections": documents,
            "commands": commands,
            "entries": entries,
        }

    # ------------------------------------------------------------------
    # Mapping helpers
    # ------------------------------------------------------------------
    def _build_mappings(
        self,
        sql_section: Dict[str, Any],
        mongo_section: Dict[str, Any],
    ) -> Dict[str, List[Dict[str, Any]]]:
        field_mappings: List[Dict[str, Any]] = []

        for table in sql_section.get("tables", []):
            for column in table.get("columns", []):
                source = column.get("source")
                if not source:
                    continue
                field_mappings.append(
                    {
                        "field_path": source,
                        "table": table["name"],
                        "column": column["name"],
                        "collection": None,
                        "decision": "sql",
                    }
                )

        for entry in mongo_section.get("entries", []):
            field_path = entry.get("field_path")
            if not field_path:
                continue
            field_mappings.append(
                {
                    "field_path": field_path,
                    "table": None,
                    "column": None,
                    "collection": entry.get("target_collection") or entry.get("parent_collection"),
                    "decision": entry.get("decision"),
                }
            )

        return {"fields": field_mappings}


__all__ = ["StorageStrategyGenerator"]
