"""Schema registry storage and parsing utilities."""

from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional

from schema_analyzer import JSONStructureAnalyzer
from classification_engine import DataClassificationEngine
from sql_normalization_engine import SQLNormalizationEngine
from mongo_strategy_engine import MongoDocumentStrategyEngine
from storage_strategy_generator import StorageStrategyGenerator


@dataclass
class FieldMetadata:
    """Represents one flattened schema field."""

    field_name: str
    data_type: str
    is_array: bool
    is_unique: bool
    is_nullable: bool
    parent_field: Optional[str]
    nesting_level: int
    raw_definition: Dict[str, Any]


class SchemaRegistry:
    """Handles schema registration and metadata persistence."""

    def __init__(self, db_path: str = "schema_registry.db") -> None:
        self.db_path = db_path
        self.analyzer = JSONStructureAnalyzer()
        self.classifier = DataClassificationEngine()
        self.normalizer = SQLNormalizationEngine()
        self.mongo_strategy = MongoDocumentStrategyEngine()
        self.storage_generator = StorageStrategyGenerator()
        self._init_db()

    # ------------------------------------------------------------------
    # Database helpers
    # ------------------------------------------------------------------
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON;")
        return conn

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schemas (
                    schema_id   INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_name TEXT NOT NULL,
                    raw_schema  TEXT NOT NULL,
                    created_at  TEXT NOT NULL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fields (
                    field_id      INTEGER PRIMARY KEY AUTOINCREMENT,
                    schema_id     INTEGER NOT NULL,
                    field_name    TEXT NOT NULL,
                    data_type     TEXT NOT NULL,
                    is_array      INTEGER NOT NULL DEFAULT 0,
                    is_unique     INTEGER NOT NULL DEFAULT 0,
                    is_nullable   INTEGER NOT NULL DEFAULT 1,
                    parent_field  TEXT,
                    nesting_level INTEGER NOT NULL DEFAULT 0,
                    metadata      TEXT,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_analysis (
                    analysis_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    schema_id      INTEGER NOT NULL,
                    field_path     TEXT NOT NULL,
                    parent_field   TEXT,
                    classification TEXT NOT NULL,
                    pattern        TEXT,
                    meaning        TEXT,
                    pipeline       TEXT,
                    pipeline_reason TEXT,
                    pipeline_confidence REAL,
                    recommendation TEXT,
                    nesting_level  INTEGER NOT NULL DEFAULT 0,
                    data_type      TEXT,
                    notes          TEXT,
                    flags          TEXT,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )
            self._ensure_column(conn, "schema_analysis", "meaning", "TEXT")
            self._ensure_column(conn, "schema_analysis", "pipeline", "TEXT")
            self._ensure_column(conn, "schema_analysis", "pipeline_reason", "TEXT")
            self._ensure_column(conn, "schema_analysis", "pipeline_confidence", "REAL")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_sql_blueprints (
                    schema_id INTEGER PRIMARY KEY,
                    blueprint TEXT NOT NULL,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_mongo_strategies (
                    schema_id INTEGER PRIMARY KEY,
                    strategy  TEXT NOT NULL,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_analysis_summary (
                    schema_id INTEGER PRIMARY KEY,
                    summary   TEXT NOT NULL,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_storage_strategies (
                    schema_id INTEGER PRIMARY KEY,
                    strategy  TEXT NOT NULL,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """
            )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def register_schema(self, entity: str, schema: Dict[str, Any]) -> Dict[str, Any]:
        """Persist a schema definition and return the stored metadata."""

        entity = (entity or "").strip()
        if not entity:
            raise ValueError("'entity' is required")
        if not isinstance(schema, dict) or not schema:
            raise ValueError("'schema' must be a non-empty object")

        prepared_schema = self.analyzer.prepare_schema(schema)
        flattened_fields = self._flatten_schema(prepared_schema)
        if not flattened_fields:
            raise ValueError("Provided schema did not yield any fields")

        created_at = datetime.utcnow().isoformat()

        with self._get_conn() as conn:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO schemas (entity_name, raw_schema, created_at) VALUES (?, ?, ?)",
                (entity, json.dumps(schema), created_at),
            )
            schema_id = cursor.lastrowid

            for field in flattened_fields:
                cursor.execute(
                    """
                    INSERT INTO fields (
                        schema_id, field_name, data_type, is_array, is_unique,
                        is_nullable, parent_field, nesting_level, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        schema_id,
                        field.field_name,
                        field.data_type,
                        int(field.is_array),
                        int(field.is_unique),
                        int(field.is_nullable),
                        field.parent_field,
                        field.nesting_level,
                        json.dumps(field.raw_definition),
                    ),
                )

            conn.commit()

        analysis = self.analyzer.analyze(prepared_schema, already_prepared=True)
        classification = self.classifier.classify_entries(analysis.get("entries", []))
        analysis["entries"] = classification.get("entries", [])
        analysis.setdefault("summary", {})
        analysis["summary"]["pipelines"] = classification["summary"].get("pipelines", {})
        analysis["summary"]["pipeline_reasons"] = classification["summary"].get("reasons", {})
        blueprint = self.normalizer.generate_blueprint(entity_name=entity, entries=analysis["entries"])
        mongo_strategy = self.mongo_strategy.generate_strategy(entity_name=entity, entries=analysis["entries"])
        storage_strategy = self.storage_generator.generate(
            entity_name=entity,
            sql_blueprint=blueprint,
            mongo_strategy=mongo_strategy,
        )
        self._save_analysis(schema_id, analysis)
        self._save_blueprint(schema_id, blueprint)
        self._save_mongo_strategy(schema_id, mongo_strategy)
        self._save_storage_strategy(schema_id, storage_strategy)

        return self.get_schema(schema_id)

    def refresh_schema_with_sample(self, schema_id: int, sample: Dict[str, Any]) -> Dict[str, Any]:
        """Extend an existing schema with a sample payload and regenerate metadata."""
        if not isinstance(sample, dict) or not sample:
            return self.get_schema(schema_id)

        schema_row = self._get_schema_row(schema_id)
        raw_schema = json.loads(schema_row["raw_schema"]) if schema_row else {}
        prepared_existing = self.analyzer.prepare_schema(raw_schema) if raw_schema else {}
        prepared_sample = self.analyzer.prepare_schema(sample)
        merged_schema = self._deep_merge_schema(prepared_existing, prepared_sample)

        flattened_fields = self._flatten_schema(merged_schema)
        if not flattened_fields:
            return self.get_schema(schema_id)

        with self._get_conn() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    "UPDATE schemas SET raw_schema = ?, updated_at = ? WHERE schema_id = ?",
                    (json.dumps(merged_schema), datetime.utcnow().isoformat(), schema_id),
                )
            except sqlite3.OperationalError:
                cursor.execute(
                    "UPDATE schemas SET raw_schema = ? WHERE schema_id = ?",
                    (json.dumps(merged_schema), schema_id),
                )
            cursor.execute("DELETE FROM fields WHERE schema_id = ?", (schema_id,))
            for field in flattened_fields:
                cursor.execute(
                    """
                    INSERT INTO fields (
                        schema_id, field_name, data_type, is_array, is_unique,
                        is_nullable, parent_field, nesting_level, metadata
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        schema_id,
                        field.field_name,
                        field.data_type,
                        int(field.is_array),
                        int(field.is_unique),
                        int(field.is_nullable),
                        field.parent_field,
                        field.nesting_level,
                        json.dumps(field.raw_definition),
                    ),
                )
            conn.commit()

        analysis = self.analyzer.analyze(merged_schema, already_prepared=True)
        classification = self.classifier.classify_entries(analysis.get("entries", []))
        analysis["entries"] = classification.get("entries", [])
        analysis.setdefault("summary", {})
        analysis["summary"]["pipelines"] = classification["summary"].get("pipelines", {})
        analysis["summary"]["pipeline_reasons"] = classification["summary"].get("reasons", {})
        blueprint = self.normalizer.generate_blueprint(entity_name=schema_row["entity_name"], entries=analysis["entries"])
        mongo_strategy = self.mongo_strategy.generate_strategy(entity_name=schema_row["entity_name"], entries=analysis["entries"])
        storage_strategy = self.storage_generator.generate(
            entity_name=schema_row["entity_name"],
            sql_blueprint=blueprint,
            mongo_strategy=mongo_strategy,
        )
        self._save_analysis(schema_id, analysis)
        self._save_blueprint(schema_id, blueprint)
        self._save_mongo_strategy(schema_id, mongo_strategy)
        self._save_storage_strategy(schema_id, storage_strategy)

        return self.get_schema(schema_id)

    def _get_schema_row(self, schema_id: int) -> Optional[sqlite3.Row]:
        with self._get_conn() as conn:
            cursor = conn.execute(
                "SELECT schema_id, entity_name, raw_schema FROM schemas WHERE schema_id = ?",
                (schema_id,),
            )
            return cursor.fetchone()

    def _deep_merge_schema(self, base: Dict[str, Any], incoming: Dict[str, Any]) -> Dict[str, Any]:
        merged = dict(base)
        for key, value in incoming.items():
            if key in merged and isinstance(merged.get(key), dict) and isinstance(value, dict):
                merged[key] = self._deep_merge_schema(merged[key], value)
            else:
                merged[key] = value
        return merged

    def list_schemas(self, entity: Optional[str] = None) -> List[Dict[str, Any]]:
        """Return all schemas, optionally filtered by entity name."""

        query = "SELECT schema_id, entity_name, created_at FROM schemas"
        params: List[Any] = []
        if entity:
            query += " WHERE entity_name = ?"
            params.append(entity)
        query += " ORDER BY created_at DESC"

        with self._get_conn() as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()

        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "schema_id": row["schema_id"],
                    "entity_name": row["entity_name"],
                    "created_at": row["created_at"],
                    "field_count": self._get_field_count(row["schema_id"]),
                    "analysis_summary": self._get_analysis_summary(row["schema_id"]),
                    "sql_blueprint_summary": self._get_blueprint_summary(row["schema_id"]),
                    "mongo_strategy_summary": self._get_mongo_strategy_summary(row["schema_id"]),
                    "storage_strategy_summary": self._get_storage_strategy_summary(row["schema_id"]),
                }
            )
        return results

    def get_schema(self, schema_id: int) -> Dict[str, Any]:
        """Fetch a schema and associated fields."""
        with self._get_conn() as conn:
            cursor = conn.execute(
                "SELECT schema_id, entity_name, raw_schema, created_at FROM schemas WHERE schema_id = ?",
                (schema_id,),
            )
            row = cursor.fetchone()
            if not row:
                raise ValueError(f"Schema {schema_id} not found")

            field_cursor = conn.execute(
                """
                SELECT field_id, field_name, data_type, is_array, is_unique,
                       is_nullable, parent_field, nesting_level, metadata
                FROM fields
                WHERE schema_id = ?
                ORDER BY nesting_level, field_name
                """,
                (schema_id,),
            )
            fields = [self._row_to_field_dict(f_row) for f_row in field_cursor.fetchall()]

        analysis = self._get_analysis(row["schema_id"])
        return {
            "schema_id": row["schema_id"],
            "entity_name": row["entity_name"],
            "created_at": row["created_at"],
            "raw_schema": json.loads(row["raw_schema"]),
            "fields": fields,
            "analysis": analysis,
            "sql_blueprint": analysis.get("sql_blueprint"),
            "mongo_strategy": analysis.get("mongo_strategy"),
            "storage_strategy": analysis.get("storage_strategy"),
        }
    def _get_blueprint_summary(self, schema_id: int) -> Dict[str, Any]:
        blueprint = self._get_blueprint(schema_id)
        if not blueprint:
            return {"table_count": 0}
        return {
            "table_count": len(blueprint.get("tables", [])),
            "root_table": blueprint.get("root_table"),
        }

    def _get_mongo_strategy_summary(self, schema_id: int) -> Dict[str, Any]:
        strategy = self._get_mongo_strategy(schema_id)
        if not strategy:
            return {"collections": 0}
        return {
            "collections": len(strategy.get("documents", [])),
            "root_collection": strategy.get("root_collection"),
            "embed_count": len(strategy.get("rules", {}).get("embed", [])),
            "reference_count": len(strategy.get("rules", {}).get("reference", [])),
        }

    def _get_storage_strategy_summary(self, schema_id: int) -> Dict[str, Any]:
        strategy = self._get_storage_strategy(schema_id)
        if not strategy:
            return {"sql_commands": 0, "mongo_commands": 0}
        return {
            "sql_commands": len(strategy.get("sql", {}).get("commands", [])),
            "mongo_commands": len(strategy.get("mongo", {}).get("commands", [])),
            "mapped_fields": len(strategy.get("mappings", {}).get("fields", [])),
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_field_count(self, schema_id: int) -> int:
        with self._get_conn() as conn:
            cursor = conn.execute(
                "SELECT COUNT(*) as count FROM fields WHERE schema_id = ?",
                (schema_id,),
            )
            result = cursor.fetchone()
            return int(result["count"]) if result else 0

    def _save_analysis(self, schema_id: int, analysis: Dict[str, Any]) -> None:
        entries = analysis.get("entries", [])
        summary = analysis.get("summary", {"total_fields": 0})

        with self._get_conn() as conn:
            conn.execute("DELETE FROM schema_analysis WHERE schema_id = ?", (schema_id,))
            conn.execute(
                "DELETE FROM schema_analysis_summary WHERE schema_id = ?",
                (schema_id,),
            )

            for entry in entries:
                conn.execute(
                    """
                    INSERT INTO schema_analysis (
                        schema_id, field_path, parent_field, classification, pattern,
                        meaning, pipeline, pipeline_reason, pipeline_confidence,
                        recommendation, nesting_level, data_type, notes, flags
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        schema_id,
                        entry.get("field_path"),
                        entry.get("parent"),
                        entry.get("classification", "unknown"),
                        entry.get("pattern"),
                        entry.get("meaning"),
                        entry.get("pipeline"),
                        entry.get("pipeline_reason"),
                        entry.get("pipeline_confidence"),
                        entry.get("recommendation"),
                        entry.get("nesting_level", 0),
                        entry.get("data_type"),
                        entry.get("notes"),
                        json.dumps(entry.get("flags", [])),
                    ),
                )

            conn.execute(
                """
                INSERT INTO schema_analysis_summary (schema_id, summary)
                VALUES (?, ?)
                """,
                (schema_id, json.dumps(summary)),
            )

            conn.commit()

    def _save_blueprint(self, schema_id: int, blueprint: Dict[str, Any]) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO schema_sql_blueprints (schema_id, blueprint)
                VALUES (?, ?)
                ON CONFLICT(schema_id) DO UPDATE SET blueprint = excluded.blueprint
                """,
                (schema_id, json.dumps(blueprint)),
            )
            conn.commit()

    def _save_mongo_strategy(self, schema_id: int, strategy: Dict[str, Any]) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO schema_mongo_strategies (schema_id, strategy)
                VALUES (?, ?)
                ON CONFLICT(schema_id) DO UPDATE SET strategy = excluded.strategy
                """,
                (schema_id, json.dumps(strategy)),
            )
            conn.commit()

    def _save_storage_strategy(self, schema_id: int, strategy: Dict[str, Any]) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO schema_storage_strategies (schema_id, strategy)
                VALUES (?, ?)
                ON CONFLICT(schema_id) DO UPDATE SET strategy = excluded.strategy
                """,
                (schema_id, json.dumps(strategy)),
            )
            conn.commit()

    def _get_analysis(self, schema_id: int) -> Dict[str, Any]:
        with self._get_conn() as conn:
            entry_cursor = conn.execute(
                """
                SELECT field_path, parent_field, classification, pattern, meaning, pipeline,
                       pipeline_reason, pipeline_confidence, recommendation,
                       nesting_level, data_type, notes, flags
                FROM schema_analysis
                WHERE schema_id = ?
                ORDER BY nesting_level, field_path
                """,
                (schema_id,),
            )
            entries = [
                {
                    "field_path": row["field_path"],
                    "parent": row["parent_field"],
                    "classification": row["classification"],
                    "pattern": row["pattern"],
                    "meaning": row["meaning"],
                    "pipeline": row["pipeline"],
                    "pipeline_reason": row["pipeline_reason"],
                    "pipeline_confidence": row["pipeline_confidence"],
                    "recommendation": row["recommendation"],
                    "nesting_level": row["nesting_level"],
                    "data_type": row["data_type"],
                    "notes": row["notes"],
                    "flags": json.loads(row["flags"]) if row["flags"] else [],
                }
                for row in entry_cursor.fetchall()
            ]

            summary_row = conn.execute(
                "SELECT summary FROM schema_analysis_summary WHERE schema_id = ?",
                (schema_id,),
            ).fetchone()

        summary = json.loads(summary_row["summary"]) if summary_row else {"total_fields": 0}
        readable = [self.analyzer.format_entry(entry) for entry in entries] if entries else []
        blueprint = self._get_blueprint(schema_id)
        strategy = self._get_mongo_strategy(schema_id)
        storage = self._get_storage_strategy(schema_id)
        return {
            "entries": entries,
            "summary": summary,
            "readable": readable,
            "sql_blueprint": blueprint,
            "mongo_strategy": strategy,
            "storage_strategy": storage,
        }

    def _get_blueprint(self, schema_id: int) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT blueprint FROM schema_sql_blueprints WHERE schema_id = ?",
                (schema_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["blueprint"])

    def _get_mongo_strategy(self, schema_id: int) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT strategy FROM schema_mongo_strategies WHERE schema_id = ?",
                (schema_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["strategy"])

    def _get_storage_strategy(self, schema_id: int) -> Optional[Dict[str, Any]]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT strategy FROM schema_storage_strategies WHERE schema_id = ?",
                (schema_id,),
            ).fetchone()
        if not row:
            return None
        return json.loads(row["strategy"])

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}
        if column not in columns:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")

    def _get_analysis_summary(self, schema_id: int) -> Dict[str, Any]:
        with self._get_conn() as conn:
            row = conn.execute(
                "SELECT summary FROM schema_analysis_summary WHERE schema_id = ?",
                (schema_id,),
            ).fetchone()
        return json.loads(row["summary"]) if row else {"total_fields": 0}

    def _row_to_field_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        return {
            "field_id": row["field_id"],
            "field_name": row["field_name"],
            "data_type": row["data_type"],
            "is_array": bool(row["is_array"]),
            "is_unique": bool(row["is_unique"]),
            "is_nullable": bool(row["is_nullable"]),
            "parent_field": row["parent_field"],
            "nesting_level": row["nesting_level"],
            "metadata": json.loads(row["metadata"]) if row["metadata"] else {},
        }

    def _flatten_schema(
        self,
        schema: Dict[str, Any],
        parent_field: Optional[str] = None,
        nesting_level: int = 0,
    ) -> List[FieldMetadata]:
        flattened: List[FieldMetadata] = []

        for field_name, definition in schema.items():
            normalized = self._normalize_definition(definition)
            data_type = normalized.get("type", "object" if "properties" in normalized else "string")
            is_array = data_type == "array"

            field_entry = FieldMetadata(
                field_name=field_name,
                data_type=self._describe_data_type(normalized),
                is_array=is_array,
                is_unique=bool(normalized.get("unique", False)),
                is_nullable=bool(normalized.get("nullable", True)),
                parent_field=parent_field,
                nesting_level=nesting_level,
                raw_definition=normalized,
            )

            flattened.append(field_entry)

            # Handle nested object definitions
            if self._has_nested_properties(normalized):
                nested_parent = f"{field_name}[]" if is_array else field_name
                nested_schema = self._extract_nested_schema(normalized)
                flattened.extend(
                    self._flatten_schema(
                        nested_schema,
                        parent_field=nested_parent,
                        nesting_level=nesting_level + 1,
                    )
                )

        return flattened

    def _normalize_definition(self, definition: Any) -> Dict[str, Any]:
        if definition is None:
            raise ValueError("Schema definition entries cannot be null")
        if isinstance(definition, str):
            known = self.analyzer.KNOWN_TYPE_TOKENS
            token = definition.lower()
            return {"type": token if token in known else "string"}
        if isinstance(definition, list):
            return {"type": "enum", "values": definition}
        if not isinstance(definition, dict):
            raise ValueError(f"Unsupported definition type: {type(definition)}")

        normalized = dict(definition)
        dtype = normalized.get("type")
        if isinstance(dtype, list):
            normalized["type"] = dtype[0]
        if not normalized.get("type") and "properties" in normalized:
            normalized["type"] = "object"
        if not normalized.get("type") and "items" in normalized:
            normalized["type"] = "array"
        return normalized

    def _has_nested_properties(self, definition: Dict[str, Any]) -> bool:
        if definition.get("type") == "object" and isinstance(definition.get("properties"), dict):
            return True
        if definition.get("type") == "array":
            items = definition.get("items")
            return isinstance(items, dict) and (
                items.get("type") == "object" or isinstance(items.get("properties"), dict)
            )
        return False

    def _extract_nested_schema(self, definition: Dict[str, Any]) -> Dict[str, Any]:
        if definition.get("type") == "object":
            return definition.get("properties", {})
        if definition.get("type") == "array":
            items = definition.get("items", {})
            if isinstance(items, dict) and (
                items.get("type") == "object" or isinstance(items.get("properties"), dict)
            ):
                return items.get("properties", {})
        return {}

    def _describe_data_type(self, definition: Dict[str, Any]) -> str:
        dtype = definition.get("type", "string")
        if dtype == "array":
            items = definition.get("items")
            if isinstance(items, dict):
                inner_type = items.get("type", "any")
                return f"array<{inner_type}>"
            return "array"
        return dtype


__all__ = ["SchemaRegistry", "FieldMetadata"]
