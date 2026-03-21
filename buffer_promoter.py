"""Utility for promoting buffered fields once placement decisions are available."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict, List, Optional

from schema_registry import SchemaRegistry
from crud_executor import HybridCRUDExecutor
from buffer_queue import BufferQueue


class BufferPromoter:
    """Processes buffer_queue entries and replays payloads when ready."""

    def __init__(
        self,
        *,
        registry: Optional[SchemaRegistry] = None,
        crud_executor: Optional[HybridCRUDExecutor] = None,
        buffer_queue: Optional[BufferQueue] = None,
    ) -> None:
        self.registry = registry or SchemaRegistry()
        self.crud_executor = crud_executor or HybridCRUDExecutor(registry=self.registry)
        self.buffer_queue = buffer_queue or BufferQueue(db_path=self.registry.db_path)

    def promote(
        self,
        *,
        schema_id: Optional[int] = None,
        limit: int = 50,
        execute: bool = False,
    ) -> Dict[str, Any]:
        entries = self.buffer_queue.list_entries(status="pending", limit=limit)
        summary = {
            "evaluated": 0,
            "processed": 0,
            "still_buffer": 0,
            "skipped": 0,
            "errors": [],
        }
        for entry in entries:
            if schema_id and entry["schema_id"] != schema_id:
                continue
            summary["evaluated"] += 1
            schema = self.registry.get_schema(entry["schema_id"])
            decision = self._field_decision(schema, entry["field_path"])
            if decision == "buffer":
                summary["still_buffer"] += 1
                continue
            payload = entry.get("payload")
            if not isinstance(payload, dict):
                summary["skipped"] += 1
                continue
            filters = self._derive_filters(schema, payload)
            operation = "update" if filters else "insert"
            try:
                self.crud_executor.execute(
                    entry["schema_id"],
                    operation=operation,
                    payload=payload,
                    filters=filters or None,
                    strategy="simple",
                    execute=execute,
                )
                self.buffer_queue.mark_processed(entry["queue_id"])
                summary["processed"] += 1
            except Exception as exc:  # pragma: no cover
                summary["errors"].append({
                    "queue_id": entry["queue_id"],
                    "error": str(exc),
                })
        return summary

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _field_decision(self, schema: Dict[str, Any], field_path: str) -> str:
        storage_strategy = schema.get("storage_strategy") or {}
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            if mapping.get("field_path") == field_path:
                return (mapping.get("decision") or "buffer").lower()
        for entry in (schema.get("analysis") or {}).get("entries", []):
            if entry.get("field_path") == field_path:
                return (entry.get("pipeline") or "buffer").lower()
        return "buffer"

    def _derive_filters(self, schema: Dict[str, Any], payload: Dict[str, Any]) -> Dict[str, Any]:
        blueprint = schema.get("sql_blueprint") or schema.get("analysis", {}).get("sql_blueprint")
        storage_strategy = schema.get("storage_strategy") or {}
        if not blueprint:
            return {}
        root_table = blueprint.get("root_table")
        primary_key = None
        for table in blueprint.get("tables", []):
            if table.get("name") == root_table:
                primary_key = table.get("primary_key")
                break
        if not root_table or not primary_key:
            return {}
        pk_mapping = self._find_mapping(storage_strategy, root_table, primary_key)
        if not pk_mapping:
            return {}
        field_path = pk_mapping.get("field_path")
        if not field_path:
            return {}
        value = self._resolve_field_value(payload, field_path)
        return {primary_key: value} if value is not None else {}

    def _find_mapping(
        self,
        storage_strategy: Dict[str, Any],
        table: str,
        column: str,
    ) -> Optional[Dict[str, Any]]:
        for mapping in storage_strategy.get("mappings", {}).get("fields", []):
            if mapping.get("table") == table and mapping.get("column") == column:
                return mapping
        return None

    def _resolve_field_value(self, payload: Any, field_path: str) -> Any:
        tokens = [token for token in (field_path or "").split(".") if token]
        return self._resolve_tokens(payload, tokens)

    def _resolve_tokens(self, current: Any, tokens: List[str]) -> Any:
        if not tokens:
            return current
        if current is None:
            return None
        token = tokens[0]
        rest = tokens[1:]
        if isinstance(current, list):
            for item in current:
                value = self._resolve_tokens(item, tokens)
                if value is not None:
                    return value
            return None
        if isinstance(current, dict):
            return self._resolve_tokens(current.get(token), rest)
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Promote buffered fields when mappings are available")
    parser.add_argument("--schema-id", type=int, default=None, help="Process only this schema id")
    parser.add_argument("--limit", type=int, default=50, help="Maximum buffer entries to inspect")
    parser.add_argument(
        "--execute",
        action="store_true",
        help="When set, run live CRUD operations instead of returning plans",
    )
    args = parser.parse_args()
    promoter = BufferPromoter()
    stats = promoter.promote(schema_id=args.schema_id, limit=args.limit, execute=args.execute)
    print(json.dumps(stats, indent=2))


if __name__ == "__main__":
    main()
