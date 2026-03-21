"""Simple SQLite-backed queue for buffer-designated fields (Pipeline 1)."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from typing import Any, Dict, List, Optional


class BufferQueue:
    """Persists undecided fields until placement decisions become available."""

    def __init__(self, db_path: str = "schema_registry.db") -> None:
        self.db_path = db_path
        self._init_table()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def enqueue(
        self,
        schema_id: int,
        field_path: str,
        value: Any,
        *,
        payload: Optional[Dict[str, Any]] = None,
        reason: str = "buffer_field",
        status: str = "pending",
    ) -> int:
        payload_value = json.dumps(value, default=self._fallback_serializer)
        payload_snapshot = (
            json.dumps(payload, default=self._fallback_serializer) if payload is not None else None
        )
        now = datetime.utcnow().isoformat()
        with self._get_conn() as conn:
            cursor = conn.execute(
                """
                INSERT INTO buffer_queue (
                    schema_id,
                    field_path,
                    value_json,
                    payload_json,
                    reason,
                    status,
                    created_at,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    schema_id,
                    field_path,
                    payload_value,
                    payload_snapshot,
                    reason,
                    status,
                    now,
                    now,
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)

    def list_entries(self, *, status: Optional[str] = None, limit: int = 50) -> List[Dict[str, Any]]:
        query = (
            "SELECT queue_id, schema_id, field_path, value_json, payload_json, reason, status, created_at "
            "FROM buffer_queue"
        )
        params: List[Any] = []  # type: ignore[assignment]
        if status:
            query += " WHERE status = ?"
            params.append(status)
        query += " ORDER BY created_at DESC LIMIT ?"
        params.append(limit)
        with self._get_conn() as conn:
            cursor = conn.execute(query, params)
            rows = cursor.fetchall()
        results: List[Dict[str, Any]] = []
        for row in rows:
            results.append(
                {
                    "queue_id": row["queue_id"],
                    "schema_id": row["schema_id"],
                    "field_path": row["field_path"],
                    "value": self._safe_json_load(row["value_json"]),
                    "payload": self._safe_json_load(row["payload_json"]) if row["payload_json"] else None,
                    "reason": row["reason"],
                    "status": row["status"],
                    "created_at": row["created_at"],
                }
            )
        return results

    def mark_processed(self, queue_id: int, *, status: str = "processed") -> None:
        with self._get_conn() as conn:
            conn.execute(
                "UPDATE buffer_queue SET status = ?, updated_at = ? WHERE queue_id = ?",
                (status, datetime.utcnow().isoformat(), queue_id),
            )
            conn.commit()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_table(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS buffer_queue (
                    queue_id    INTEGER PRIMARY KEY AUTOINCREMENT,
                    schema_id   INTEGER NOT NULL,
                    field_path  TEXT NOT NULL,
                    value_json  TEXT NOT NULL,
                    payload_json TEXT,
                    reason      TEXT NOT NULL,
                    status      TEXT NOT NULL DEFAULT 'pending',
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT NOT NULL,
                    FOREIGN KEY(schema_id) REFERENCES schemas(schema_id) ON DELETE CASCADE
                )
                """,
            )
            self._ensure_payload_column(conn)
            conn.commit()

    def _ensure_payload_column(self, conn: sqlite3.Connection) -> None:
        cursor = conn.execute("PRAGMA table_info(buffer_queue)")
        columns = {row[1] for row in cursor.fetchall()}
        if "payload_json" not in columns:
            conn.execute("ALTER TABLE buffer_queue ADD COLUMN payload_json TEXT")
            conn.commit()

    def _safe_json_load(self, raw: str) -> Any:
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def _fallback_serializer(self, value: Any) -> Any:
        if hasattr(value, "isoformat"):
            try:
                return value.isoformat()
            except Exception:
                return str(value)
        if isinstance(value, (set, bytes)):
            return list(value) if isinstance(value, set) else value.decode("utf-8", "ignore")
        return value


__all__ = ["BufferQueue"]
