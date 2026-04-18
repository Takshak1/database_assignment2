"""FastAPI service exposing schema registration endpoints."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from compat import patch_typing_forward_ref

patch_typing_forward_ref()

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

try:  # pragma: no cover - pydantic v2 compatibility
    from pydantic import ConfigDict
except Exception:  # pragma: no cover - pydantic v1
    ConfigDict = None  # type: ignore

from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from crud_executor import HybridCRUDExecutor, DEFAULT_MYSQL_CONFIG
from buffer_queue import BufferQueue

try:  # pragma: no cover - optional dependency
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover
    mysql_connector = None  # type: ignore

try:  # pragma: no cover - optional dependency
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None  # type: ignore

app = FastAPI(
    title="JSON Schema Registry",
    description="Register and explore incoming JSON payload structures",
    version="0.1.0",
)

BASE_DIR = Path(__file__).resolve().parent


def _resolve_runtime_path(value: Optional[str], default_name: str) -> str:
    candidate = Path(value or default_name)
    if candidate.is_absolute():
        return str(candidate)
    return str((BASE_DIR / candidate).resolve())


REGISTRY_DB_PATH = _resolve_runtime_path(os.getenv("SCHEMA_REGISTRY_DB"), "schema_registry.db")
METADATA_FILE_PATH = _resolve_runtime_path(os.getenv("METADATA_FILE"), "metadata.json")

registry = SchemaRegistry(db_path=REGISTRY_DB_PATH)
query_engine = CRUDQueryEngine(registry=registry, metadata_file=METADATA_FILE_PATH)
crud_executor = HybridCRUDExecutor(registry=registry, metadata_file=METADATA_FILE_PATH)
buffer_queue = BufferQueue(db_path=registry.db_path)


def _auto_create_sql_tables(schema: Dict[str, Any]) -> Dict[str, Any]:
    enabled = os.getenv("AUTO_CREATE_SQL", "1").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return {"attempted": False, "created": 0, "errors": []}

    sql_commands = (
        schema.get("storage_strategy", {})
        .get("sql", {})
        .get("commands", [])
    )
    if not sql_commands:
        return {"attempted": True, "created": 0, "errors": ["No SQL DDL commands available."]}

    if mysql_connector is None:
        return {"attempted": True, "created": 0, "errors": ["mysql-connector-python not installed."]}

    errors: List[str] = []
    created = 0
    conn = mysql_connector.connect(**DEFAULT_MYSQL_CONFIG)
    cursor = conn.cursor()
    try:
        for command in sql_commands:
            try:
                cursor.execute(command)
                created += 1
            except Exception as exc:  # pragma: no cover - db-specific
                errors.append(str(exc))
        conn.commit()
    finally:
        cursor.close()
        conn.close()

    return {"attempted": True, "created": created, "errors": errors}


def _auto_create_mongo_collections(schema: Dict[str, Any]) -> Dict[str, Any]:
    enabled = os.getenv("AUTO_CREATE_MONGO", "1").strip().lower() in {"1", "true", "yes", "on"}
    if not enabled:
        return {"attempted": False, "created": 0, "errors": []}

    collections = (
        schema.get("storage_strategy", {})
        .get("mongo", {})
        .get("collections", [])
    )
    if not collections:
        return {"attempted": True, "created": 0, "errors": ["No Mongo collections available."]}

    if MongoClient is None:
        return {"attempted": True, "created": 0, "errors": ["pymongo is not installed."]}

    errors: List[str] = []
    created = 0
    client = MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        serverSelectionTimeoutMS=4000,
    )
    try:
        db = client[os.getenv("MONGO_DATABASE", "streaming_db")]
        existing = set(db.list_collection_names())
        for entry in collections:
            name = entry.get("collection") if isinstance(entry, dict) else entry
            if not name:
                continue
            if name in existing:
                continue
            try:
                db.create_collection(name)
                created += 1
            except Exception as exc:  # pragma: no cover
                errors.append(str(exc))
    finally:
        client.close()

    return {"attempted": True, "created": created, "errors": errors}


def _reset_registry_db() -> Dict[str, Any]:
    global registry, query_engine, crud_executor, buffer_queue
    db_path = registry.db_path
    try:
        if os.path.exists(db_path):
            os.remove(db_path)
    except Exception as exc:
        return {"success": False, "error": str(exc), "db_path": db_path}
    registry = SchemaRegistry(db_path=db_path)
    query_engine = CRUDQueryEngine(registry=registry, metadata_file=METADATA_FILE_PATH)
    crud_executor = HybridCRUDExecutor(registry=registry, metadata_file=METADATA_FILE_PATH)
    buffer_queue = BufferQueue(db_path=registry.db_path)
    return {"success": True, "db_path": db_path}


class SchemaRegistrationRequest(BaseModel):
    entity: str = Field(..., min_length=1, description="Logical entity name")
    schema_payload: Dict[str, Any] = Field(
        ..., alias="schema", description="JSON-style schema definition"
    )

    if ConfigDict is not None:  # pragma: no cover - pydantic v2
        model_config = ConfigDict(populate_by_name=True)
    else:  # pragma: no cover - pydantic v1
        class Config:  # noqa: D401 - pydantic v1 config
            allow_population_by_field_name = True


class QueryRequest(BaseModel):
    operation: str = Field(default="read", description="CRUD operation", pattern="^(?i)(read|create|update|delete)$")
    fields: List[str] = Field(default_factory=list, description="Fields to fetch or mutate")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Optional equality filters")
    limit: Optional[int] = Field(default=None, ge=1, le=1000, description="Optional result cap for read operations")


class CRUDOperationRequest(BaseModel):
    operation: str = Field(..., description="CRUD operation", pattern="^(?i)(insert|read|update|delete)$")
    payload: Optional[Dict[str, Any]] = Field(default=None, description="Payload for insert/update")
    fields: List[str] = Field(default_factory=list, description="Fields for read operations")
    filters: Optional[Dict[str, Any]] = Field(default=None, description="Equality filters / identifiers")
    strategy: str = Field(
        default="simple",
        description="Strategy selector (simple/advanced/entity/sub-entity)",
    )
    execute: bool = Field(default=False, description="Set true to run statements; false returns plan only")
    limit: Optional[int] = Field(default=None, ge=1, le=1000, description="Optional limit for reads")


class CRUDAutoRequest(CRUDOperationRequest):
    entity: str = Field(..., min_length=1, description="Logical entity name")


class IngestionRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="Raw JSON record to ingest")
    execute: bool = Field(default=False, description="Set true to write to SQL/Mongo; false returns plan only")
    strategy: str = Field(default="simple", description="Insert strategy selector (maps to CRUD executor)")


@app.post("/register_schema")
def register_schema(payload: SchemaRegistrationRequest) -> Dict[str, Any]:
    """Persist an entity schema and return metadata."""
    try:
        stored = registry.register_schema(payload.entity, payload.schema_payload)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="Failed to register schema") from exc

    auto_create_sql = _auto_create_sql_tables(stored)
    auto_create_mongo = _auto_create_mongo_collections(stored)
    return {
        "message": "Schema registered",
        "schema": stored,
        "auto_create_sql": auto_create_sql,
        "auto_create_mongo": auto_create_mongo,
    }


@app.post("/reset_registry")
def reset_registry() -> Dict[str, Any]:
    """Reset the schema registry database (use when safe to wipe metadata)."""
    result = _reset_registry_db()
    if not result.get("success"):
        raise HTTPException(status_code=500, detail=result.get("error"))
    return {"message": "Registry reset", **result}


@app.get("/schemas")
def list_schemas(entity: Optional[str] = Query(default=None, description="Filter by entity name")) -> Dict[str, Any]:
    """List all known schemas, optionally filtering by entity."""
    schemas = registry.list_schemas(entity)
    return {"count": len(schemas), "schemas": schemas}


@app.get("/schemas/{schema_id}")
def get_schema(schema_id: int) -> Dict[str, Any]:
    """Return a schema definition and field metadata."""
    try:
        schema = registry.get_schema(schema_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return schema


@app.post("/schemas/{schema_id}/query_plan")
def build_query_plan(schema_id: int, payload: QueryRequest) -> Dict[str, Any]:
    """Generate SQL/Mongo query plans for the requested fields."""
    try:
        plan = query_engine.plan_query(schema_id, payload.dict())
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=500, detail="Failed to build query plan") from exc
    if isinstance(plan.get("sql"), dict) and isinstance(plan["sql"].get("parameters"), list):
        plan["sql"]["parameters"] = {f"param_{idx}": value for idx, value in enumerate(plan["sql"]["parameters"])}
    return {"message": "Query plan generated", "plan": plan}


@app.post("/schemas/{schema_id}/crud")
def execute_crud(schema_id: int, payload: CRUDOperationRequest) -> Dict[str, Any]:
    """Execute or plan CRUD operations across SQL/Mongo backends."""
    try:
        result = crud_executor.execute(
            schema_id,
            operation=payload.operation,
            payload=payload.payload,
            fields=payload.fields,
            filters=payload.filters,
            strategy=payload.strategy,
            execute=payload.execute,
            limit=payload.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="Failed to run CRUD operation") from exc
    return {"message": "CRUD operation processed", "result": result.to_dict()}


@app.post("/crud_auto")
def execute_crud_auto(payload: CRUDAutoRequest) -> Dict[str, Any]:
    """Execute CRUD using auto-registered entity schemas when needed."""
    schema_id, auto_registration = _resolve_or_register_schema(payload.entity, payload.payload or {})
    try:
        result = crud_executor.execute(
            schema_id,
            operation=payload.operation,
            payload=payload.payload,
            fields=payload.fields,
            filters=payload.filters,
            strategy=payload.strategy,
            execute=payload.execute,
            limit=payload.limit,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="Failed to run CRUD operation") from exc
    return {
        "message": "CRUD operation processed",
        "schema_id": schema_id,
        "auto_registration": auto_registration,
        "result": result.to_dict(),
    }


@app.post("/ingest/{schema_id}")
def ingest_record(schema_id: int, request: IngestionRequest) -> Dict[str, Any]:
    """Ingest a JSON record using stored storage strategies and buffer undecided fields."""
    try:
        schema = registry.get_schema(schema_id)
    except ValueError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    buffer_fields = _buffer_field_paths(schema)
    buffered: List[Dict[str, Any]] = []
    for field_path in buffer_fields:
        value = _resolve_field_value(request.payload, field_path)
        if value is None:
            continue
        queue_id = buffer_queue.enqueue(
            schema_id,
            field_path,
            value,
            payload=request.payload,
        )
        buffered.append({"queue_id": queue_id, "field_path": field_path})

    try:
        crud_result = crud_executor.execute(
            schema_id,
            operation="insert",
            payload=request.payload,
            strategy=request.strategy,
            execute=request.execute,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail="Failed to ingest payload") from exc

    return {
        "message": "Ingestion processed",
        "buffered_fields": buffered,
        "crud": crud_result.to_dict(),
    }


def _buffer_field_paths(schema: Dict[str, Any]) -> List[str]:
    analysis = schema.get("analysis") or {}
    entries = analysis.get("entries") or []
    buffer_paths: List[str] = []
    for entry in entries:
        pipeline = (entry.get("pipeline") or "").lower()
        field_path = entry.get("field_path")
        if pipeline == "buffer" and field_path:
            buffer_paths.append(field_path)
    return buffer_paths


def _resolve_field_value(payload: Any, field_path: str) -> Any:
    tokens = [token for token in (field_path or "").split(".") if token]
    return _resolve_tokens(payload, tokens)


def _resolve_tokens(current: Any, tokens: List[str]) -> Any:
    if not tokens:
        return current
    if current is None:
        return None
    token = tokens[0]
    rest = tokens[1:]
    if isinstance(current, list):
        collected: List[Any] = []
        for item in current:
            value = _resolve_tokens(item, tokens)
            if value is None:
                continue
            if isinstance(value, list):
                collected.extend(value)
            else:
                collected.append(value)
        return collected or None
    if isinstance(current, dict):
        return _resolve_tokens(current.get(token), rest)
    return None


def _resolve_or_register_schema(entity: str, payload: Dict[str, Any]) -> Tuple[int, Dict[str, Any]]:
    entity = (entity or "").strip()
    if not entity:
        raise HTTPException(status_code=400, detail="'entity' is required")
    existing = registry.list_schemas(entity)
    if existing:
        return int(existing[0]["schema_id"]), {"status": "existing"}
    if not payload:
        raise HTTPException(status_code=400, detail="Payload is required to register a new entity")
    stored = registry.register_schema(entity, payload)
    return int(stored["schema_id"]), {"status": "registered"}
