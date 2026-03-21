"""FastAPI service exposing schema registration endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from compat import patch_typing_forward_ref

patch_typing_forward_ref()

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel, Field

from schema_registry import SchemaRegistry
from crud_query_engine import CRUDQueryEngine
from crud_executor import HybridCRUDExecutor
from buffer_queue import BufferQueue

app = FastAPI(
    title="JSON Schema Registry",
    description="Register and explore incoming JSON payload structures",
    version="0.1.0",
)
registry = SchemaRegistry()
query_engine = CRUDQueryEngine(registry=registry)
crud_executor = HybridCRUDExecutor(registry=registry)
buffer_queue = BufferQueue(db_path=registry.db_path)


class SchemaRegistrationRequest(BaseModel):
    entity: str = Field(..., min_length=1, description="Logical entity name")
    schema: Dict[str, Any] = Field(..., description="JSON-style schema definition")


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


class IngestionRequest(BaseModel):
    payload: Dict[str, Any] = Field(..., description="Raw JSON record to ingest")
    execute: bool = Field(default=False, description="Set true to write to SQL/Mongo; false returns plan only")
    strategy: str = Field(default="simple", description="Insert strategy selector (maps to CRUD executor)")


@app.post("/register_schema")
def register_schema(payload: SchemaRegistrationRequest) -> Dict[str, Any]:
    """Persist an entity schema and return metadata."""
    try:
        stored = registry.register_schema(payload.entity, payload.schema)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(status_code=500, detail="Failed to register schema") from exc

    return {"message": "Schema registered", "schema": stored}


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
