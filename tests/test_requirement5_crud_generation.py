from __future__ import annotations

from typing import Any, Dict

from crud_query_engine import CRUDQueryEngine


def test_read_plan_contains_sql_statement(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "comments"],
            "filters": {"username": "neo"},
            "limit": 25,
        },
    )

    assert plan["sql"]["statement"].startswith("SELECT")
    assert plan["merge"]["merge_key"], "Merge plan should expose a key for stitching"
    comments_location = next(loc for loc in plan["field_locations"] if loc["requested"] == "comments")
    assert comments_location["storage"] in {"sql", "mongo"}


def test_filters_translate_into_sql_parameters(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username"],
            "filters": {"username": "trinity"},
        },
    )

    where_clause = plan["sql"]["where"]
    params = plan["sql"]["parameters"]
    assert "username" in where_clause
    assert params
    assert params[0] == "trinity"


def test_merge_plan_describes_response_shape(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "comments"],
        },
    )

    merge_plan = plan["merge"]
    assert merge_plan["strategy"]
    assert "response_shape" in merge_plan
    assert merge_plan["response_shape"]["requested_fields"] == ["username", "comments"]


def _register_hybrid_event_schema(registry) -> Dict[str, Any]:
    schema = {
        "username": {"type": "string", "unique": True},
        "event_id": {"type": "integer"},
        "tags": {
            "type": "array",
            "items": {"type": "string"},
        },
        "activity": {
            "type": "object",
            "properties": {
                "device": {
                    "type": "object",
                    "properties": {
                        "os": {"type": "string"},
                        "meta": {
                            "type": "object",
                            "properties": {
                                "version": {"type": "string"},
                            },
                        },
                    },
                }
            },
        },
    }
    return registry.register_schema("hybrid_event", schema)


def test_edge_mixed_fields_require_sql_and_mongo_plans(registry) -> None:
    stored = _register_hybrid_event_schema(registry)

    planner = CRUDQueryEngine(registry=registry)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "tags", "activity.device.meta.version"],
            "limit": 5,
        },
    )

    assert plan["sql"] is not None
    assert plan["mongo"], "Expected at least one Mongo read when deep/array fields are requested"

    locations = {loc["requested"]: loc for loc in plan["field_locations"]}
    assert locations["username"]["storage"] == "sql"
    assert locations["tags"]["storage"] == "mongo"
    assert locations["activity.device.meta.version"]["storage"] == "mongo"
    assert plan["merge"]["strategy"] == "client_side_join"


def test_edge_split_filters_route_to_sql_and_mongo(registry) -> None:
    stored = _register_hybrid_event_schema(registry)

    planner = CRUDQueryEngine(registry=registry)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "tags"],
            "filters": {
                "username": "neo",
                "tags": "urgent",
            },
        },
    )

    assert "username" in (plan["sql"]["where"] or "")
    assert plan["sql"]["parameters"] == ["neo"]

    mongo_filters = [entry.get("filter") or {} for entry in plan["mongo"]]
    assert any("tags" in doc and doc["tags"] == "urgent" for doc in mongo_filters)


def test_edge_missing_field_does_not_break_hybrid_plan(registry) -> None:
    stored = _register_hybrid_event_schema(registry)

    planner = CRUDQueryEngine(registry=registry)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["username", "tags", "not_a_field"],
        },
    )

    missing = [loc for loc in plan["field_locations"] if loc["requested"] == "not_a_field"]
    assert missing and missing[0]["status"] == "missing"
    assert plan["sql"] is not None
    assert plan["mongo"]


def test_filter_alias_student_id_maps_to_id_for_student_entity(registry) -> None:
    stored = registry.register_schema(
        "student",
        {
            "id": {"type": "string", "unique": True},
            "name": {"type": "string"},
            "cgpa": {"type": "number"},
            "scholarship": {"type": "integer"},
        },
    )

    planner = CRUDQueryEngine(registry=registry)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["id", "name", "cgpa", "scholarship"],
            "filters": {"student_id": "STU-2024-101"},
            "limit": 5,
        },
    )

    assert plan["sql"] is not None
    assert "id" in (plan["sql"]["where"] or "")
    assert plan["sql"]["parameters"]
    assert all(value == "STU-2024-101" for value in plan["sql"]["parameters"])


def test_filter_uses_blueprint_column_when_mapping_missing(registry, monkeypatch) -> None:
    stored = registry.register_schema(
        "student",
        {
            "student_id": {"type": "string", "unique": True},
            "name": {"type": "string"},
            "cgpa": {"type": "number"},
            "scholarship": {"type": "integer"},
        },
    )
    planner = CRUDQueryEngine(registry=registry)
    original_build_field_map = planner._build_field_map

    def _without_student_id(storage_strategy):
        field_map = original_build_field_map(storage_strategy)
        field_map.pop("student_id", None)
        return field_map

    monkeypatch.setattr(planner, "_build_field_map", _without_student_id)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["name", "cgpa", "scholarship"],
            "filters": {"student_id": "STU-2024-101"},
            "limit": 5,
        },
    )

    assert plan["sql"] is not None
    assert "student.student_id = %s" == plan["sql"]["where"]
    assert plan["sql"]["parameters"] == ["STU-2024-101"]


def test_read_select_includes_hinted_sql_field_via_table_fallback(registry, monkeypatch) -> None:
    stored = registry.register_schema(
        "student",
        {
            "student_id": {"type": "string", "unique": True},
            "name": {"type": "string"},
            "cgpa": {"type": "number"},
            "scholarship": {"type": "integer"},
        },
    )

    planner = CRUDQueryEngine(registry=registry)
    original_locate = planner._locate_field

    def _locate_with_hinted_student_id(field, field_map, table_map):
        location = original_locate(field, field_map, table_map)
        if field == "student_id":
            location.status = "hint"
            location.notes = "metadata_hint"
            location.table = None
            location.column = None
            location.related_columns = None
        return location

    monkeypatch.setattr(planner, "_locate_field", _locate_with_hinted_student_id)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["student_id", "name", "cgpa", "scholarship"],
            "filters": {"student_id": "STU-2024-101"},
            "limit": 5,
        },
    )

    assert plan["sql"] is not None
    select_clause = " ".join(plan["sql"].get("select") or [])
    assert "student.student_id" in select_clause


def test_read_select_overrides_incorrect_hint_table(registry, monkeypatch) -> None:
    stored = registry.register_schema(
        "student",
        {
            "student_id": {"type": "string", "unique": True},
            "name": {"type": "string"},
            "cgpa": {"type": "number"},
            "scholarship": {"type": "integer"},
        },
    )

    planner = CRUDQueryEngine(registry=registry)
    original_locate = planner._locate_field

    def _locate_with_wrong_hint_table(field, field_map, table_map):
        location = original_locate(field, field_map, table_map)
        if field == "student_id":
            location.status = "hint"
            location.notes = "metadata_hint"
            location.table = "logs"
            location.column = None
            location.related_columns = None
        return location

    monkeypatch.setattr(planner, "_locate_field", _locate_with_wrong_hint_table)
    plan = planner.plan_query(
        stored["schema_id"],
        {
            "operation": "read",
            "fields": ["student_id", "name", "cgpa", "scholarship"],
            "filters": {"student_id": "STU-2024-101"},
            "limit": 5,
        },
    )

    assert plan["sql"] is not None
    select_clause = " ".join(plan["sql"].get("select") or [])
    assert "student.student_id" in select_clause
    assert "logs.student_id" not in select_clause
