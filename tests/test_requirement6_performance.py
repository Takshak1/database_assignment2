from __future__ import annotations

from typing import Dict


def test_projection_prunes_unused_columns(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username"],
            "limit": 10,
        },
    )

    select_list = plan["sql"]["select"]
    assert select_list == ["post.username AS post_username"]
    assert plan["sql"]["statement"].endswith("LIMIT 10")
    assert plan["sql"]["tables"] == ["post"]


def test_filters_reduce_row_scans(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username"],
            "filters": {"username": "spoon"},
        },
    )

    where_clause = plan["sql"]["where"]
    assert "post.username" in where_clause
    assert plan["sql"]["parameters"], "Filter parameters should be bound for prepared execution"


def test_single_table_query_skips_unnecessary_joins(crud_engine, stored_schema: Dict[str, object]) -> None:
    plan = crud_engine.plan_query(
        stored_schema["schema_id"],
        {
            "operation": "read",
            "fields": ["username"],
        },
    )

    assert plan["sql"]["joins"] == []
    assert plan["sql"]["base_table"] == "post"
