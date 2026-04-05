from __future__ import annotations

import pytest

from crud_executor import HybridCRUDExecutor


def test_simple_update_with_filters_promotes_to_advanced(registry, monkeypatch) -> None:
    executor = HybridCRUDExecutor(registry=registry)

    monkeypatch.setattr(executor.registry, "refresh_schema_with_sample", lambda *args, **kwargs: None)
    monkeypatch.setattr(executor, "_auto_create_sql_tables", lambda schema_id: {"attempted": False, "errors": []})
    monkeypatch.setattr(executor, "_transaction_enabled", lambda: False)

    def _fake_plan_query(schema_id, request):
        strategy = (request.get("strategy") or "").lower()
        if strategy == "advanced":
            return {
                "schema_id": schema_id,
                "operation": "update",
                "strategy": "advanced",
                "filters": {"student_id": "STU-2024-101"},
                "sql": [{"table": "student", "set": {"scholarship": 1}}],
                "mongo": [],
            }
        return {
            "schema_id": schema_id,
            "operation": "update",
            "strategy": "simple",
            "delete": {},
            "insert": {},
        }

    monkeypatch.setattr(executor.query_engine, "plan_query", _fake_plan_query)
    monkeypatch.setattr(
        executor,
        "_execute_sql_updates",
        lambda sql_updates, filters, **kwargs: [{"table": "student", "affected": 1}],
    )
    monkeypatch.setattr(executor, "_execute_mongo_updates", lambda mongo_updates, filters: [])

    result = executor._handle_update(
        1,
        {"scholarship": 1},
        {"student_id": "STU-2024-101"},
        strategy="simple",
        execute=True,
    )

    assert result["strategy"] == "simple"
    assert result["effective_strategy"] == "advanced"
    assert result["sql"] == [{"table": "student", "affected": 1}]
    assert "auto-promoted" in (result.get("note") or "")


def test_unfiltered_simple_update_is_blocked(registry, monkeypatch) -> None:
    executor = HybridCRUDExecutor(registry=registry)
    monkeypatch.setattr(executor, "_allow_unfiltered_destructive_writes", lambda: False)

    with pytest.raises(ValueError, match="Refused unfiltered simple update"):
        executor._handle_update(
            1,
            {"scholarship": 1},
            {},
            strategy="simple",
            execute=True,
        )


def test_unfiltered_entity_delete_is_blocked(registry, monkeypatch) -> None:
    executor = HybridCRUDExecutor(registry=registry)
    monkeypatch.setattr(executor, "_allow_unfiltered_destructive_writes", lambda: False)
    monkeypatch.setattr(
        executor.query_engine,
        "plan_query",
        lambda schema_id, request: {
            "schema_id": schema_id,
            "operation": "delete",
            "strategy": "entity",
            "filters": {},
            "sql": {"tables": ["student"]},
            "mongo": {"collections": []},
        },
    )

    with pytest.raises(ValueError, match="Refused unfiltered delete"):
        executor._handle_delete(
            1,
            {},
            strategy="entity",
            execute=True,
        )
