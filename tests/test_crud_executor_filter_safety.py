from __future__ import annotations

from crud_executor import HybridCRUDExecutor


def test_table_where_casts_values_to_char(registry) -> None:
    executor = HybridCRUDExecutor(registry=registry)

    where = executor._build_table_where(
        {"student_id": "STU-2024-001"},
        table_columns={"student_id"},
    )

    assert where["usable"] is True
    assert where["values"] == ["STU-2024-001"]
    assert "CAST(student_id AS CHAR) = CAST(%s AS CHAR)" in where["clause"]


def test_table_where_ignores_columns_not_in_table(registry) -> None:
    executor = HybridCRUDExecutor(registry=registry)

    where = executor._build_table_where(
        {"student_id": "STU-2024-001"},
        table_columns={"university_id"},
    )

    assert where["usable"] is False
    assert where["clause"] == ""
    assert where["values"] == []
    assert where["ignored_filters"] == ["student_id"]


def test_table_where_without_table_metadata_keeps_filters(registry) -> None:
    executor = HybridCRUDExecutor(registry=registry)

    where = executor._build_table_where(
        {"student_id": "STU-2024-001", "year": 2024},
        table_columns=None,
    )

    assert where["usable"] is True
    assert len(where["values"]) == 2
    assert where["ignored_filters"] == []
