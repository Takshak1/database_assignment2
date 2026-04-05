from __future__ import annotations

from crud_executor import HybridCRUDExecutor


def test_read_sql_aliases_are_normalized_to_requested_fields(registry) -> None:
    executor = HybridCRUDExecutor(registry=registry)

    rows = [
        {
            "student_student_id": 2,
            "student_name": "Rajan Patel",
            "student_cgpa": 3.62,
            "student_scholarship": 0,
        }
    ]
    field_locations = [
        {"requested": "student_id", "storage": "sql", "table": "student", "column": "student_id"},
        {"requested": "name", "storage": "sql", "table": "student", "column": "name"},
        {"requested": "cgpa", "storage": "sql", "table": "student", "column": "cgpa"},
        {"requested": "scholarship", "storage": "sql", "table": "student", "column": "scholarship"},
    ]

    normalized = executor._normalize_read_sql_rows(rows, field_locations)
    assert normalized == [
        {
            "student_id": 2,
            "name": "Rajan Patel",
            "cgpa": 3.62,
            "scholarship": 0,
        }
    ]
