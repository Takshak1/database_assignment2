from __future__ import annotations

import importlib
import sys
import types


def _import_dashboard_module():
    if "fastapi" not in sys.modules:
        fastapi_stub = types.ModuleType("fastapi")

        class _FakeApp:
            def __init__(self, *args, **kwargs):
                pass

            def get(self, *args, **kwargs):
                def _decorator(func):
                    return func

                return _decorator

            def post(self, *args, **kwargs):
                def _decorator(func):
                    return func

                return _decorator

        def _form(value=None):
            return value

        fastapi_stub.FastAPI = _FakeApp
        fastapi_stub.Form = _form

        fastapi_responses_stub = types.ModuleType("fastapi.responses")

        class _HTMLResponse(str):
            pass

        fastapi_responses_stub.HTMLResponse = _HTMLResponse
        sys.modules["fastapi"] = fastapi_stub
        sys.modules["fastapi.responses"] = fastapi_responses_stub

    return importlib.import_module("dashboard_web")


dashboard_web = _import_dashboard_module()


def test_fk_query_uses_aliases_for_self_reference() -> None:
    statement = dashboard_web._build_fk_violation_statement(
        "university",
        "university",
        "university_id",
        "university_id",
    )

    assert " AS child_tbl " in statement
    assert " AS parent_tbl " in statement
    assert "LEFT JOIN `university` AS parent_tbl" in statement
    assert "child_tbl.`university_id` = parent_tbl.`university_id`" in statement
    assert "FROM `university` AS child_tbl" in statement


def test_fk_query_quotes_mysql_identifiers() -> None:
    statement = dashboard_web._build_fk_violation_statement(
        "student-record",
        "university",
        "advisor`id",
        "university id",
    )

    assert "`student-record`" in statement
    assert "child_tbl.`advisor``id`" in statement
    assert "parent_tbl.`university id`" in statement


def test_table_name_normalization_handles_db_prefix_and_case() -> None:
    assert dashboard_web._normalize_table_lookup_name("streaming_db.departments") == "departments"
    assert dashboard_web._normalize_table_lookup_name("`University`") == "university"


def test_table_availability_checks_normalized_names() -> None:
    existing = {"university", "placement"}
    assert dashboard_web._is_sql_table_available(existing, "streaming_db.university")
    assert dashboard_web._is_sql_table_available(existing, "`PLACEMENT`")
    assert not dashboard_web._is_sql_table_available(existing, "departments")
