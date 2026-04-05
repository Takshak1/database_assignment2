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
_build_empty_read_reason = dashboard_web._build_empty_read_reason
_format_field_chips = dashboard_web._format_field_chips


def test_empty_reason_missing_fields_is_explicit() -> None:
    details = {
        "field_locations": [
            {"requested": "username", "status": "resolved", "storage": "sql"},
            {"requested": "unknown_field", "status": "missing", "notes": "field_not_found"},
        ],
        "sql": {"statement": "SELECT ..."},
        "result_summary": {"sql_rows": 0, "mongo_documents": 0, "merged_items": 0},
    }

    reason = _build_empty_read_reason(details)
    assert "Requested fields could not be resolved" in reason
    assert "unknown_field" in reason


def test_empty_reason_sql_only_zero_rows() -> None:
    details = {
        "field_locations": [
            {"requested": "username", "status": "resolved", "storage": "sql"},
        ],
        "sql": {"statement": "SELECT ..."},
        "result_summary": {"sql_rows": 0, "mongo_documents": 0, "merged_items": 0},
    }

    reason = _build_empty_read_reason(details)
    assert reason == "No SQL rows matched the current filters."


def test_empty_reason_merge_mismatch_sql_without_mongo_docs() -> None:
    details = {
        "field_locations": [
            {"requested": "username", "status": "resolved", "storage": "sql"},
            {"requested": "comments", "status": "resolved", "storage": "mongo"},
        ],
        "sql": {"statement": "SELECT ..."},
        "mongo": [{"collection": "post_comments", "filter": {}}],
        "merge": {"merge_key": "post_id"},
        "result_summary": {"sql_rows": 3, "mongo_documents": 0, "merged_items": 0},
    }

    reason = _build_empty_read_reason(details)
    assert "no matching Mongo documents" in reason
    assert "post_id" in reason


def test_hint_status_not_reported_as_missing_field() -> None:
    details = {
        "field_locations": [
            {"requested": "student_id", "status": "hint", "storage": "sql", "notes": "metadata_hint"},
            {"requested": "name", "status": "resolved", "storage": "sql"},
        ],
        "sql": {"statement": "SELECT ...", "where": "student.student_id = %s", "parameters": ["STU-2024-101"]},
        "result_summary": {"sql_rows": 0, "mongo_documents": 0, "merged_items": 0},
    }

    reason = _build_empty_read_reason(details)
    assert "could not be resolved" not in reason


def test_sql_zero_match_reason_uses_specific_hint_when_available(monkeypatch) -> None:
    details = {
        "field_locations": [
            {"requested": "student_id", "status": "hint", "storage": "sql"},
            {"requested": "name", "status": "resolved", "storage": "sql"},
        ],
        "sql": {"statement": "SELECT ...", "where": "student.student_id = %s", "parameters": ["STU-2024-101"]},
        "result_summary": {"sql_rows": 0, "mongo_documents": 0, "merged_items": 0},
    }

    monkeypatch.setattr(
        dashboard_web,
        "_describe_sql_zero_match_reason",
        lambda _details: "Specific SQL mismatch hint",
    )
    reason = _build_empty_read_reason(details)
    assert reason == "Specific SQL mismatch hint"


def test_field_chip_rendering_has_separators() -> None:
    html = _format_field_chips(["student_id", "name", "cgpa"])
    assert "</span> <span" in html
