from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path


def _import_dashboard_module(module_name: str):
    original_fastapi = sys.modules.get("fastapi")
    original_fastapi_responses = sys.modules.get("fastapi.responses")
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

    try:
        return importlib.import_module(module_name)
    finally:
        if original_fastapi is not None:
            sys.modules["fastapi"] = original_fastapi
        else:
            sys.modules.pop("fastapi", None)
        if original_fastapi_responses is not None:
            sys.modules["fastapi.responses"] = original_fastapi_responses
        else:
            sys.modules.pop("fastapi.responses", None)


dashboard = _import_dashboard_module("dashboard")


def test_dashboard_py_compatibility_exports_app() -> None:
    assert hasattr(dashboard, "app")
    assert dashboard.app is not None


def test_dashboard_enhancement_logical_presentation() -> None:
    query_record = dashboard.QueryRecord(
        query_input={"operation": "read", "fields": ["student_id", "name"]},
        status="ok",
        logical_result=[{"student_id": "STU-1", "name": "Alice"}],
        summary={
            "logical_plan": {
                "requested_fields": ["student_id", "name"],
                "resolved_fields": ["student_id", "name"],
                "missing_fields": [],
            },
            "explainability": [
                {"field": "student_id", "reason": "Exact field mapping", "status": "resolved"},
                {"field": "name", "reason": "Exact field mapping", "status": "resolved"},
            ],
        },
        timestamp="2026-04-18T10:00:00",
        duration_ms=12.3,
    )

    explainability_html = dashboard._render_query_explainability(query_record)
    result_html = dashboard._render_logical_result_table(query_record.logical_result)

    assert "Logical Plan View" in explainability_html
    assert "Explainability Badges" in explainability_html
    assert "Requested fields" in explainability_html
    lower_html = explainability_html.lower()
    assert "mysql" not in lower_html
    assert "mongodb" not in lower_html
    assert "select " not in lower_html
    assert " from " not in lower_html

    assert "<table class='result-table'>" in result_html
    assert "student_id" in result_html
    assert "Alice" in result_html
    assert "<h4>Logical result</h4>" in explainability_html
    assert "<table class='result-table'>" in explainability_html
    assert "<pre>[{" not in explainability_html


def test_performance_evaluation_benchmark_structure(tmp_path: Path) -> None:
    import performance_benchmark

    report = performance_benchmark.run_benchmark(
        runs=2,
        execute=False,
        dataset_path=Path("university_data.json").resolve(),
        metadata_file=tmp_path / "benchmark_metadata.json",
        registry_db=tmp_path / "benchmark_registry.db",
    )

    assert report["metadata"]["mode"] == "dry_run"
    assert report["metadata"]["runs"] == 2
    assert report["distribution"]["sql"] + report["distribution"]["mongo"] + report["distribution"]["buffer"] > 0

    for metric_name in [
        "ingestion",
        "logical_query",
        "metadata_lookup",
        "transaction_coordination_overhead",
    ]:
        metric = report[metric_name]
        assert "avg_latency_ms" in metric
        assert "p95_latency_ms" in metric
        assert "throughput_ops_per_sec" in metric
        assert metric["runs"] == 2


def test_comparative_analysis_tradeoff_outputs(tmp_path: Path) -> None:
    import comparative_evaluation

    report = comparative_evaluation.run_comparison(
        iterations=3,
        dataset=Path("university_data.json").resolve(),
        execute=False,
        registry_db=tmp_path / "comparison_registry.db",
        metadata_file=tmp_path / "comparison_metadata.json",
    )

    assert report["meta"]["mode"] == "dry_run"
    assert "overhead" in report
    assert "throughput_curve" in report
    assert "summary_table" in report
    assert len(report["summary_table"]) == 3

    overhead = report["overhead"]
    assert set(overhead.keys()) == {"read", "nested_read", "update"}
    assert "absolute_ms" in overhead["read"]
    assert "relative_percent" in overhead["read"]

    curve = report["throughput_curve"]
    assert len(curve["workloads"]) == len(curve["logical_ops_per_sec"]) == len(curve["direct_sql_ops_per_sec"])
    assert len(curve["workloads"]) == len(curve["direct_mongo_ops_per_sec"])
    assert curve["workloads"] == sorted(curve["workloads"])


def test_system_packaging_instructions_complete() -> None:
    readme = Path("README.md").read_text(encoding="utf-8")
    runbook = Path("docs/ASSIGNMENT4_RUNBOOK.md").read_text(encoding="utf-8")

    assert "pip install -r requirements.txt" in readme
    assert "MYSQL_HOST" in readme and "MONGO_HOST" in readme
    assert "uvicorn schema_registry_api:app" in readme
    assert "logical_dashboard_cli.py" in readme
    assert "uvicorn dashboard_web:app" in readme

    assert "db_connectivity_check.py" in runbook
    assert "university_ingest.py" in runbook
    assert "logical_dashboard_cli.py" in runbook
    assert "uvicorn dashboard_web:app" in runbook
