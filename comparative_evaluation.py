"""Assignment 4 comparative evaluation.

Compares logical abstraction latency/throughput against direct backend access:
- logical read vs direct SQL read
- logical nested access vs direct Mongo read
- logical update vs direct mixed update

Produces table outputs and plots in docs/perf_artifacts/.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import matplotlib.pyplot as plt

from crud_executor import HybridCRUDExecutor
from schema_registry import SchemaRegistry

try:
    import mysql.connector as mysql_connector
except Exception:  # pragma: no cover
    mysql_connector = None

try:
    from pymongo import MongoClient
except Exception:  # pragma: no cover
    MongoClient = None

BASE_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = BASE_DIR / "docs" / "perf_artifacts"


def _ms(start: float) -> float:
    return (time.perf_counter() - start) * 1000.0


def _avg(values: List[float]) -> float:
    return statistics.fmean(values) if values else 0.0


def _throughput(values_ms: List[float]) -> float:
    total_sec = sum(values_ms) / 1000.0 if values_ms else 0.0
    return (len(values_ms) / total_sec) if total_sec > 0 else 0.0


def _round3(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    return round(float(value), 3)


def _build_workload_points(iterations: int) -> List[int]:
    base_points = {
        1,
        2,
        5,
        max(1, iterations // 4),
        max(1, iterations // 2),
        max(1, iterations),
    }
    points = sorted(point for point in base_points if 1 <= point <= iterations)
    return points or [1]


def _calc_overhead(logical_ms: float, direct_ms: Optional[float]) -> Dict[str, Optional[float]]:
    if direct_ms is None or direct_ms <= 0:
        return {
            "absolute_ms": None,
            "relative_percent": None,
        }
    absolute = logical_ms - direct_ms
    relative = (absolute / direct_ms) * 100.0
    return {
        "absolute_ms": _round3(absolute),
        "relative_percent": _round3(relative),
    }


def _load_sample(dataset: Path) -> Dict[str, Any]:
    with dataset.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    if isinstance(payload, list):
        return payload[0]
    if isinstance(payload, dict) and "records" in payload and isinstance(payload["records"], list):
        return payload["records"][0]
    return payload


def _ensure_schema(registry: SchemaRegistry, sample: Dict[str, Any]) -> int:
    entity = "university_data"
    existing = registry.list_schemas(entity)
    if existing:
        return int(existing[0]["schema_id"])
    stored = registry.register_schema(entity, sample)
    return int(stored["schema_id"])


def _execute_direct_sql_read(executor: HybridCRUDExecutor, sql_plan: Optional[Dict[str, Any]], execute: bool) -> Optional[float]:
    if not execute or mysql_connector is None or not sql_plan:
        return None
    started = time.perf_counter()
    try:
        executor._execute_sql_select(sql_plan)
    except Exception:
        return None
    return _ms(started)


def _execute_direct_mongo_read(executor: HybridCRUDExecutor, mongo_plan: Optional[List[Dict[str, Any]]], execute: bool) -> Optional[float]:
    if not execute or MongoClient is None or not mongo_plan:
        return None
    started = time.perf_counter()
    try:
        executor._execute_mongo_reads(mongo_plan)
    except Exception:
        return None
    return _ms(started)


def _execute_direct_multi_update(
    executor: HybridCRUDExecutor,
    advanced_update_plan: Optional[Dict[str, Any]],
    execute: bool,
) -> Optional[float]:
    if not execute or mysql_connector is None or MongoClient is None or not advanced_update_plan:
        return None
    sql_updates = advanced_update_plan.get("sql") or []
    mongo_updates = advanced_update_plan.get("mongo") or []
    filters = advanced_update_plan.get("filters") or {}
    if not sql_updates and not mongo_updates:
        return None

    started = time.perf_counter()
    try:
        if sql_updates:
            executor._execute_sql_updates(sql_updates, filters)
        if mongo_updates:
            executor._execute_mongo_updates(mongo_updates, filters)
    except Exception:
        return None
    return _ms(started)


def _execute_direct_sql_update(
    executor: HybridCRUDExecutor,
    advanced_update_plan: Optional[Dict[str, Any]],
    execute: bool,
) -> Optional[float]:
    if not execute or mysql_connector is None or not advanced_update_plan:
        return None
    sql_updates = advanced_update_plan.get("sql") or []
    filters = advanced_update_plan.get("filters") or {}
    if not sql_updates:
        return None

    started = time.perf_counter()
    try:
        executor._execute_sql_updates(sql_updates, filters)
    except Exception:
        return None
    return _ms(started)


def _execute_direct_mongo_update(
    executor: HybridCRUDExecutor,
    advanced_update_plan: Optional[Dict[str, Any]],
    execute: bool,
) -> Optional[float]:
    if not execute or MongoClient is None or not advanced_update_plan:
        return None
    mongo_updates = advanced_update_plan.get("mongo") or []
    filters = advanced_update_plan.get("filters") or {}
    if not mongo_updates:
        return None

    started = time.perf_counter()
    try:
        executor._execute_mongo_updates(mongo_updates, filters)
    except Exception:
        return None
    return _ms(started)


def _workload_curve(
    points: List[int],
    logical_latencies: Dict[str, List[float]],
    direct_latencies: Dict[str, List[float]],
) -> Dict[str, Any]:
    scenarios = ["read", "nested_read", "update"]
    logical_curve: List[float] = []
    direct_sql_curve: List[float] = []
    direct_mongo_curve: List[float] = []
    direct_sql_update_curve: List[float] = []
    direct_mongo_update_curve: List[float] = []

    for point in points:
        logical_window: List[float] = []
        for scenario in scenarios:
            logical_window.extend(logical_latencies.get(scenario, [])[:point])

        logical_curve.append(round(_throughput(logical_window), 3))
        direct_sql_curve.append(round(_throughput(direct_latencies.get("read", [])[:point]), 3))
        direct_mongo_curve.append(round(_throughput(direct_latencies.get("nested_read", [])[:point]), 3))
        direct_sql_update_curve.append(round(_throughput(direct_latencies.get("sql_update", [])[:point]), 3))
        direct_mongo_update_curve.append(round(_throughput(direct_latencies.get("mongo_update", [])[:point]), 3))

    return {
        "workloads": points,
        "logical_ops_per_sec": logical_curve,
        "direct_sql_ops_per_sec": direct_sql_curve,
        "direct_mongo_ops_per_sec": direct_mongo_curve,
        "direct_sql_update_ops_per_sec": direct_sql_update_curve,
        "direct_mongo_update_ops_per_sec": direct_mongo_update_curve,
    }


def run_comparison(
    iterations: int,
    dataset: Path,
    execute: bool,
    registry_db: Path,
    metadata_file: Path,
) -> Dict[str, Any]:
    registry = SchemaRegistry(db_path=str(registry_db))
    executor = HybridCRUDExecutor(registry=registry, metadata_file=str(metadata_file))

    sample = _load_sample(dataset)
    schema_id = _ensure_schema(registry, sample)

    logical_read: List[float] = []
    direct_sql_read: List[float] = []
    logical_nested: List[float] = []
    direct_mongo_read: List[float] = []
    logical_update: List[float] = []
    direct_sql_update: List[float] = []
    direct_mongo_update: List[float] = []
    direct_update_combined: List[float] = []

    read_request = {
        "operation": "read",
        "fields": ["id", "name", "university_name", "city"],
        "filters": {},
        "limit": 10,
    }
    nested_request = {
        "operation": "read",
        "fields": ["faculty_members.specializations", "placements.top_recruiters"],
        "filters": {},
        "limit": 10,
    }
    update_request = {
        "operation": "update",
        "payload": sample,
        "filters": {"id": sample.get("id")},
        "strategy": "advanced",
    }

    read_plan = executor.query_engine.plan_query(schema_id, read_request)
    nested_plan = executor.query_engine.plan_query(schema_id, nested_request)
    advanced_update_plan = executor.query_engine.plan_query(schema_id, update_request)

    for _ in range(iterations):
        start = time.perf_counter()
        executor.execute(
            schema_id,
            operation="read",
            fields=read_request["fields"],
            filters={},
            limit=10,
            execute=execute,
        )
        logical_read.append(_ms(start))

        start = time.perf_counter()
        executor.execute(
            schema_id,
            operation="read",
            fields=nested_request["fields"],
            filters={},
            limit=10,
            execute=execute,
        )
        logical_nested.append(_ms(start))

        start = time.perf_counter()
        executor.execute(
            schema_id,
            operation="update",
            payload=sample,
            filters={"id": sample.get("id")},
            strategy="advanced",
            execute=execute,
        )
        logical_update.append(_ms(start))

        sql_ms = _execute_direct_sql_read(executor, read_plan.get("sql"), execute)
        if sql_ms is not None:
            direct_sql_read.append(sql_ms)

        mongo_ms = _execute_direct_mongo_read(executor, nested_plan.get("mongo"), execute)
        if mongo_ms is not None:
            direct_mongo_read.append(mongo_ms)

        sql_update_ms = _execute_direct_sql_update(executor, advanced_update_plan, execute)
        mongo_update_ms = _execute_direct_mongo_update(executor, advanced_update_plan, execute)
        if sql_update_ms is not None:
            direct_sql_update.append(sql_update_ms)
        if mongo_update_ms is not None:
            direct_mongo_update.append(mongo_update_ms)
        if sql_update_ms is not None and mongo_update_ms is not None:
            direct_update_combined.append(sql_update_ms + mongo_update_ms)

    workload_points = _build_workload_points(iterations)

    logical_latency = {
        "read": logical_read,
        "nested_read": logical_nested,
        "update": logical_update,
    }
    direct_latency = {
        "read": direct_sql_read,
        "nested_read": direct_mongo_read,
        "update": direct_update_combined,
        "sql_update": direct_sql_update,
        "mongo_update": direct_mongo_update,
    }

    logical_read_avg = _avg(logical_read)
    direct_sql_avg = _avg(direct_sql_read) if direct_sql_read else None
    logical_nested_avg = _avg(logical_nested)
    direct_mongo_avg = _avg(direct_mongo_read) if direct_mongo_read else None
    logical_update_avg = _avg(logical_update)
    direct_update_avg = _avg(direct_update_combined) if direct_update_combined else None
    direct_sql_update_avg = _avg(direct_sql_update) if direct_sql_update else None
    direct_mongo_update_avg = _avg(direct_mongo_update) if direct_mongo_update else None

    summary_table = [
        {
            "scenario": "User Retrieval (Logical vs Direct SQL)",
            "logical_avg_ms": _round3(logical_read_avg),
            "direct_avg_ms": _round3(direct_sql_avg),
            "overhead": _calc_overhead(logical_read_avg, direct_sql_avg),
        },
        {
            "scenario": "Nested Access (Logical vs Direct Mongo)",
            "logical_avg_ms": _round3(logical_nested_avg),
            "direct_avg_ms": _round3(direct_mongo_avg),
            "overhead": _calc_overhead(logical_nested_avg, direct_mongo_avg),
        },
        {
            "scenario": "Cross-Entity Update (Logical vs Direct Mixed)",
            "logical_avg_ms": _round3(logical_update_avg),
            "direct_avg_ms": _round3(direct_update_avg),
            "overhead": _calc_overhead(logical_update_avg, direct_update_avg),
        },
    ]

    throughput_curve = _workload_curve(workload_points, logical_latency, direct_latency)

    direct_sql_latencies_flat = direct_sql_read + direct_sql_update
    direct_mongo_latencies_flat = direct_mongo_read + direct_mongo_update

    return {
        "meta": {
            "iterations": iterations,
            "mode": "execute" if execute else "dry_run",
            "mysql_host": os.getenv("MYSQL_HOST", "localhost"),
            "mongo_host": os.getenv("MONGO_HOST", "localhost"),
        },
        "logical": {
            "read_avg_ms": _round3(logical_read_avg),
            "nested_read_avg_ms": _round3(logical_nested_avg),
            "update_avg_ms": _round3(logical_update_avg),
            "throughput_ops_per_sec": round(_throughput(logical_read + logical_nested + logical_update), 3),
        },
        "direct": {
            "sql_read_avg_ms": _round3(direct_sql_avg),
            "mongo_read_avg_ms": _round3(direct_mongo_avg),
            "sql_update_avg_ms": _round3(direct_sql_update_avg),
            "mongo_update_avg_ms": _round3(direct_mongo_update_avg),
            "mixed_update_avg_ms": _round3(direct_update_avg),
            "sql_throughput_ops_per_sec": round(_throughput(direct_sql_latencies_flat), 3) if direct_sql_latencies_flat else 0.0,
            "mongo_throughput_ops_per_sec": round(_throughput(direct_mongo_latencies_flat), 3) if direct_mongo_latencies_flat else 0.0,
        },
        "overhead": {
            "read": _calc_overhead(logical_read_avg, direct_sql_avg),
            "nested_read": _calc_overhead(logical_nested_avg, direct_mongo_avg),
            "update": _calc_overhead(logical_update_avg, direct_update_avg),
        },
        "throughput_curve": throughput_curve,
        "summary_table": summary_table,
    }


def _plot(report: Dict[str, Any], output_prefix: str) -> Dict[str, str]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    bar_path = ARTIFACT_DIR / f"{output_prefix}_latency_bar.png"
    line_path = ARTIFACT_DIR / f"{output_prefix}_throughput_line.png"
    table_path = ARTIFACT_DIR / f"{output_prefix}_metrics_table.csv"
    json_path = ARTIFACT_DIR / f"{output_prefix}_comparison.json"

    labels = [
        "Logical Read",
        "Direct SQL Read",
        "Logical Nested",
        "Direct Mongo Read",
        "Logical Update",
        "Direct Mixed Update",
    ]
    values = [
        report["logical"].get("read_avg_ms", 0.0),
        report["direct"].get("sql_read_avg_ms") or 0.0,
        report["logical"].get("nested_read_avg_ms", 0.0),
        report["direct"].get("mongo_read_avg_ms") or 0.0,
        report["logical"].get("update_avg_ms", 0.0),
        report["direct"].get("mixed_update_avg_ms") or 0.0,
    ]

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(labels, values, color=["#0ea5e9", "#f97316", "#14b8a6", "#a16207", "#6366f1", "#ef4444"])
    ax.set_ylabel("Latency (ms)")
    ax.set_title("Assignment 4 Comparative Query Latency")
    ax.tick_params(axis="x", rotation=25)
    fig.tight_layout()
    fig.savefig(bar_path, dpi=160)
    plt.close(fig)

    fig, ax = plt.subplots(figsize=(8, 4))
    curve = report.get("throughput_curve", {})
    workloads = curve.get("workloads") or [1]
    logical_series = curve.get("logical_ops_per_sec") or [report["logical"].get("throughput_ops_per_sec", 0.0)]
    direct_sql_series = curve.get("direct_sql_ops_per_sec") or [0.0]
    direct_mongo_series = curve.get("direct_mongo_ops_per_sec") or [0.0]
    ax.plot(workloads, logical_series, marker="o", linewidth=2, color="#0f766e", label="Logical")
    ax.plot(workloads, direct_sql_series, marker="^", linewidth=2, color="#0369a1", label="Direct SQL")
    ax.plot(workloads, direct_mongo_series, marker="d", linewidth=2, color="#7c3aed", label="Direct MongoDB")
    ax.set_xlabel("Workload (operations per scenario)")
    ax.set_ylabel("Throughput (ops/sec)")
    ax.set_title("Throughput Under Increasing Workload (Framework vs SQL vs MongoDB)")
    ax.grid(True, linestyle="--", alpha=0.4)
    ax.legend()
    fig.tight_layout()
    fig.savefig(line_path, dpi=160)
    plt.close(fig)

    with table_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "scenario",
                "logical_avg_ms",
                "direct_avg_ms",
                "overhead_abs_ms",
                "overhead_percent",
            ],
        )
        writer.writeheader()
        for row in report.get("summary_table", []):
            overhead = row.get("overhead") or {}
            writer.writerow(
                {
                    "scenario": row.get("scenario"),
                    "logical_avg_ms": row.get("logical_avg_ms"),
                    "direct_avg_ms": row.get("direct_avg_ms"),
                    "overhead_abs_ms": overhead.get("absolute_ms"),
                    "overhead_percent": overhead.get("relative_percent"),
                }
            )

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    return {
        "json": str(json_path),
        "table_csv": str(table_path),
        "latency_bar": str(bar_path),
        "throughput_line": str(line_path),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Assignment 4 comparative evaluation")
    parser.add_argument("--iterations", type=int, default=20)
    parser.add_argument("--dataset", default="university_data.json")
    parser.add_argument("--execute", action="store_true", help="Run direct backend measurements")
    parser.add_argument("--metadata", default=None, help="Metadata file path (default: docs/perf_artifacts/benchmark_metadata.json)")
    parser.add_argument("--registry", default=None, help="Schema registry DB path (default: docs/perf_artifacts/benchmark_schema_registry.db)")
    parser.add_argument("--output-prefix", default="assignment4_comparison")

    args = parser.parse_args()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = (BASE_DIR / args.metadata).resolve() if args.metadata else (ARTIFACT_DIR / "benchmark_metadata.json")
    registry_path = (BASE_DIR / args.registry).resolve() if args.registry else (ARTIFACT_DIR / "benchmark_schema_registry.db")

    if not metadata_path.exists():
        with metadata_path.open("w", encoding="utf-8") as handle:
            json.dump({}, handle)

    report = run_comparison(
        iterations=max(1, args.iterations),
        dataset=(BASE_DIR / args.dataset).resolve(),
        execute=args.execute,
        registry_db=registry_path,
        metadata_file=metadata_path,
    )
    artifacts = _plot(report, args.output_prefix)
    print(json.dumps({"report": report, "artifacts": artifacts}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
