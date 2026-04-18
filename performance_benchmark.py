"""Assignment 4 performance benchmarking suite.

Measures logical-layer performance for:
- ingestion latency
- logical query latency
- metadata lookup overhead
- transaction coordination overhead

The script can run in dry-run mode (default) or execute mode against live backends.
Outputs JSON and CSV artifacts in docs/perf_artifacts/.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Tuple

from crud_executor import HybridCRUDExecutor
from metadata_manager import MetadataManager
from schema_registry import SchemaRegistry

BASE_DIR = Path(__file__).resolve().parent
ARTIFACT_DIR = BASE_DIR / "docs" / "perf_artifacts"


@dataclass
class RunMetric:
    name: str
    latency_ms: float
    success: bool


def _percentile(values: List[float], pct: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return float(values[0])
    ordered = sorted(float(v) for v in values)
    rank = max(0, min(len(ordered) - 1, int(round((pct / 100.0) * (len(ordered) - 1)))))
    return float(ordered[rank])


def _summary(metrics: List[RunMetric]) -> Dict[str, Any]:
    latencies = [m.latency_ms for m in metrics]
    ok_count = sum(1 for m in metrics if m.success)
    total = len(metrics)
    avg_ms = statistics.fmean(latencies) if latencies else 0.0
    total_seconds = sum(latencies) / 1000.0 if latencies else 0.0
    throughput = (ok_count / total_seconds) if total_seconds > 0 else 0.0
    return {
        "runs": total,
        "success": ok_count,
        "failed": total - ok_count,
        "avg_latency_ms": round(avg_ms, 3),
        "p50_latency_ms": round(_percentile(latencies, 50), 3),
        "p95_latency_ms": round(_percentile(latencies, 95), 3),
        "throughput_ops_per_sec": round(throughput, 3),
    }


def _read_entity_schema(file_path: Path) -> Tuple[str, Dict[str, Any], List[Dict[str, Any]]]:
    with file_path.open("r", encoding="utf-8") as handle:
        payload = json.load(handle)

    if isinstance(payload, dict) and "records" in payload:
        records = payload["records"]
    elif isinstance(payload, list):
        records = payload
    else:
        records = [payload]

    if not records:
        raise ValueError("No records found in dataset file")

    sample = records[0]
    entity_name = "university_data"
    return entity_name, sample, records


def _register_entity(registry: SchemaRegistry, entity_name: str, schema_payload: Dict[str, Any]) -> int:
    existing = registry.list_schemas(entity_name)
    if existing:
        return int(existing[0]["schema_id"])
    stored = registry.register_schema(entity_name, schema_payload)
    return int(stored["schema_id"])


def _field_distribution(schema: Dict[str, Any]) -> Dict[str, int]:
    def _normalize_storage_backend(raw_value: Any) -> str:
        token = str(raw_value or "").strip().lower()
        if token in {"sql", "mysql", "relational", "rdbms"}:
            return "sql"
        if token in {"mongo", "mongodb", "document", "nosql", "embed", "embedded", "reference"}:
            return "mongo"
        if token in {"buffer", "cache", "queue"}:
            return "buffer"
        return "unknown"

    counts = {"sql": 0, "mongo": 0, "buffer": 0, "unknown": 0}
    storage_strategy = schema.get("storage_strategy") or {}
    field_mappings = storage_strategy.get("mappings", {}).get("fields", [])

    if field_mappings:
        for mapping in field_mappings:
            backend = _normalize_storage_backend(mapping.get("decision") or mapping.get("storage"))
            counts[backend] += 1
        return counts

    for field in schema.get("fields", []):
        backend = _normalize_storage_backend(field.get("storage_strategy") or field.get("decision") or field.get("storage"))
        counts[backend] += 1
    return counts


def _sanitize_write_payload(value: Any) -> Any:
    if isinstance(value, dict):
        sanitized: Dict[str, Any] = {}
        for key, item in value.items():
            if isinstance(item, list):
                continue
            if isinstance(item, dict):
                sanitized_item = _sanitize_write_payload(item)
                if sanitized_item:
                    sanitized[key] = sanitized_item
                continue
            sanitized[key] = item
        return sanitized
    return value


def _time_call(name: str, func) -> RunMetric:
    started = time.perf_counter()
    try:
        func()
        elapsed = (time.perf_counter() - started) * 1000.0
        return RunMetric(name=name, latency_ms=elapsed, success=True)
    except Exception:
        elapsed = (time.perf_counter() - started) * 1000.0
        return RunMetric(name=name, latency_ms=elapsed, success=False)


def run_benchmark(
    runs: int,
    execute: bool,
    dataset_path: Path,
    metadata_file: Path,
    registry_db: Path,
) -> Dict[str, Any]:
    entity_name, sample, records = _read_entity_schema(dataset_path)
    registry = SchemaRegistry(db_path=str(registry_db))
    executor = HybridCRUDExecutor(registry=registry, metadata_file=str(metadata_file))
    metadata_manager = MetadataManager(metadata_file=str(metadata_file))

    write_sample = _sanitize_write_payload(sample)
    write_records = [_sanitize_write_payload(record) for record in records] if records else [write_sample]

    schema_id = _register_entity(registry, entity_name, write_sample)
    schema = registry.get_schema(schema_id)

    ingest_metrics: List[RunMetric] = []
    query_metrics: List[RunMetric] = []
    metadata_metrics: List[RunMetric] = []
    tx_coord_metrics: List[RunMetric] = []

    selected_records = write_records[:runs] if len(write_records) >= runs else write_records
    if not selected_records:
        selected_records = [write_sample]

    for idx in range(runs):
        payload = selected_records[idx % len(selected_records)]

        ingest_metrics.append(
            _time_call(
                name="ingest",
                func=lambda p=payload: executor.execute(
                    schema_id,
                    operation="insert",
                    payload=p,
                    strategy="simple",
                    execute=execute,
                ),
            )
        )

        query_metrics.append(
            _time_call(
                name="logical_read",
                func=lambda: executor.execute(
                    schema_id,
                    operation="read",
                    fields=["id", "name", "university_name", "city"],
                    filters={},
                    limit=10,
                    execute=execute,
                ),
            )
        )

        metadata_metrics.append(
            _time_call(
                name="metadata_lookup",
                func=lambda: metadata_manager.field_metadata,
            )
        )

        original_tx = os.getenv("TRANSACTION_COORDINATION", "1")
        try:
            os.environ["TRANSACTION_COORDINATION"] = "1"
            with_tx = _time_call(
                name="tx_on",
                func=lambda p=payload: executor.execute(
                    schema_id,
                    operation="update",
                    payload=p,
                    filters={"id": payload.get("id")},
                    strategy="simple",
                    execute=execute,
                ),
            )

            os.environ["TRANSACTION_COORDINATION"] = "0"
            without_tx = _time_call(
                name="tx_off",
                func=lambda p=payload: executor.execute(
                    schema_id,
                    operation="update",
                    payload=p,
                    filters={"id": payload.get("id")},
                    strategy="simple",
                    execute=execute,
                ),
            )

            tx_coord_metrics.append(
                RunMetric(
                    name="transaction_overhead",
                    latency_ms=max(with_tx.latency_ms - without_tx.latency_ms, 0.0),
                    success=with_tx.success and without_tx.success,
                )
            )
        finally:
            os.environ["TRANSACTION_COORDINATION"] = original_tx

    return {
        "metadata": {
            "mode": "execute" if execute else "dry_run",
            "runs": runs,
            "dataset": str(dataset_path),
            "schema_id": schema_id,
            "entity_name": entity_name,
        },
        "distribution": _field_distribution(schema),
        "ingestion": _summary(ingest_metrics),
        "logical_query": _summary(query_metrics),
        "metadata_lookup": _summary(metadata_metrics),
        "transaction_coordination_overhead": _summary(tx_coord_metrics),
    }


def _write_outputs(report: Dict[str, Any], output_prefix: str) -> Dict[str, str]:
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    json_path = ARTIFACT_DIR / f"{output_prefix}_summary.json"
    csv_path = ARTIFACT_DIR / f"{output_prefix}_summary.csv"

    with json_path.open("w", encoding="utf-8") as handle:
        json.dump(report, handle, indent=2)

    rows = []
    for section in ["ingestion", "logical_query", "metadata_lookup", "transaction_coordination_overhead"]:
        metrics = report.get(section, {})
        row = {"metric": section}
        row.update(metrics)
        rows.append(row)

    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "metric",
                "runs",
                "success",
                "failed",
                "avg_latency_ms",
                "p50_latency_ms",
                "p95_latency_ms",
                "throughput_ops_per_sec",
            ],
        )
        writer.writeheader()
        for row in rows:
            writer.writerow(row)

    return {"json": str(json_path), "csv": str(csv_path)}


def main() -> int:
    parser = argparse.ArgumentParser(description="Assignment 4 performance benchmark")
    parser.add_argument("--runs", type=int, default=30, help="Number of benchmark iterations")
    parser.add_argument("--execute", action="store_true", help="Execute on live SQL/Mongo backends")
    parser.add_argument("--dataset", default="university_data.json", help="Input dataset path")
    parser.add_argument("--metadata", default=None, help="Metadata file path (default: docs/perf_artifacts/benchmark_metadata.json)")
    parser.add_argument("--registry", default=None, help="Schema registry DB path (default: docs/perf_artifacts/benchmark_schema_registry.db)")
    parser.add_argument("--output-prefix", default="assignment4_perf", help="Artifact filename prefix")

    args = parser.parse_args()

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    metadata_path = (BASE_DIR / args.metadata).resolve() if args.metadata else (ARTIFACT_DIR / "benchmark_metadata.json")
    registry_path = (BASE_DIR / args.registry).resolve() if args.registry else (ARTIFACT_DIR / "benchmark_schema_registry.db")

    if not metadata_path.exists():
        with metadata_path.open("w", encoding="utf-8") as handle:
            json.dump({}, handle)

    report = run_benchmark(
        runs=max(1, args.runs),
        execute=args.execute,
        dataset_path=(BASE_DIR / args.dataset).resolve(),
        metadata_file=metadata_path,
        registry_db=registry_path,
    )

    outputs = _write_outputs(report, args.output_prefix)
    print(json.dumps({"report": report, "artifacts": outputs}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
