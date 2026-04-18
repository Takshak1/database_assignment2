# Assignment 4 Performance Comparison Report

## Objective

This report compares the hybrid logical abstraction framework against direct backend access to quantify trade-offs in latency, throughput, and processing overhead.

## Experimental Setup

- Dataset: university_data.json
- Comparative runs: 20 iterations
- Benchmark runs: 30 iterations
- Mode: execute (live backends)
- Environment: local MySQL + MongoDB, framework execution via logical CRUD interface

## Experiment Scenarios

1. User retrieval:
- Retrieving user records through the logical query interface
- Direct SQL queries

2. Nested document access:
- Logical framework nested read
- Direct MongoDB read

3. Cross-entity updates:
- Logical framework update
- Direct mixed backend update

## Metrics Collected

- Query latency (average)
- Update latency (average)
- Throughput (operations per second)
- Framework overhead relative to direct access

## Results Summary

| Scenario | Logical Avg (ms) | Direct Avg (ms) | Overhead (ms) | Overhead (%) |
|---|---:|---:|---:|---:|
| User Retrieval (Logical vs Direct SQL) | 52.885 | 6.803 | 46.082 | 677.413 |
| Nested Access (Logical vs Direct Mongo) | 77.770 | 35.076 | 42.694 | 121.719 |
| Cross-Entity Update (Logical vs Direct Mixed) | 174.499 | 43.230 | 131.270 | 303.657 |

## Throughput Comparison

- Logical framework throughput: 9.831 ops/sec
- Direct access throughput: 35.249 ops/sec

Throughput under increasing workload (ops/sec):

| Workload Point | Logical | Direct |
|---:|---:|---:|
| 1 | 9.228 | 49.141 |
| 2 | 9.363 | 39.487 |
| 5 | 9.383 | 37.362 |
| 10 | 9.385 | 36.105 |
| 20 | 9.831 | 35.249 |

## Additional Benchmark Signals

From the benchmark suite (execute mode, 30 runs):

- Logical query avg latency: 62.750 ms
- Logical query p95 latency: 79.743 ms
- Ingestion avg latency: 129.298 ms
- Transaction coordination overhead avg: 5.171 ms

## Interpretation and Trade-offs

### Where abstraction introduces overhead

- User retrieval has large relative overhead (677.413%), mostly from planning, metadata-driven routing, merge handling, and result normalization compared to a single direct SQL statement.
- Nested access overhead remains significant (121.719%) because logical execution performs path resolution and abstraction-layer processing before backend reads.
- Cross-entity updates show high overhead (303.657%) due to coordination and multi-stage execution across entities/stores.

### Where abstraction simplifies development and improves accessibility

- One logical interface handles heterogeneous data access without requiring query rewrites per backend.
- Developers can request logical fields and nested paths directly, reducing backend-specific query complexity in application code.
- Cross-entity operations are expressed in a unified format, improving consistency and maintainability for teams that do not want to manage SQL and Mongo logic separately.
- Logical query history and explainability in the dashboard make behavior easier to inspect than ad-hoc direct queries.

## Visualizations and Artifacts

Generated artifacts:

- docs/perf_artifacts/assignment4_comparison_latency_bar.png
- docs/perf_artifacts/assignment4_comparison_throughput_line.png
- docs/perf_artifacts/assignment4_comparison_metrics_table.csv
- docs/perf_artifacts/assignment4_comparison_comparison.json
- docs/perf_artifacts/assignment4_perf_summary.csv
- docs/perf_artifacts/assignment4_perf_summary.json

## Conclusion

The hybrid logical abstraction layer provides substantial usability and development advantages, but it introduces measurable execution overhead versus direct database access. The direct path is preferable for latency-critical hot paths, while the logical layer is beneficial for productivity, portability, and schema-level accessibility.
