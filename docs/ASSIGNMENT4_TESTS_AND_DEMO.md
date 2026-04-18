# Assignment 4 Focused Tests and Demo Guide

This guide documents only the required verification areas:

1. Dashboard enhancement usability and logical data presentation
2. Performance evaluation quality of benchmarking experiments
3. Comparative analysis of abstraction vs performance trade-offs
4. System packaging completeness and reproducibility

## Kept Test Files

- `tests/test_requirement7_marking_criteria.py`
- `tests/test_dashboard_empty_reason.py`
- `tests/test_architecture_random_end_to_end.py`

These files cover all required areas, plus one full random-dataset architecture verification.

## What Each Test File Validates

### 1) Dashboard Enhancement Usability and Logical Data Presentation

Covered by:

- `test_dashboard_py_compatibility_exports_app`
- `test_dashboard_enhancement_logical_presentation`
- all `test_empty_reason_*` tests
- `test_field_chip_rendering_has_separators`

Validation focus:

- Dashboard app importability
- Logical-plan and explainability rendering
- No backend-specific SQL/Mongo leakage in user-facing explainability text
- Clear empty-result messaging for unresolved fields, zero SQL matches, merge mismatch

### 2) Performance Evaluation Quality of Benchmarking Experiments

Covered by:

- `test_performance_evaluation_benchmark_structure`

Validation focus:

- ingestion latency metric
- logical query latency metric
- metadata lookup overhead metric
- transaction coordination overhead metric
- average and p95 latency fields
- throughput field
- backend distribution is populated (sql/mongo/buffer not all zero)

### 3) Comparative Analysis Trade-offs

Covered by:

- `test_comparative_analysis_tradeoff_outputs`

Validation focus:

- logical vs direct sections
- overhead table for read/nested_read/update
- throughput curve structure and ordering
- summary table completeness

### 4) System Packaging Completeness and Reproducibility

Covered by:

- `test_system_packaging_instructions_complete`

Validation focus:

- setup instructions in README and runbook
- backend env variables present
- API, CLI, and dashboard startup commands documented

## How to Run the Focused Tests

From `D:\A02\database_assignment2`:

```powershell
..\.venv\Scripts\python.exe -m pytest tests\test_requirement7_marking_criteria.py tests\test_dashboard_empty_reason.py tests\test_architecture_random_end_to_end.py -q
```

## Full Random JSON End-to-End Architecture Verification

Covered by:

- `tests/test_architecture_random_end_to_end.py`

Validation flow in this test:

1. Generates a random nested JSON dataset (students, faculty, placements, contact).
2. Registers the random schema via API (`/register_schema`).
3. Ingests all random records through API (`/ingest/{schema_id}`).
4. Builds logical query plans (`/schemas/{schema_id}/query_plan`).
5. Verifies insert/read/update/delete planning via `/schemas/{schema_id}/crud`.
6. Verifies auto-registration path via `/crud_auto`.
7. Confirms metadata persistence and placement decisions.
8. Runs dry-run benchmark and comparative-evaluation over the random dataset.

Run it alone:

```powershell
..\.venv\Scripts\python.exe -m pytest tests\test_architecture_random_end_to_end.py -q
```

## Live Execute Variant (First Variant, Gated)

The same file also includes a live `execute=True` variant:

- `test_random_json_database_end_to_end_architecture_live_execute`

This test is intentionally gated behind an environment flag so normal CI/local runs remain stable.

Enable and run:

```powershell
$env:RUN_LIVE_E2E = "1"
..\.venv\Scripts\python.exe -m pytest tests\test_architecture_random_end_to_end.py -q
```

Notes:

- If the flag is not set, the live variant is skipped.
- If MySQL or MongoDB is unreachable, the live variant self-skips with a clear reason.

## Demo Presentation Flow

Use this sequence in your demo.

### Step 1: Show Focused Test Scope

- Open `tests/test_requirement7_marking_criteria.py`
- Open `tests/test_dashboard_empty_reason.py`
- Explain that only required grading criteria are kept

### Step 2: Run Tests Live

```powershell
..\.venv\Scripts\python.exe -m pytest tests\test_requirement7_marking_criteria.py tests\test_dashboard_empty_reason.py tests\test_architecture_random_end_to_end.py -q
```

Show:

- all tests pass
- each requirement has explicit assertions
- random JSON end-to-end architecture test passes

### Step 3: Run Performance Benchmark (Execute Mode)

```powershell
..\.venv\Scripts\python.exe performance_benchmark.py --runs 10 --execute
```

Show in terminal output / artifacts:

- ingestion avg and p95 latency
- logical query avg and p95 latency
- metadata lookup overhead
- transaction coordination overhead
- throughput values
- backend distribution (`sql`, `mongo`, `buffer`, `unknown`)

Artifacts generated:

- `docs/perf_artifacts/assignment4_perf_summary.json`
- `docs/perf_artifacts/assignment4_perf_summary.csv`

### Step 4: Run Comparative Evaluation (Execute Mode)

```powershell
..\.venv\Scripts\python.exe comparative_evaluation.py --iterations 10 --execute
```

Show:

- logical vs direct average latency
- overhead absolute and relative percentages
- throughput curve logical vs direct

Artifacts generated:

- `docs/perf_artifacts/assignment4_comparison_comparison.json`
- `docs/perf_artifacts/assignment4_comparison_metrics_table.csv`
- `docs/perf_artifacts/assignment4_comparison_latency_bar.png`
- `docs/perf_artifacts/assignment4_comparison_throughput_line.png`

### Step 5: Show Packaging and Reproducibility

- Open `README.md`
- Open `docs/ASSIGNMENT4_RUNBOOK.md`

Highlight:

- dependency install
- environment variables
- API start command
- logical CLI command
- dashboard start command

## Optional Dashboard Start Command (for demo visuals)

Use explicit app directory to avoid import path issues:

```powershell
D:\A02\.venv\Scripts\python.exe -m uvicorn dashboard_web:app --app-dir D:\A02\database_assignment2 --port 8003
```

Open:

- `http://127.0.0.1:8003`
