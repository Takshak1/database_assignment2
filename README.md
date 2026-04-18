# Adaptive Hybrid Database Framework

This project implements a logical database layer over SQL and MongoDB with metadata-driven routing, automatic schema analysis, query planning, and hybrid CRUD execution.

## Assignment Coverage

- Requirement 1: Ingestion and normalization
- Requirement 2: SQL table/key generation
- Requirement 3: Mongo strategy (embed vs reference)
- Requirement 4: Metadata system and structural registry
- Requirement 5: CRUD query generation
- Requirement 6: Performance considerations
- Assignment 4: Dashboard enhancement, benchmarking, comparative analysis, and final packaging

## Core Components

- main.py: orchestration entry point
- schema_registry.py: schema and metadata persistence
- schema_registry_api.py: FastAPI registry and CRUD endpoints
- crud_query_engine.py: logical query planning
- crud_executor.py: hybrid CRUD execution across backends
- result_aggregator.py: logical merge of SQL and Mongo results
- dashboard_web.py: logical web dashboard
- logical_dashboard_cli.py: logical CLI dashboard
- performance_benchmark.py: Assignment 4 benchmark suite
- comparative_evaluation.py: Assignment 4 direct-vs-logical comparison with plots

## Quick Start

1. Create and activate virtual environment:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Run API:

```powershell
uvicorn schema_registry_api:app --reload --port 8002
```

If `8002` is occupied, run the API on `8004` and point ingestion helpers to the same endpoint:

```powershell
uvicorn schema_registry_api:app --reload --port 8004
$env:SCHEMA_REGISTRY_API_ENDPOINT = "http://127.0.0.1:8004/crud_auto"
```

4. Run dashboard:

```powershell
uvicorn dashboard_web:app --reload --port 8003
```

5. Open dashboard:

- http://127.0.0.1:8003

## Final System Packaging (Reproducible Setup)

Use this section for a clean machine setup with minimal configuration.

### 1. Clone and install

```powershell
git clone <your-github-repo-url>
Set-Location database_assignment2
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### 2. Configure environment

```powershell
Copy-Item .env.example .env
```

Update `.env` values only if your local MySQL/MongoDB credentials differ.

### 3. Start SQL and MongoDB backends

Minimum requirement:
- MySQL server reachable at `MYSQL_HOST:3306`
- MongoDB server reachable at `MONGO_HOST:MONGO_PORT`

Run connectivity check:

```powershell
python db_connectivity_check.py
```

### 4. Start ingestion API

```powershell
uvicorn schema_registry_api:app --reload --port 8002
```

### 5. Ingest initial dataset

In another terminal:

```powershell
python university_ingest.py --execute --endpoint http://127.0.0.1:8002/crud_auto
```

### 6. Run logical query interface

Logical CLI query example:

```powershell
python logical_dashboard_cli.py --list-entities
python logical_dashboard_cli.py --query 1 --fields "name,university_name,city" --limit 5 --execute
```

Logical API query example:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8002/schemas/1/crud" -ContentType "application/json" -Body '{"operation":"read","fields":["name","university_name","city"],"limit":5,"execute":true}'
```

### 7. Launch dashboard

```powershell
uvicorn dashboard_web:app --reload --port 8003
```

Open:
- http://127.0.0.1:8003

### 8. Reproducibility validation

```powershell
python verify_setup.py
python -m pytest
```

## Rebuild From Scratch 

Use this section to fully remake the project on a new machine.

### Requirement Mapping 

1. Source code repository (GitHub)
- Push this folder to GitHub.
- Include all code, `docs/`, `scripts/`, and `tests/`.

2. Setup instructions for dependencies
- Run `powershell -ExecutionPolicy Bypass -File .\\scripts\\setup.ps1`
- Or manual: create `.venv`, install `requirements.txt`.

3. Configure SQL and MongoDB backends
- Copy `.env.example` to `.env`.
- Set `MYSQL_*` and `MONGO_*` only if local defaults differ.
- Validate with `python db_connectivity_check.py`.

4. Run the ingestion API
- `python -m uvicorn schema_registry_api:app --reload --port 8002`

5. Run the logical query interface
- CLI: `python logical_dashboard_cli.py --list-entities`
- CLI read example:
	`python logical_dashboard_cli.py --query 1 --fields "name,university_name,city" --limit 5 --execute`

6. Launch the dashboard
- `python -m uvicorn dashboard_web:app --reload --port 8003`
- Open `http://127.0.0.1:8003`

### Fast Reproduction Sequence

```powershell
git clone <your-github-repo-url>
Set-Location database_assignment2
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\start_services.ps1
```

### Optional Validation Pack

```powershell
python verify_setup.py
python -m pytest
python performance_benchmark.py --runs 10 --execute
python comparative_evaluation.py --iterations 10 --execute
```

## Dashboard Features 

The dashboard exposes logical-only information and avoids backend implementation details.

- Active session overview
- Logical entity list
- Entity field inspection
- Sample logical instances
- CRUD execution result display
- Query execution history
- Query monitor with latency and throughput metrics
- Logical routing explainability and plan summaries

## API Endpoints

- POST /register_schema
- GET /schemas
- GET /schemas/{schema_id}
- POST /schemas/{schema_id}/query_plan
- POST /schemas/{schema_id}/crud
- POST /crud_auto
- POST /ingest/{schema_id}
- POST /reset_registry

## Performance Evaluation 

Run dry-run benchmark:

```powershell
python performance_benchmark.py --runs 30
```

Run live benchmark:

```powershell
python performance_benchmark.py --runs 30 --execute
```

Metrics collected:

- ingestion latency
- logical query latency
- metadata lookup overhead
- transaction coordination overhead
- average and p95 latency
- throughput (ops/sec)
- field distribution across storage backends

Outputs:

- docs/perf_artifacts/assignment4_perf_summary.json
- docs/perf_artifacts/assignment4_perf_summary.csv

## Comparative Analysis 

Run dry-run comparison:

```powershell
python comparative_evaluation.py --iterations 20
```

Run live comparison:

```powershell
python comparative_evaluation.py --iterations 20 --execute
```

Outputs:

- docs/perf_artifacts/assignment4_comparison_comparison.json
- docs/perf_artifacts/assignment4_comparison_metrics_table.csv
- docs/perf_artifacts/assignment4_comparison_latency_bar.png
- docs/perf_artifacts/assignment4_comparison_throughput_line.png

Comparative report fields include:

- scenario latency (logical vs direct)
- query processing overhead introduced by abstraction
- throughput under increasing workload

## Final Packaging 

Detailed packaging and reproducibility instructions are in:

- docs/ASSIGNMENT4_RUNBOOK.md
- docs/assignment4_report_template.md
- docs/SYSTEM_PACKAGE.md

Minimal-effort setup and startup helpers:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
powershell -ExecutionPolicy Bypass -File .\scripts\start_services.ps1
```

## Testing

Run all tests:

```powershell
python -m pytest
```

Run selected dashboard tests:

```powershell
python -m pytest tests/test_dashboard_empty_reason.py tests/test_dashboard_fk_query.py
```

## Environment Variables

- SCHEMA_REGISTRY_DB (default: schema_registry.db)
- METADATA_FILE (default: metadata.json)
- DASHBOARD_EXECUTE (default: 0)
- MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
- MONGO_HOST, MONGO_PORT, MONGO_DATABASE
- TRANSACTION_COORDINATION (default: 1)
- SCHEMA_REGISTRY_API_ENDPOINT (default: http://127.0.0.1:8002/crud_auto)

## Notes

- Use execute mode only when MySQL and MongoDB are running and reachable.
- Dry-run mode is suitable for validating logical planning and routing behavior without backend writes.
