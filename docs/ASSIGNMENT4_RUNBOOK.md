# Assignment 4 Runbook

This runbook covers dashboard enhancement usage, benchmarking, comparative analysis, and final packaging steps.

## 1. Environment Setup

Quick automated path (recommended):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
```

1. Create virtual environment and activate:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
```

2. Install dependencies:

```powershell
pip install -r requirements.txt
```

3. Configure backend connection variables (optional for dry-run benchmarking):

- MySQL: `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE`
- MongoDB: `MONGO_HOST`, `MONGO_PORT`, `MONGO_DATABASE`
- Registry/metadata: `SCHEMA_REGISTRY_DB`, `METADATA_FILE`
- Optional API helper endpoint: `SCHEMA_REGISTRY_API_ENDPOINT` (example: `http://127.0.0.1:8004/crud_auto`)

## 2. Run Core Services

Quick automated service startup (optional):

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_services.ps1
```

1. Start schema registry and ingestion API:

```powershell
uvicorn schema_registry_api:app --reload --port 8002
```

If port `8002` is already in use, run on `8004` and keep all ingestion scripts pointed to the same endpoint:

```powershell
uvicorn schema_registry_api:app --reload --port 8004
$env:SCHEMA_REGISTRY_API_ENDPOINT = "http://127.0.0.1:8004/crud_auto"
```

2. Start logical dashboard:

```powershell
uvicorn dashboard_web:app --reload --port 8003
```

3. Open dashboard:

- http://127.0.0.1:8003

Dashboard pages for Assignment 4:
- Home (active session + logical query metrics)
- Entities (logical entities + field inspection + sample instances)
- CRUD (logical read/insert/update/delete)
- Query Monitor (latency/throughput and operation breakdown)
- Query History (executed logical request history)

Notes:
- Dashboard views are logical and schema-focused.
- Backend routing internals are intentionally omitted from primary user pages.

Ingestion command (execute mode):

```powershell
python university_ingest.py --execute --endpoint $env:SCHEMA_REGISTRY_API_ENDPOINT
```

## 3. Performance Benchmarking (Phase 2)

Dry-run mode (no backend writes):

```powershell
python performance_benchmark.py --runs 30
```

Live execution mode (requires SQL and Mongo):

```powershell
python performance_benchmark.py --runs 30 --execute
```

If Mongo is standalone (non-replica-set), transactional session writes are automatically retried without a Mongo session.

Artifacts are generated in `docs/perf_artifacts/`:
- `assignment4_perf_summary.json`
- `assignment4_perf_summary.csv`

Measured metrics:
- Ingestion latency
- Logical query latency
- Metadata lookup overhead
- Transaction coordination overhead
- Throughput
- Field distribution across storage backends

## 4. Comparative Evaluation (Phase 3)

Dry-run comparison (logical-layer only):

```powershell
python comparative_evaluation.py --iterations 20
```

Live backend comparison:

```powershell
python comparative_evaluation.py --iterations 20 --execute
```

Artifacts in `docs/perf_artifacts/`:
- `assignment4_comparison_comparison.json`
- `assignment4_comparison_metrics_table.csv`
- `assignment4_comparison_latency_bar.png`
- `assignment4_comparison_throughput_line.png`

Generated visualizations:
- Bar chart for latency comparisons
- Line chart for throughput under increasing workload
- Table summarizing logical vs direct latency and framework overhead

Notes:
- Direct-vs-logical values are populated in `--execute` mode.
- In dry-run mode, direct backend values are `null` and throughput is shown as `0.0`.

## 5. System Packaging Checklist (Phase 4)

1. Repository packaging:
- Push full source code to GitHub repository
- Include runbook, README, tests, and performance/comparison scripts
- Include generated artifacts under `docs/perf_artifacts/`

2. Dependency setup instructions:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
```

3. Backend configuration instructions:
- Set SQL and Mongo variables in `.env` (`MYSQL_*`, `MONGO_*`)
- Keep defaults if local services run on localhost with standard ports
- Validate connectivity:

```powershell
python db_connectivity_check.py
```

4. Run ingestion API:

```powershell
uvicorn schema_registry_api:app --reload --port 8002
```

5. Run ingestion workflow:

```powershell
python university_ingest.py --execute --endpoint http://127.0.0.1:8002/crud_auto
```

6. Run logical query interface:

```powershell
python logical_dashboard_cli.py --list-entities
python logical_dashboard_cli.py --query 1 --fields "name,university_name,city" --limit 5 --execute
```

API-based logical read option:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8002/schemas/1/crud" -ContentType "application/json" -Body '{"operation":"read","fields":["name","university_name","city"],"limit":5,"execute":true}'
```

7. Launch dashboard:

```powershell
uvicorn dashboard_web:app --reload --port 8003
```

Open http://127.0.0.1:8003

8. Reproducibility validation:

```powershell
python verify_setup.py
python -m pytest
python performance_benchmark.py --runs 10
python comparative_evaluation.py --iterations 10
```

9. Bundle expected submission assets:
- final report PDF (external export from your editor)
- demo video link
- GitHub repository link

## 6. Suggested Report Structure

1. Dashboard enhancements
2. Benchmark design and metrics
3. Comparative analysis and interpretation
4. Limitations and future work
5. Reproducibility and packaging details
