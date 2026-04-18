# Complete Reproducible Package Guide

This guide provides a minimal-effort path for any user to install and run the hybrid framework.

## 1) Source Code Repository (GitHub)

1. Create a GitHub repository.
2. Push this project root (`database_assignment2`) to that repository.
3. Share the repository URL with users.

Example push flow:

```powershell
git init
git add .
git commit -m "Initial reproducible package"
git branch -M main
git remote add origin <your-github-repo-url>
git push -u origin main
```

## 2) Setup Dependencies

From the project root:

```powershell
Set-Location database_assignment2
powershell -ExecutionPolicy Bypass -File .\scripts\setup.ps1
```

This script:
- creates `.venv` if needed
- installs `requirements.txt`
- creates `.env` from `.env.example` if missing
- runs connectivity and setup checks
- runs tests

## 3) Configure SQL and MongoDB Backends

Update `.env` only if local credentials differ from defaults:

```env
MYSQL_HOST=localhost
MYSQL_USER=root
MYSQL_PASSWORD=change_me
MYSQL_DATABASE=streaming_db

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=streaming_db
```

Verify connectivity:

```powershell
.\.venv\Scripts\python.exe db_connectivity_check.py
```

Expected output for healthy setup:
- `MYSQL_OK`
- `MONGO_OK`

## 4) Run the Ingestion API

```powershell
.\.venv\Scripts\python.exe -m uvicorn schema_registry_api:app --reload --port 8002
```

Then ingest sample data (new terminal):

```powershell
.\.venv\Scripts\python.exe university_ingest.py --execute --endpoint http://127.0.0.1:8002/crud_auto
```

## 5) Run the Logical Query Interface

CLI examples:

```powershell
.\.venv\Scripts\python.exe logical_dashboard_cli.py --list-entities
.\.venv\Scripts\python.exe logical_dashboard_cli.py --query 1 --fields "name,university_name,city" --limit 5 --execute
```

API example:

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8002/schemas/1/crud" -ContentType "application/json" -Body '{"operation":"read","fields":["name","university_name","city"],"limit":5,"execute":true}'
```

## 6) Launch the Dashboard

```powershell
.\.venv\Scripts\python.exe -m uvicorn dashboard_web:app --reload --port 8003
```

Open: http://127.0.0.1:8003

Dashboard quick checks:
- ACID report: http://127.0.0.1:8003/acid
- Connection test: http://127.0.0.1:8003/connections

## 7) Minimal One-Command Service Startup (Optional)

After setup, start both API and dashboard automatically:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start_services.ps1
```

## 8) Validation for Reproducibility

```powershell
.\.venv\Scripts\python.exe verify_setup.py
.\.venv\Scripts\python.exe -m pytest
.\.venv\Scripts\python.exe performance_benchmark.py --runs 10 --execute
.\.venv\Scripts\python.exe comparative_evaluation.py --iterations 10 --execute
```

Generated artifacts are stored under `docs/perf_artifacts/`.

## 9) Completion Notes

- This package is intended to run with minimal configuration using defaults in `.env.example`.
- Users only need to edit `.env` when local SQL/Mongo credentials differ.
