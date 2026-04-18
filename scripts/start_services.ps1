Param(
    [int]$ApiPort = 8002,
    [int]$DashboardPort = 8003
)

$ErrorActionPreference = 'Stop'

$python = '.\\.venv\\Scripts\\python.exe'
if (-not (Test-Path $python)) {
    throw 'Virtual environment not found. Run scripts/setup.ps1 first.'
}

if (-not (Test-Path '.env')) {
    Write-Warning '.env not found. Creating from .env.example with default values.'
    Copy-Item .env.example .env
}

Write-Host "Starting API on port $ApiPort ..."
Start-Process -WindowStyle Normal -FilePath $python -ArgumentList @('-m', 'uvicorn', 'schema_registry_api:app', '--reload', '--port', "$ApiPort") -WorkingDirectory (Get-Location)

Write-Host "Starting dashboard on port $DashboardPort ..."
Start-Process -WindowStyle Normal -FilePath $python -ArgumentList @('-m', 'uvicorn', 'dashboard_web:app', '--reload', '--port', "$DashboardPort") -WorkingDirectory (Get-Location)

Write-Host "API: http://127.0.0.1:$ApiPort"
Write-Host "Dashboard: http://127.0.0.1:$DashboardPort"
Write-Host 'Use Ctrl+C in each opened terminal window to stop services.'
