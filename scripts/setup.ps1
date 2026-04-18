Param(
    [switch]$SkipTests
)

$ErrorActionPreference = 'Stop'

Write-Host '[1/5] Creating virtual environment (if missing)...'
if (-not (Test-Path '.venv')) {
    python -m venv .venv
}

$python = '.\\.venv\\Scripts\\python.exe'
if (-not (Test-Path $python)) {
    throw 'Virtual environment Python not found at .\\.venv\\Scripts\\python.exe'
}

Write-Host '[2/5] Installing dependencies...'
& $python -m pip install --upgrade pip
& $python -m pip install -r requirements.txt

Write-Host '[3/5] Preparing .env file...'
if (-not (Test-Path '.env')) {
    Copy-Item .env.example .env
    Write-Host 'Created .env from .env.example'
} else {
    Write-Host '.env already exists; keeping current values'
}

Write-Host '[4/5] Verifying backend connectivity...'
& $python db_connectivity_check.py

Write-Host '[5/5] Running setup validation...'
& $python verify_setup.py
if (-not $SkipTests) {
    & $python -m pytest
}

Write-Host 'Setup complete. Next: run scripts/start_services.ps1'
