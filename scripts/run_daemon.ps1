# Windows PowerShell script to launch Immich Multi-Sync Daemon in background
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Error "Virtual environment python not found at $VenvPython. Run python -m venv .venv and pip install -r requirements.txt first."
    exit 1
}

Write-Host "Starting Immich Multi-Sync Daemon..." -ForegroundColor Cyan
& $VenvPython sync.py daemon
