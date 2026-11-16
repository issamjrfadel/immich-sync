# PowerShell script to launch the Immich Multi-Sync Web UI
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProjectRoot = Split-Path -Parent $ScriptDir
Set-Location $ProjectRoot

$VenvPython = Join-Path $ProjectRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $VenvPython)) {
    Write-Error "Virtual environment python not found at $VenvPython."
    exit 1
}

Write-Host "Starting Immich Multi-Sync Web UI..." -ForegroundColor Cyan
& $VenvPython app.py
