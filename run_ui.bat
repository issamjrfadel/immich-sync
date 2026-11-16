@echo off
title Immich Multi-Sync UI
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found. Please install requirements first.
    pause
    exit /b 1
)

echo Starting Immich Multi-Sync Web UI...
".venv\Scripts\python.exe" app.py
pause
