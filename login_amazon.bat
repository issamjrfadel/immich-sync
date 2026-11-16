@echo off
title Amazon Photos Login Helper
cd /d "%~dp0"

if not exist ".venv\Scripts\python.exe" (
    echo [ERROR] Virtual environment not found.
    pause
    exit /b 1
)

echo ========================================================
echo   Immich Multi-Sync: Amazon Photos Login Helper
echo ========================================================
echo.
echo Launching automated Amazon Photos sign-in window...
echo.
".venv\Scripts\python.exe" -m src.clients.amazon_login %*
echo.
pause
