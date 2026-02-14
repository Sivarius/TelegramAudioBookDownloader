@echo off
setlocal
cd /d "%~dp0"

set "NO_PAUSE=0"
if /I "%~1"=="--no-pause" set "NO_PAUSE=1"

echo [1/4] Checking Python...
where python >nul 2>nul
if errorlevel 1 (
  echo Python is not found in PATH.
  echo Install Python 3.10+ and re-run this script.
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)

echo [2/4] Preparing virtual environment (.venv)...
if exist ".venv\Scripts\python.exe" (
  echo .venv already exists.
) else (
  python -m venv .venv
  if errorlevel 1 (
    echo Failed to create .venv
    if "%NO_PAUSE%"=="0" pause
    exit /b 1
  )
)

echo [3/4] Upgrading pip...
".venv\Scripts\python.exe" -m pip install --upgrade pip
if errorlevel 1 (
  echo Failed to upgrade pip
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)

echo [4/4] Installing dependencies from requirements.txt...
".venv\Scripts\python.exe" -m pip install -r requirements.txt
if errorlevel 1 (
  echo Failed to install dependencies
  if "%NO_PAUSE%"=="0" pause
  exit /b 1
)

echo.
echo Setup complete.
echo Start the web UI with start_web_ui.bat
if "%NO_PAUSE%"=="0" pause
