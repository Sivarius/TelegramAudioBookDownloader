@echo off
setlocal
cd /d "%~dp0"

echo Starting TeleGaParser Web UI...

if exist ".venv\Scripts\python.exe" (
  set "PYTHON_EXE=.venv\Scripts\python.exe"
) else (
  echo Virtual environment is missing. Running dependency setup...
  call install_dependencies.bat --no-pause
  if errorlevel 1 (
    echo.
    echo Setup failed. Cannot continue.
    pause
    exit /b 1
  )

  if exist ".venv\Scripts\python.exe" (
    set "PYTHON_EXE=.venv\Scripts\python.exe"
  ) else (
    echo.
    echo .venv\Scripts\python.exe not found after setup.
    pause
    exit /b 1
  )
)

if not exist ".env" (
  if exist ".env.example" (
    copy /Y ".env.example" ".env" >nul
    echo Created .env from .env.example
  )
)

%PYTHON_EXE% src\web_app.py
if errorlevel 1 (
  echo.
  echo Web UI exited with error.
  exit /b 1
)
