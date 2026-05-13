@echo off
setlocal

set "REPO_ROOT=%~dp0"
set "PYTHON_EXE=%REPO_ROOT%.venv\Scripts\python.exe"
set "STAGE=%~1"

if "%STAGE%"=="" set "STAGE=consumption"

if not exist "%PYTHON_EXE%" (
  echo No se encontro la venv en "%PYTHON_EXE%". Crea primero el entorno con: python -m venv .venv
  exit /b 1
)

pushd "%REPO_ROOT%" >nul
"%PYTHON_EXE%" -m src.main --stage %STAGE%
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %EXIT_CODE%
