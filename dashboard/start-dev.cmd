@echo off
setlocal

set "DASHBOARD_ROOT=%~dp0"
set "INSTALL=false"
set "SKIP_SYNC=false"

:parse_args
if "%~1"=="" goto run
if /I "%~1"=="--install" (
  set "INSTALL=true"
  shift
  goto parse_args
)
if /I "%~1"=="--skip-sync" (
  set "SKIP_SYNC=true"
  shift
  goto parse_args
)
echo Argumento no reconocido: %~1
exit /b 1

:run
pushd "%DASHBOARD_ROOT%" >nul

if /I "%INSTALL%"=="true" (
  call npm.cmd install
  if errorlevel 1 goto end
)

if /I not "%SKIP_SYNC%"=="true" (
  call npm.cmd run sync:data
  if errorlevel 1 goto end
)

call npm.cmd run dev

:end
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%
