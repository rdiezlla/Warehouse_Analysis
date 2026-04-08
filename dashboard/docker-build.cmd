@echo off
setlocal

pushd "%~dp0\.." >nul
docker compose run --rm dashboard-build
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%
