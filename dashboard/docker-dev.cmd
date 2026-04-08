@echo off
setlocal

pushd "%~dp0\.." >nul
docker compose up --build dashboard-dev
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul
exit /b %EXIT_CODE%
