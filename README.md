# Warehouse_Analysis

Pipeline de forecasting operativo + dashboard web.

## Política oficial del repo

- Pipeline forecast: se mantiene en Python host.
- Web/dashboard: la única vía oficial de desarrollo y build es Docker.
- `weekly_direct` sigue siendo solo diagnóstico y no entra en la capa operativa.

## Instalación una sola vez

Necesitas:
- Docker Desktop o equivalente con `docker compose`
- Python 3.11+ para el pipeline

Preparación del pipeline:

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements.txt
```

En Windows PowerShell:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
```

## Flujo 1: pipeline + Docker

Caso de uso:
- has actualizado forecast o datos
- quieres regenerar todo y verlo en la web

1. Ejecutar pipeline:

```bash
python -m src.main --stage all
python -m src.main --stage consumption
```

En Windows:

```powershell
.\run-pipeline.cmd all
.\run-pipeline.cmd consumption
```

2. Levantar dashboard con Docker:

```bash
docker compose up --build dashboard-dev
```

Wrappers útiles:
- Windows PowerShell: [dashboard/docker-dev.ps1](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.ps1)
- Windows CMD: [dashboard/docker-dev.cmd](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.cmd)
- macOS/Linux: [dashboard/docker-dev.sh](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.sh)

Abrir:
- [http://localhost:5173](http://localhost:5173)

3. Parar la web:

```bash
docker compose down
```

## Flujo 2: solo Docker para la web

Caso de uso:
- no has tocado forecast
- ya tienes `outputs/consumption` actualizado
- solo quieres trabajar en frontend

Arrancar:

```bash
docker compose up --build dashboard-dev
```

Parar:

```bash
docker compose down
```

## Build oficial de la web

Comando oficial:

```bash
docker compose run --rm dashboard-build
```

Wrappers útiles:
- Windows PowerShell: [dashboard/docker-build.ps1](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.ps1)
- Windows CMD: [dashboard/docker-build.cmd](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.cmd)
- macOS/Linux: [dashboard/docker-build.sh](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.sh)

La build queda en:
- [dashboard/dist](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\dist)

No hace falta Node instalado en el host.

## Despliegue de la build

Puedes copiar:
- [dashboard/dist](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\dist)
- [dashboard/deploy/serve-dist.ps1](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\deploy\serve-dist.ps1)

En Windows:

```powershell
powershell -ExecutionPolicy Bypass -File .\dashboard\deploy\serve-dist.ps1 -Port 8080 -Root .\dashboard\dist
```

Abrir:
- [http://localhost:8080](http://localhost:8080)

## Datos que consume la web

La web sincroniza desde:
- `outputs/consumption`

Tablas principales:
- `consumo_forecast_diario`
- `consumo_forecast_semanal`
- `consumo_vs_2024_diario`
- `consumo_vs_2024_semanal`
- `consumo_progreso_actual`
- `dim_kpi`

## Docker del dashboard

Archivos relevantes:
- [docker-compose.yml](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\docker-compose.yml)
- [dashboard/Dockerfile](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\Dockerfile)
- [dashboard/scripts/docker-dashboard.sh](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\scripts\docker-dashboard.sh)

Notas:
- el contenedor monta el repo y lee `outputs/consumption`
- `node_modules` vive en volumen Docker para evitar problemas en Windows/macOS
- los scripts `npm` quedan como mecanismo interno del contenedor, no como flujo oficial del host
