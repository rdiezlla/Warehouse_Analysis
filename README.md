# Warehouse_Analysis

Pipeline de forecasting operativo + dashboard web (modulo Forecast).

## 1) Requisitos

Maquina de desarrollo:
- Python 3.11+ (recomendado)
- Node.js 20+ y npm

Maquina de visualizacion sin Node:
- Solo navegador
- En Windows, PowerShell (usa `dashboard/deploy/serve-dist.ps1`)

Nota Windows:
- Si `npm` falla en PowerShell con `npm.ps1` bloqueado, usa `npm.cmd` o `dashboard\start-dev.cmd`.
- Para el pipeline, usa `.\.venv\Scripts\python.exe` o `run-pipeline.cmd`.

## 2) Primera instalacion (una sola vez)

Desde la raiz del repo:

```bash
python -m venv .venv
./.venv/Scripts/python.exe -m pip install -r requirements.txt
```

Instalar frontend:

```bash
cd dashboard
npm install
```

En Windows PowerShell:

```powershell
Set-Location .\dashboard
.\start-dev.cmd --install
```

## 3) Comandos rapidos

### A) Pipeline + web (actualiza forecast y levanta dashboard)

```bash
python -m src.main --stage all
python -m src.main --stage consumption
cd dashboard
npm run sync:data
npm run dev
```

En Windows PowerShell:

```powershell
.\run-pipeline.cmd all
.\run-pipeline.cmd consumption
Set-Location .\dashboard
.\start-dev.cmd
```

Abrir:
- `http://localhost:5173`

### B) Solo web (si ya tienes datos actualizados o no quieres recalcular forecast)

```bash
cd dashboard
npm run dev
```

En Windows PowerShell:

```powershell
Set-Location .\dashboard
.\start-dev.cmd
```

Abrir:
- `http://localhost:5173`

### C) Compilar web (release)

```bash
cd dashboard
npm run build:release
```

En Windows PowerShell:

```powershell
Set-Location .\dashboard
npm.cmd run build:release
```

Salida:
- `dashboard/dist/`

### D) Ver la build compilada en local

```bash
cd dashboard
npm run preview
```

En Windows PowerShell:

```powershell
Set-Location .\dashboard
npm.cmd run preview
```

Abrir:
- `http://localhost:4173`

Nota:
- `npm run dev` ya compila automaticamente en modo desarrollo (hot reload). No hace falta compilar manualmente cada vez.

## 4) Despliegue en otro ordenador sin Node (Windows)

En el PC de desarrollo, copia al PC destino:
- `dashboard/dist/`
- `dashboard/deploy/serve-dist.ps1`

En el PC destino (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -File .\serve-dist.ps1 -Port 8080 -Root ..\dist
```

Abrir:
- `http://localhost:8080`

## 5) Cuando se actualizan datos (operativa recomendada)

1. Ejecutar pipeline en el repo:

```bash
python -m src.main --stage all
python -m src.main --stage consumption
```

En Windows PowerShell:

```powershell
.\run-pipeline.cmd all
.\run-pipeline.cmd consumption
```

2. Sincronizar y recompilar dashboard:

```bash
cd dashboard
npm run sync:data
npm run build:release
```

En Windows PowerShell:

```powershell
Set-Location .\dashboard
npm.cmd run sync:data
npm.cmd run build:release
```

3. Copiar `dist/` al PC sin Node y arrancar con `serve-dist.ps1`.

## 6) Fuentes de datos del dashboard Forecast

El dashboard consume desde `outputs/consumption`:
- `consumo_forecast_diario`
- `consumo_forecast_semanal`
- `consumo_vs_2024_diario`
- `consumo_vs_2024_semanal`
- `consumo_progreso_actual`
- `dim_kpi`

Si faltan tablas minimas, `sync:data` genera un modo demo para evitar rotura visual.

## 7) Ubicacion de ingesta de Excel

La ingesta del pipeline usa los Excel del proyecto en:
- la raiz del repo (`ROOT`)
- `data/raw`

Principalmente:
- `Informacion_albaranaes.xlsx`
- `movimientos.xlsx`
- `lineas_solicitudes_con_pedidos.xlsx`
- `maestro_dimensiones_limpio.xlsx`
