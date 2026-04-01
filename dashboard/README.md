# Warehouse Analysis Dashboard (V1 - Forecast)

Dashboard web moderno para comparar `actual vs forecast vs 2024`.

## Stack

- React 19
- Vite
- TypeScript
- Tailwind CSS v4
- Recharts

## Estructura

```text
dashboard/
  deploy/
    serve-dist.ps1
  public/
    data/
  scripts/
    sync-consumption-data.mjs
  src/
    app/
    components/
    features/forecast/
    layouts/
    pages/
    services/
    styles/
    types/
    utils/
```

## Datos que consume Forecast

Origen principal:
- `../outputs/consumption`

Tablas:
- `consumo_forecast_diario`
- `consumo_forecast_semanal`
- `consumo_vs_2024_diario`
- `consumo_vs_2024_semanal`
- `consumo_progreso_actual`
- `dim_kpi`

`npm run sync:data`:
1. Lee JSON/CSV de `outputs/consumption`.
2. Genera/actualiza `public/data/*.json`.
3. Actualiza `public/data/_metadata.json`.

## Primera vez (solo frontend)

```bash
cd /Users/rubendiezllamas/Desktop/proyectos/Warehouse_Analysis/dashboard
npm install
```

## Comandos rapidos

### A) Pipeline + web (actualiza forecast y levanta dashboard)

```bash
cd /Users/rubendiezllamas/Desktop/proyectos/Warehouse_Analysis
source .venv/bin/activate
python -m src.main --stage all
python -m src.main --stage consumption
cd dashboard
npm run sync:data
npm run dev
```

Abrir:
- `http://localhost:5173`

### B) Solo web (si ya tienes los datos)

```bash
cd /Users/rubendiezllamas/Desktop/proyectos/Warehouse_Analysis/dashboard
npm run dev
```

Abrir:
- `http://localhost:5173`

### C) Compilar release

```bash
cd /Users/rubendiezllamas/Desktop/proyectos/Warehouse_Analysis/dashboard
npm run build:release
```

Salida:
- `dashboard/dist/`

### D) Ver build compilada en local

```bash
cd /Users/rubendiezllamas/Desktop/proyectos/Warehouse_Analysis/dashboard
npm run preview
```

Abrir:
- `http://localhost:4173`

Nota:
- `npm run dev` compila automaticamente en memoria con hot reload.

## Despliegue en Windows sin Node

Copiar al equipo destino:
- `dist/`
- `deploy/serve-dist.ps1`

Ejecutar en PowerShell (desde `deploy`):

```powershell
powershell -ExecutionPolicy Bypass -File .\serve-dist.ps1 -Port 8080 -Root ..\dist
```

Abrir:
- `http://localhost:8080`

## Scripts

- `npm run dev`: desarrollo Vite
- `npm run sync:data`: sincroniza outputs de consumo a `public/data`
- `npm run dev:with-data`: sync + dev
- `npm run build`: build frontend
- `npm run build:release`: sync + build
- `npm run preview`: servir build local (requiere Node)
