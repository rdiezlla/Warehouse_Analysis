# Warehouse Analysis Dashboard (V1)

Primera versión del dashboard web moderno para `Warehouse_Analysis`, centrada solo en el módulo **Forecast**:
- comparación `actual vs forecast vs 2024`
- tarjetas KPI operativas
- gráficas de líneas por KPI clave
- filtro multi-trimestre (Q1-Q4)

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
    data/                        # JSON consumidos por la app
  scripts/
    sync-consumption-data.mjs    # exporta/convierte capa consumo -> public/data
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

## Fuente de datos (Forecast)

La app consume, en orden de prioridad, tablas de la capa de consumo en:

`../outputs/consumption`

Tablas objetivo:
- `consumo_forecast_diario`
- `consumo_forecast_semanal`
- `consumo_vs_2024_diario`
- `consumo_vs_2024_semanal`
- `consumo_progreso_actual`
- `dim_kpi`

El script `npm run sync:data`:
1. lee `.json` o `.csv` de esas tablas
2. normaliza para frontend
3. genera `public/data/*.json`
4. crea `public/data/_metadata.json`

Si no encuentra una capa de consumo completa, genera un dataset demo para que la UI no se rompa.

## Desarrollo local (con Node)

```bash
cd dashboard
npm install
npm run sync:data
npm run dev
```

Alternativa directa:

```bash
npm run dev:with-data
```

## Build de producción

```bash
cd dashboard
npm run build:release
```

Salida estática:

`dashboard/dist/`

## Despliegue local en otro ordenador SIN Node (Windows)

1. Copia la carpeta `dashboard/dist` y `dashboard/deploy/serve-dist.ps1` al equipo destino.
2. Abre PowerShell en la carpeta `deploy`.
3. Lanza:

```powershell
powershell -ExecutionPolicy Bypass -File .\serve-dist.ps1 -Port 8080 -Root ..\dist
```

4. Abre el navegador en:

`http://localhost:8080`

El servidor usa `.NET HttpListener` (nativo de Windows/PowerShell), sin Node.

## Cómo actualizar datos cuando cambie el forecast

En el equipo de desarrollo (con Node):

1. Regenera capa de consumo del repo principal si aplica:

```bash
python -m src.main --stage consumption
```

2. Sincroniza los JSON del dashboard:

```bash
cd dashboard
npm run sync:data
```

3. Recompila:

```bash
npm run build:release
```

4. Copia de nuevo `dist/` al equipo sin Node.

## Notas de arquitectura para siguientes iteraciones

- Servicio de datos desacoplado (`src/services/forecastDataService.ts`) listo para migrar de ficheros a API.
- Tipado de tablas semanal incluido, aunque la V1 visualiza foco diario.
- Sidebar preparada con placeholders deshabilitados para futuros módulos.
