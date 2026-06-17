# Warehouse Analysis

Aplicacion local modular para analisis operativo de almacen con tres modulos:

- Forecast
- ABC
- Market Basket

El flujo principal es Docker. No hace falta ejecutar `pnpm install`, `pnpm run dev` ni instalar dependencias Python en el host para el uso normal.

## Comandos Principales

Levantar dashboard:

```bash
docker compose up --build dashboard-dev
```

La URL local es:

```text
http://localhost:5173/
```

Ejecutar todos los pipelines:

```bash
docker compose run --rm pipeline python -m src.pipelines.run_all
```

Ejecutar Forecast:

```bash
docker compose run --rm pipeline python -m src.pipelines.forecast.run_forecast_pipeline
```

Ejecutar ABC:

```bash
docker compose run --rm pipeline python -m src.pipelines.abc.run_abc_pipeline
```

Ejecutar Market Basket:

```bash
docker compose run --rm pipeline python -m src.pipelines.market_basket.run_market_basket_pipeline
```

Validar Python:

```bash
docker compose run --rm pipeline python -m compileall src
```

Build del dashboard:

```bash
docker compose run --rm dashboard-build pnpm run build
```

Este comando solo compila la app en `dashboard/dist`; no levanta un servidor ni muestra un localhost.

## Datos Locales

La carpeta unica de ingesta del proyecto debe ser:

```text
data/input/
```

Contenido esperado:

```text
data/input/
  movimientos.xlsx
  movimientos_pedido_externo_lookup.parquet
  lineas_solicitudes_con_pedidos.xlsx
  maestro_dimensiones_limpio.xlsx
  Informacion_albaranaes.xlsx
  26-05-2026.xlsx
```

La foto de stock puede tener cualquier fecha con formato `dd-mm-yyyy.xlsx`; si hay varias fotos en `data/input/`, el pipeline usa la mas reciente.

`movimientos_pedido_externo_lookup.parquet` es una herramienta opcional de recuperacion si un export antiguo de `movimientos.xlsx` no trae `Pedido externo` en movimientos PI historicos. No se usa por defecto para no ocultar errores de origen. Se genera una vez desde el historico con:

```bash
docker compose run --rm pipeline python -m src.pipelines.common.build_movimientos_lookup
```

Para usarlo de forma explicita en una ejecucion concreta:

```bash
WAREHOUSE_ENABLE_MOVIMIENTOS_LOOKUP=1 docker compose run --rm pipeline python -m src.pipelines.market_basket.run_market_basket_pipeline
```

Si `movimientos.xlsx` ya viene corregido con `Pedido externo`, no hace falta activar este lookup.

Por compatibilidad local, la capa comun todavia puede leer desde ubicaciones antiguas si falta algo en `data/input/`:

- `data/raw/`
- la raiz del repositorio
- `_legacy_imports/abc/`
- `_legacy_imports/market_basket/`

Cuando esto ocurre, el pipeline emite un warning. La idea final es que `_legacy_imports/` quede solo como historico y que ningun pipeline dependa de esa carpeta. No se borran ni sustituyen las carpetas antiguas `data/raw`, `data/interim`, `data/processed` ni `outputs`.

## Capa Normalizada

Los pipelines comunes generan:

```text
data/normalized/
  movimientos_normalizados.parquet
  lineas_normalizadas.parquet
  articulos_normalizados.parquet
  albaranes_normalizados.parquet
  stock_snapshot_normalizado.parquet
```

Si un input no existe o una foto de stock no es valida, se registra un warning y se continua con el resto.

## Outputs

Los outputs modulares se escriben en:

```text
data/output/forecast/
data/output/abc/
  abc_sku.csv
  abc_sku.parquet
  abc_auditoria.xlsx
  csv/
  parquet/
  json/
data/output/market_basket/
  afinidad_pares.csv
  afinidad_reglas.csv
  kpi_resumen.csv
  calidad_datos.csv
  transacciones_resumen.csv
  articulos_resumen.csv
  item_metrics.csv
  clusters_sku.csv
  hubs_sku.csv
  raw_temporal_pairs.csv
  temporal_stability_metrics.csv
  series_temporales.csv
  plots/
  logs/
```

El dashboard consume solo JSON estaticos desde:

```text
dashboard/public/data/forecast/
dashboard/public/data/abc/
dashboard/public/data/market_basket/
```

Forecast mantiene compatibilidad con los JSON existentes:

- `dashboard/public/data/consumo_forecast_diario.json`
- `dashboard/public/data/consumo_forecast_semanal.json`

## Dashboard

El dashboard se levanta con Docker y muestra:

- Forecast
- ABC
- Market Basket

Forecast sigue usando el modulo existente. ABC y Market Basket usan los JSON generados por sus pipelines.

## GitHub

La subida remota y la limpieza de historial quedan para mas adelante. En esta fase se trabaja en local y no se borran datos ni outputs locales.

Carpetas y artefactos que no deberian subirse:

- `.venv/`, `venv/`
- `node_modules/`
- `dist/`, `build/`
- `_legacy_imports/`
- `data/input/`
- `data/normalized/`
- `data/output/`
- `outputs/`, `output/`
- Excels, Parquet, PKL, logs y datos reales

Mas detalle en `docs/data_pipeline.md`.
