# Warehouse Analysis

Aplicacion local modular para analisis operativo de almacen con tres modulos:

- Forecast
- ABC
- Market Basket

El flujo principal es Docker. No hace falta ejecutar `npm install`, `npm run dev` ni instalar dependencias Python en el host para el uso normal.

## Comandos Principales

Levantar dashboard:

```bash
docker compose up --build dashboard-dev
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
docker compose run --rm dashboard-build npm run build
```

## Datos Locales

La carpeta unica de ingesta del proyecto debe ser:

```text
data/input/
```

Contenido esperado:

```text
data/input/
  movimientos.xlsx
  lineas_solicitudes_con_pedidos.xlsx
  maestro_dimensiones_limpio.xlsx
  Informacion_albaranaes.xlsx
  29-04-2026.xlsx
```

La foto de stock puede tener cualquier fecha con formato `dd-mm-yyyy.xlsx`; si hay varias fotos en `data/input/`, el pipeline usa la mas reciente. En tu caso actual, la foto correcta es `29-04-2026.xlsx`.

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
data/output/market_basket/
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
