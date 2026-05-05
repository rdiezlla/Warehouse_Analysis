# Data Pipeline

## Flujo Principal

El proyecto queda preparado para trabajar en local con Docker y tres modulos:

- Forecast
- ABC
- Market Basket

La carpeta canonica de ingesta es:

```text
data/input/
```

Debe contener, cuando esten disponibles:

| Dataset | Archivo esperado | Uso principal |
| --- | --- | --- |
| Movimientos | `movimientos.xlsx` | Forecast, ABC y Market Basket |
| Lineas de solicitudes | `lineas_solicitudes_con_pedidos.xlsx` | Fechas de servicio, pedidos, accion envio/recogida |
| Maestro de articulos | `maestro_dimensiones_limpio.xlsx` | Enriquecimiento de SKUs/articulos |
| Albaranes | `Informacion_albaranaes.xlsx` | Contexto operativo y reporting |
| Foto stock | `dd-mm-yyyy.xlsx` | Stock actual para ABC/auditoria |

Si hay varias fotos de stock dentro de `data/input/`, se usa la mas reciente segun la fecha del nombre. Ejemplo actual recomendado:

```text
data/input/29-04-2026.xlsx
```

La capa comun todavia puede leer por compatibilidad local desde:

1. `data/raw/`
2. raiz del repositorio
3. `_legacy_imports/abc/`
4. `_legacy_imports/market_basket/`

Si un fichero se carga desde estas rutas antiguas, el pipeline emite un warning. Esta compatibilidad existe para no romper el trabajo local actual, pero la arquitectura objetivo es que `_legacy_imports/` no sea una dependencia operativa.

Si un fichero falta o una foto de stock no se puede leer, el pipeline emite warnings y continua con los datasets disponibles.

## Capa Normalizada

El comando:

```bash
docker compose run --rm pipeline python -m src.pipelines.run_all
```

genera, cuando hay datos suficientes:

- `data/normalized/movimientos_normalizados.parquet`
- `data/normalized/lineas_normalizadas.parquet`
- `data/normalized/articulos_normalizados.parquet`
- `data/normalized/albaranes_normalizados.parquet`
- `data/normalized/stock_snapshot_normalizado.parquet`

Esta carpeta es el contrato comun para los modulos. Forecast, ABC y Market Basket deben consumir preferentemente estos Parquet y no volver a leer Excels originales con logica propia.

## Salidas Organizadas

Los outputs analiticos se separan por modulo:

```text
data/output/
  forecast/
  abc/
  market_basket/
```

Los JSON ligeros para la UI se publican por modulo:

```text
dashboard/public/data/
  forecast/
  abc/
  market_basket/
```

Por compatibilidad, Forecast mantiene tambien los JSON historicos directamente en `dashboard/public/data/`.

## Columnas Estandar De Movimientos

`movimientos_normalizados.parquet` usa estas columnas principales:

- `movement_type`
- `started_at`
- `completed_at`
- `operational_date`
- `sku`
- `sku_description`
- `quantity`
- `owner`
- `operator`
- `location`
- `order_id`
- `external_order_id`
- `client`
- `external_client`
- `service_code`
- `basket_date`
- `transaction_id`

`transaction_id` se deriva como `external_order_id + "|" + owner` cuando ambos campos existen.

## Columnas Estandar De Lineas

`lineas_normalizadas.parquet` incluye `action` y `service_flow`. `service_flow` se deriva directamente de la columna `Accion`:

- `ENVIO` o `ENTREGA` -> `envio`
- `RECOGIDA` -> `recogida`

Esto evita depender del prefijo de `codigo_generico` para separar envios y recogidas.

## Forecast

Forecast se mantiene con el flujo existente de `src.main`. El wrapper nuevo esta en:

```text
src/pipelines/forecast/run_forecast_pipeline.py
```

Por defecto ejecuta el stage `consumption`, que mantiene la compatibilidad con los JSON actuales del dashboard:

- `dashboard/public/data/consumo_forecast_diario.json`
- `dashboard/public/data/consumo_forecast_semanal.json`

## ABC

ABC consume `movimientos_normalizados.parquet` y, si existe, `stock_snapshot_normalizado.parquet`.

Outputs:

- `data/output/abc/abc_sku.csv`
- `data/output/abc/abc_sku.parquet`
- `data/output/abc/abc_auditoria.xlsx`
- `dashboard/public/data/abc/abc_sku.json`

La primera integracion calcula ranking por SKU, lineas PI, unidades PI, porcentaje individual, porcentaje acumulado, categoria ABC y una auditoria inicial con candidatos de traslado. La auditoria legacy completa sigue conservada en `_legacy_imports/abc/output/auditoria/`, pero no debe ser una dependencia del pipeline nuevo.

## Market Basket

Market Basket consume `movimientos_normalizados.parquet`, filtra `PI`, excluye filas sin `external_order_id` y crea cestas por `transaction_id`.

Outputs:

- `data/output/market_basket/pares_frecuentes.csv`
- `data/output/market_basket/pares_frecuentes.parquet`
- `data/output/market_basket/reglas_asociacion.csv`
- `data/output/market_basket/reglas_asociacion.parquet`
- `dashboard/public/data/market_basket/pares_frecuentes.json`
- `dashboard/public/data/market_basket/reglas_asociacion.json`

## Anadir Un Modulo Nuevo

1. Consumir datos desde `data/normalized/`.
2. Crear un pipeline en `src/pipelines/<modulo>/`.
3. Guardar outputs analiticos en `data/output/<modulo>/`.
4. Publicar solo JSON ligero para dashboard en `dashboard/public/data/<modulo>/`.
5. Mantener validaciones y warnings en la capa comun si el modulo necesita nuevas columnas.
