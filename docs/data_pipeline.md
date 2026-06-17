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
| Lookup de pedido externo historico | `movimientos_pedido_externo_lookup.parquet` | Reconstruir cestas PI cuando `movimientos.xlsx` no trae `Pedido externo` historico |
| Lineas de solicitudes | `lineas_solicitudes_con_pedidos.xlsx` | Fechas de servicio, pedidos, accion envio/recogida |
| Maestro de articulos | `maestro_dimensiones_limpio.xlsx` | Enriquecimiento de SKUs/articulos |
| Albaranes | `Informacion_albaranaes.xlsx` | Contexto operativo y reporting |
| Foto stock | `dd-mm-yyyy.xlsx` | Stock actual para ABC/auditoria |

Si hay varias fotos de stock dentro de `data/input/`, se usa la mas reciente segun la fecha del nombre. Ejemplo:

```text
data/input/26-05-2026.xlsx
```

La capa comun todavia puede leer por compatibilidad local desde:

1. `data/raw/`
2. raiz del repositorio
3. `_legacy_imports/abc/`
4. `_legacy_imports/market_basket/`

Si un fichero se carga desde estas rutas antiguas, el pipeline emite un warning. Esta compatibilidad existe para no romper el trabajo local actual, pero la arquitectura objetivo es que `_legacy_imports/` no sea una dependencia operativa.

Si un fichero falta o una foto de stock no se puede leer, el pipeline emite warnings y continua con los datasets disponibles.

### Lookup De Pedido Externo Para Market Basket

Un export de `movimientos.xlsx` puede conservar los movimientos PI pero venir sin `Pedido externo` en historico. En ese caso Market Basket no puede formar cestas por pedido, aunque los pickings existan. Lo correcto es arreglar el origen; el lookup es solo una herramienta de recuperacion o auditoria.

Para evitar depender de `_legacy_imports/` en ejecucion normal, se puede generar una tabla local de enriquecimiento:

```bash
docker compose run --rm pipeline python -m src.pipelines.common.build_movimientos_lookup
```

Esto escribe:

- `data/input/movimientos_pedido_externo_lookup.parquet`
- `data/input/movimientos_pedido_externo_lookup.csv`

La clave de cruce es `paleta + sku + fecha_finalizacion + cantidad + propietario`. El pipeline comun solo usa el lookup si se activa explicitamente:

```bash
WAREHOUSE_ENABLE_MOVIMIENTOS_LOOKUP=1 docker compose run --rm pipeline python -m src.pipelines.market_basket.run_market_basket_pipeline
```

Si se activa, rellena `external_order_id` solo en movimientos `PI` que no lo traen de origen y marca:

- `external_order_id_enriched = 1`
- `external_order_id_source = lookup`

Los movimientos que ya traen `Pedido externo` quedan como `external_order_id_source = source`.

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
- `pallet`
- `order_id`
- `external_order_id`
- `external_order_id_enriched`
- `external_order_id_source`
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
- `data/output/abc/csv/stock_abc_actual_article.csv`
- `data/output/abc/parquet/stock_abc_actual_article.parquet`
- `data/output/abc/parquet/stock_abc_actual_owner_article.parquet`
- `data/output/abc/parquet/stock_abc_historico_trimestral_article.parquet`
- `data/output/abc/parquet/stock_abc_historico_trimestral_owner_article.parquet`
- `data/output/abc/parquet/stock_abc_cambios_trimestrales.parquet`
- `data/output/abc/parquet/stock_abc_temporalidad_article.parquet`
- `data/output/abc/parquet/stock_abc_temporalidad_monthly_article.parquet`
- `data/output/abc/parquet/stock_abc_temporalidad_quarterly_article.parquet`
- `data/output/abc/parquet/stock_abc_decision_almacen_article.parquet`
- `data/output/abc/json/stock_abc_resumen_kpis.json`
- `data/output/abc/json/stock_abc_resumen_temporalidad.json`
- `dashboard/public/data/abc/abc_sku.json`
- `dashboard/public/data/abc/stock_abc_resumen_kpis.json`
- `dashboard/public/data/abc/stock_abc_resumen_temporalidad.json`

ABC genera ranking por SKU, outputs 30d/YTD/trimestrales, cambios ABC, temporalidad, decision de almacen y una auditoria Excel amplia. La auditoria legacy sigue conservada en `_legacy_imports/abc/output/auditoria/`, pero el pipeline nuevo ya no depende de esa carpeta.

## Market Basket

Market Basket consume `movimientos_normalizados.parquet`, filtra `PI`, excluye filas sin `external_order_id` y crea cestas por `transaction_id`.

Outputs principales:

- `data/output/market_basket/pares_frecuentes.csv`
- `data/output/market_basket/pares_frecuentes.parquet`
- `data/output/market_basket/reglas_asociacion.csv`
- `data/output/market_basket/reglas_asociacion.parquet`
- `data/output/market_basket/afinidad_pares.csv`
- `data/output/market_basket/afinidad_reglas.csv`
- `data/output/market_basket/kpi_resumen.csv`
- `data/output/market_basket/calidad_datos.csv`
- `data/output/market_basket/transacciones_resumen.csv`
- `data/output/market_basket/articulos_resumen.csv`
- `data/output/market_basket/articulos_por_propietario.csv`
- `data/output/market_basket/item_metrics.csv`
- `data/output/market_basket/sku_location_profile.csv`
- `data/output/market_basket/clusters_sku.csv`
- `data/output/market_basket/hubs_sku.csv`
- `data/output/market_basket/raw_temporal_pairs.csv`
- `data/output/market_basket/temporal_stability_metrics.csv`
- `data/output/market_basket/series_temporales.csv`
- `data/output/market_basket/resumen_ejecutivo.md`
- `data/output/market_basket/metadata_modelo.json`
- `data/output/market_basket/plots/`
- `data/output/market_basket/logs/`
- `dashboard/public/data/market_basket/pares_frecuentes.json`
- `dashboard/public/data/market_basket/reglas_asociacion.json`

El wrapper nuevo ejecuta el pipeline completo de Market Basket de 8 etapas. `pares_frecuentes` y `reglas_asociacion` se mantienen como nombres compatibles para el dashboard; `afinidad_pares` y `afinidad_reglas` conservan la nomenclatura legacy.

## Anadir Un Modulo Nuevo

1. Consumir datos desde `data/normalized/`.
2. Crear un pipeline en `src/pipelines/<modulo>/`.
3. Guardar outputs analiticos en `data/output/<modulo>/`.
4. Publicar solo JSON ligero para dashboard en `dashboard/public/data/<modulo>/`.
5. Mantener validaciones y warnings en la capa comun si el modulo necesita nuevas columnas.
