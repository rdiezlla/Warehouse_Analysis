# Regression Audit - Warehouse_Analysis

## 1) Versión de referencia utilizada
- Referencia estable: `a4c10c3` (2026-03-30 21:40, último estado antes de cambios de datos del commit `3a7a867`).
- Versión actual auditada: `7e8a4cd` (2026-04-01 20:14).

## 2) Cambios detectados entre referencia y actual (git)
```text
M	.DS_Store
M	dashboard/public/data/_metadata.json
M	dashboard/public/data/consumo_forecast_diario.json
M	dashboard/public/data/consumo_forecast_semanal.json
M	dashboard/public/data/consumo_progreso_actual.json
M	dashboard/public/data/consumo_vs_2024_diario.json
M	dashboard/public/data/consumo_vs_2024_semanal.json
M	dashboard/public/data/dim_kpi.json
M	data/.DS_Store
M	lineas_solicitudes_con_pedidos.xlsx
M	movimientos.xlsx
M	src/.DS_Store
```
- No hay cambios en `src/` del pipeline entre referencia y actual; cambian principalmente `movimientos.xlsx`, `lineas_solicitudes_con_pedidos.xlsx` y JSON de dashboard.

## 3) Diagnóstico por capas
### 3.1 Inputs
- `movimientos.xlsx`: filas `291125` -> `291402` (+277).
- `solicitudes`: filas `5424` -> `5435` (+11).
- Cobertura join `movimientos_albaranes_global`: `0.316544` -> `0.316244` (estable, ligera caída).
- `movimientos_mal.xlsx` tiene `291125` filas y `38` columnas; coincide exactamente con la referencia en tamaño/distribución y no con el actual.
- Cambio estructural relevante en `movimientos`: `38` -> `21` columnas, con `17` columnas antiguas ausentes en actual (`alt`, `art`, `cap_pal_ubi`, `cod_ext`, `col`, `denominacion_art`, `denominacion_propietario`, `ind_drive_n`, `num_pal_ubi`, `pasillo`, `propie`, `sit`, `t_alt_pal`, `t_anc_pal`, `v1_ext`, `v2_ext`, `vl_ext`).
- Importante: las columnas canónicas que el pipeline usa en limpieza (`tipo_movimiento`, `fecha_inicio`, `fecha_finalizacion`, `articulo`, `denominacion_articulo`, `cantidad`, `operario`, `denominacion_operario`, `ubicacion`, `pedido`, `pedido_externo`, `cliente`, `cliente_externo`) siguen presentes.

### 3.2 Facts
- `fact_servicio_dia`: filas `1125` -> `1126`, fechas `2021-03-22..2026-12-07` -> `2021-03-22..2026-12-07`, gaps `962` -> `961`.
- `fact_picking_dia`: filas `830` -> `832`, fechas `2022-01-03..2026-03-27` -> `2022-01-03..2026-03-31`, gaps `715` -> `717`.
- `fact_cartera`: filas `1714` -> `1716`, fechas `2025-11-03..2026-12-07` -> `2025-11-03..2026-12-07`, gaps `248` -> `247`.
- No hay rotura catastrófica en facts; hay cambios incrementales coherentes con nuevos registros de entrada.

### 3.3 Forecast / evaluación
- Comparativa OOF (backtest) recalculada en ambos entornos con el mismo código y distinto input:
  - `ds_entregas_sge_dia`: WAPE `1.2401` -> `1.2401` (Δ +0.0000), MAE `1.5506` -> `1.5506`.
  - `ds_entregas_sge_semana`: WAPE `1.2073` -> `1.2073` (Δ +0.0000), MAE `10.9203` -> `10.9203`.
  - `ds_recogidas_ege_dia`: WAPE `1.3282` -> `1.3282` (Δ +0.0000), MAE `1.3805` -> `1.3805`.
  - `ds_recogidas_ege_semana`: WAPE `1.2063` -> `1.2063` (Δ +0.0000), MAE `9.0791` -> `9.0791`.
  - `ds_entregas_sgp_dia`: WAPE `1.2733` -> `1.2728` (Δ -0.0005), MAE `6.3495` -> `6.3492`.
  - `ds_entregas_sgp_semana`: WAPE `1.2780` -> `1.2732` (Δ -0.0048), MAE `45.7303` -> `45.5759`.
- Resultado de auditoría: **no hay degradación real del forecast** en backtest entre referencia y actual; las diferencias son nulas o ligeramente favorables en actual.
- El ranking de modelos ganadores se mantiene estable (0 cambios).

### 3.4 Capa de consumo y dashboard
- Tablas desalineadas pipeline vs dashboard: 11 casos (sumando referencia y actual).
- En versión actual: 5/6 tablas con hash distinto entre `outputs/consumption` y `dashboard/public/data` en los worktrees auditados (staleness/sync pendiente).
- En la referencia, `dashboard/public/data` era demo (`sourceMode=demo`), por tanto no representa el estado real del pipeline.

## 4) Causa más probable de regresión
- Commit sospechoso principal: `3a7a867` (`nuevo pip`), porque modifica directamente `movimientos.xlsx` y `lineas_solicitudes_con_pedidos.xlsx` sin cambios de código pipeline.
- No se observa rotura funcional del pipeline ni empeoramiento de métricas de forecast en esta comparación controlada.
- La causa más probable de la percepción de regresión es **mezcla de salidas / visualización**:
  - Antes el dashboard estaba en `sourceMode=demo`.
  - Ahora consume outputs reales.
  - Y además puede quedar desincronizado respecto a `outputs/consumption` si no se ejecuta `sync:data` tras regenerar pipeline.
- Como causa secundaria, hay cambio de estructura en `movimientos` (menos columnas), pero no afecta a columnas canónicas requeridas por el pipeline actual.

## 5) Impacto por KPI (resumen)
- `ds_entregas_sge_dia`: ΔWAPE +0.0000, ΔMAE +0.0000, ΔBias +0.0000.
- `ds_entregas_sge_semana`: ΔWAPE +0.0000, ΔMAE +0.0000, ΔBias +0.0000.
- `ds_recogidas_ege_dia`: ΔWAPE +0.0000, ΔMAE +0.0000, ΔBias +0.0000.
- `ds_recogidas_ege_semana`: ΔWAPE +0.0000, ΔMAE +0.0000, ΔBias +0.0000.
- `ds_entregas_sgp_dia`: ΔWAPE -0.0005, ΔMAE -0.0003, ΔBias -0.0002.
- `ds_entregas_sgp_semana`: ΔWAPE -0.0048, ΔMAE -0.1543, ΔBias -0.0046.
- `ds_picking_units_pi_semana`: ΔWAPE -0.0052, ΔMAE -4.1785, ΔBias -0.0008.
- `ds_picking_lines_pi_semana`: ΔWAPE -0.0071, ΔMAE -1.0522, ΔBias -0.0071.
- `ds_picking_lines_pi_dia`: ΔWAPE -0.0157, ΔMAE -0.1522, ΔBias -0.0187.
- `ds_picking_units_pi_dia`: ΔWAPE -0.0180, ΔMAE -4.3165, ΔBias -0.0156.

## 6) ¿Está implicado `movimientos`?
- Está implicado en el sentido de que **sí cambió** (filas y estructura de columnas) y `movimientos_mal.xlsx` reproduce el estado previo.
- Pero no aparece como causante directo de degradación de forecast en esta auditoría: las columnas críticas siguen presentes y las métricas no empeoran.

## 7) Corrección mínima recomendada (sin reescritura)
1. Añadir validación explícita de esquema de entrada en ingesta para `movimientos` y `solicitudes` (alerta fuerte si desaparecen columnas no canónicas relevantes de negocio).
2. Añadir check de frescura en dashboard: bloquear publicación si `dashboard/public/data/_metadata.json` está desalineado con timestamp/hash de `outputs/consumption`.
3. Operativa recomendada: tras `python -m src.main --stage consumption`, ejecutar siempre `dashboard/scripts/sync-consumption-data.mjs` antes de revisar el dashboard.

## 8) Archivos de evidencia generados
- `input_regression_comparison.csv`
- `facts_regression_comparison.csv`
- `forecast_regression_comparison.csv`
- `dashboard_data_regression.csv`
