# Warehouse Operational Forecasting

Proyecto local de forecasting operativo para operaciones log?sticas reales orientado a producci?n local, reproducible y modular.

## Objetivo

El pipeline construye y valida forecasts de servicios y picking con separaci?n expl?cita entre demanda por fecha de servicio y carga real por fecha operativa.

Salidas principales:
- Forecast diario y semanal de `n_entregas_SGE`
- Forecast diario y semanal de `n_entregas_SGP`
- Forecast diario y semanal de `n_recogidas_EGE`
- Forecast diario y semanal de `picking_lines_PI`
- Forecast diario y semanal de `picking_units_PI`
- Si hay cobertura suficiente con maestro: `picking_kg_dia`, `picking_m3_dia`

## Estructura

- `config/`: configuraci?n, aliases y reglas regex
- `data/raw/`: copia local controlada de los Excel originales
- `data/interim/`: tablas limpias intermedias
- `data/processed/`: facts, datasets y tablas finales
- `data/external/`: calendarios y referencias reproducibles
- `outputs/`: QA, m?tricas, forecasts, modelos, plots y reportes
- `src/`: c?digo fuente del pipeline

## Instalaci?n

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## Configuraci?n

Archivo principal: `config/settings.yaml`

Puntos relevantes:
- nombres esperados de los 4 Excel
- horizontes de forecast y backtesting
- ventanas de lags/rolling
- pesos de nowcasting
- thresholds de QA

Los aliases de columnas viven en `config/column_aliases.yaml`.

## Ejecuci?n

Pipeline completo:

```bash
python -m src.main --stage all
```

Etapas individuales:

```bash
python -m src.main --stage qa
python -m src.main --stage features
python -m src.main --stage train
python -m src.main --stage backtest
python -m src.main --stage forecast
```

## Qu? hace cada etapa

- `qa`: ingesta, limpieza robusta, normalizaci?n de claves, calendario, joins, facts y reportes QA
- `features`: datasets diarios y semanales por KPI
- `train`: entrenamiento de modelos lineales y boosting, m?s serializaci?n
- `backtest`: walk-forward temporal diario y semanal y comparativas
- `forecast`: forecasts finales, transformador servicio->picking, nowcasting y reconciliaci?n semanal

## Outputs importantes

- `data/processed/fact_servicio_dia.*`
- `data/processed/fact_picking_dia.*`
- `data/processed/fact_cartera.*`
- `data/processed/dim_date.*`
- `outputs/backtests/backtest_metrics_*.csv`
- `outputs/forecasts/daily_forecasts.csv`
- `outputs/forecasts/weekly_forecasts.csv`
- `outputs/models/model_registry.json`
- `outputs/reports/*.md`
- `outputs/plots/*.png`

## Limitaciones conocidas

- La cobertura de joins entre movimientos y albaranes no es completa; el pipeline lo reporta expl?citamente y degrada con warnings.
- `2025` queda excluido del entrenamiento normal y se trata como apagado estructural.
- La se?al de solicitudes se usa principalmente para nowcasting de corto plazo y stress test 2026.
- CR y EP quedan preparados en facts y reporting, pero la V1 productiva prioriza PI.

## Siguientes pasos sugeridos

- Extender validaci?n operativa de CR y EP
- Incorporar snapshots operativos de cartera si aparecen nuevas fuentes
- Afinar intensidades del transformador por urgencia y familia de art?culo
