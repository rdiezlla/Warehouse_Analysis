# Dashboard Forecast

La única vía oficial para desarrollar y compilar esta web es Docker.

## Requisitos

- Docker Desktop o equivalente con `docker compose`
- `outputs/consumption` generado si quieres trabajar con datos reales

## Desarrollo oficial

Desde la raíz del repo:

```bash
docker compose up --build dashboard-dev
```

Wrappers:
- Windows PowerShell: [docker-dev.ps1](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.ps1)
- Windows CMD: [docker-dev.cmd](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.cmd)
- macOS/Linux: [docker-dev.sh](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-dev.sh)

Abrir:
- [http://localhost:5173](http://localhost:5173)

Parar:

```bash
docker compose down
```

## Build oficial

Desde la raíz del repo:

```bash
docker compose run --rm dashboard-build
```

Wrappers:
- Windows PowerShell: [docker-build.ps1](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.ps1)
- Windows CMD: [docker-build.cmd](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.cmd)
- macOS/Linux: [docker-build.sh](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\docker-build.sh)

Salida:
- [dist](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\dist)

## Datos

El contenedor ejecuta internamente:
- `npm run sync:data`
- `npm run dev`
- `npm run build:release`

La fuente de datos esperada es:
- `../outputs/consumption`

Si la capa de consumo no existe o está incompleta, `sync:data` cae al modo demo del proyecto.

## Scripts internos

Los scripts `npm` de [package.json](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\package.json) se mantienen porque Docker los usa dentro del contenedor.

No se consideran flujo oficial para ejecutar la web directamente en el host.
