# Dashboard Forecast

La unica via oficial para desarrollar y compilar esta web es Docker. Dentro de Docker se usa `pnpm`, no `npm`.

## Requisitos

- Docker Desktop o equivalente con `docker compose`
- `outputs/consumption` generado si quieres trabajar con datos reales

## Desarrollo Oficial

Desde la raiz del repo:

```bash
docker compose up --build dashboard-dev
```

Abrir:

- [http://localhost:5173](http://localhost:5173)

Parar:

```bash
docker compose down
```

## Build Oficial

Desde la raiz del repo:

```bash
docker compose run --rm dashboard-build
```

Tambien puedes ejecutar el script de build directamente:

```bash
docker compose run --rm dashboard-build pnpm run build
```

Este comando solo genera `dist`; no levanta servidor ni muestra localhost. Para abrir la web usa `dashboard-dev`.

## Datos

El contenedor ejecuta internamente:

- `pnpm run sync:data`
- `pnpm exec vite --host 0.0.0.0 --port 5173`
- `pnpm run build:release`

La fuente de datos esperada es:

- `../outputs/consumption`

Si la capa de consumo no existe o esta incompleta, `sync:data` cae al modo demo del proyecto.

## Scripts Internos

Los scripts de [package.json](C:\Users\rdiezl\Desktop\proyecto\Warehouse_Analysis\dashboard\package.json) se ejecutan con `pnpm` dentro del contenedor. El lockfile operativo es `pnpm-lock.yaml`.

No se consideran flujo oficial para ejecutar la web directamente en el host.
