# WanderSync Travel Platform

Plataforma de empaquetamiento turístico dinámico (vuelo + hotel + auto) construida con microservicios, **GraphQL**, **patrón SAGA**, **Dask**, **Prefect** y **Docker Compose**.

Parcial Práctico del Segundo Corte — Patrones Arquitectónicos Avanzados.

📄 **Documento técnico de arquitectura:** [`docs/ARQUITECTURA.md`](docs/ARQUITECTURA.md)
🔒 **Auditoría de dependencias:** [`docs/security/README.md`](docs/security/README.md)

## Arquitectura en una línea

`Frontend React` → `API Gateway GraphQL` → `Orders (orquestador SAGA)` → `Flights · Hotels · Cars` → `Supabase (PostgreSQL + pg_graphql)` ← `Prefect + Dask (scraping distribuido)`

## Puesta en marcha

1. Crear un proyecto en [Supabase](https://supabase.com) y ejecutar en el **SQL Editor** el contenido de
   [`supabase/migrations/20261003000000_wandersync_schema.sql`](supabase/migrations/20261003000000_wandersync_schema.sql).
2. Copiar `.env.example` a `.env` y completar `SUPABASE_URL`, `SUPABASE_SERVICE_KEY`, `DATABASE_URL` (Session pooler) y `REDIS_PASSWORD`.
3. Levantar todo:

   ```bash
   docker compose up --build -d
   ```

| URL | Servicio |
|---|---|
| http://localhost:3000 | Frontend |
| http://localhost:8080/graphql | API Gateway GraphQL (explorador GraphiQL) |
| http://localhost:4200 | Panel de Prefect |
| http://localhost:8787 | Panel de Dask |
| http://localhost:8090 | Sitio simulado que se scrapea |

## Estructura

```
├── frontend/          React + Apollo Client (nginx)
├── gateway/           API Gateway GraphQL: auth, sesiones, rate limiting
├── services/          Microservicios: orders (SAGA), flights, hotels, cars
├── pipeline/          Flujo Prefect + tareas Dask (scraping, limpieza, carga)
├── mock-provider/     Fuente de datos simulada con fallos inyectados
├── supabase/          Migración del esquema
├── scripts/           Auditoría de seguridad y pruebas del gateway
└── docs/              Documento técnico y reportes de auditoría
```

## Pruebas

```bash
# SAGA: una reserva exitosa y una con fallo simulado en el auto
docker compose exec orders-service python -m app.demo CAR

# Gateway de extremo a extremo (21 verificaciones)
docker compose exec -T gateway python - < scripts/test_gateway.py

# Auditoría de dependencias (pip-audit + npm audit)
./scripts/security-audit.sh
```
