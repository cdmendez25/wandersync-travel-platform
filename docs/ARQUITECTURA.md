# WanderSync Travel Solutions — Documento Técnico de Arquitectura

**Asignatura:** Patrones Arquitectónicos Avanzados · **Evaluación:** Parcial Práctico del Segundo Corte
**Proyecto:** Plataforma de Empaquetamiento Turístico Dinámico

---

## Tabla de contenido

1. [Contexto y problema](#1-contexto-y-problema)
2. [Stack tecnológico](#2-stack-tecnológico)
3. [Arquitectura general](#3-arquitectura-general)
4. [Componentes y propiedad de datos](#4-componentes-y-propiedad-de-datos)
5. [Modelo de datos (Supabase)](#5-modelo-de-datos-supabase)
6. [Ingesta distribuida: Scraping + Dask + Prefect](#6-ingesta-distribuida-scraping--dask--prefect)
7. [API Gateway GraphQL](#7-api-gateway-graphql)
8. [Patrón SAGA](#8-patrón-saga)
9. [Ciberseguridad por diseño](#9-ciberseguridad-por-diseño)
10. [Despliegue con Docker Compose](#10-despliegue-con-docker-compose)
11. [Pruebas y evidencias](#11-pruebas-y-evidencias)
12. [Decisiones técnicas](#12-decisiones-técnicas)
13. [Limitaciones conocidas y trabajo futuro](#13-limitaciones-conocidas-y-trabajo-futuro)
14. [Guía de la demostración](#14-guía-de-la-demostración)

---

## 1. Contexto y problema

WanderSync vende paquetes turísticos que combinan **vuelo + hotel + auto** en una sola compra. La arquitectura anterior presentaba dos problemas:

| Problema | Consecuencia | Solución en este rediseño |
|---|---|---|
| **Reservas huérfanas**: el pago y el vuelo se confirmaban, pero el hotel o el auto fallaban en la red y no había reversión | Clientes cobrados por paquetes incompletos; datos inconsistentes | **Patrón SAGA orquestado como flow de Prefect**, con compensaciones automáticas, idempotencia y recuperación tras caídas (§8) |
| **Sincronización masiva de tarifas** bloqueaba la persistencia y la red | Cuellos de botella y servicios principales bloqueados | **Ingesta asíncrona y distribuida** con Dask, orquestada y observada con Prefect (§6) |

Además, el sistema se diseñó con **seguridad desde el inicio** (§9): sesiones resistentes a *Session Fixation*, contraseñas con Argon2id, *rate limiting* y auditoría de dependencias.

---

## 2. Stack tecnológico

| Capa | Tecnología | Justificación |
|---|---|---|
| Frontend | React 18 + Vite 8 + Apollo Client, servido con nginx (no root) | Apollo permite consultas GraphQL declarativas que piden solo los campos que cada pantalla muestra |
| API Gateway | Python 3.12 + FastAPI + **Strawberry GraphQL** | Esquema tipado en Python, resolvers asíncronos (consultas en paralelo) y extensiones de seguridad (límites de profundidad y alias) |
| Microservicios | FastAPI + psycopg 3 (pool de conexiones) | Servicios pequeños y explícitos; SQL directo para operaciones atómicas de inventario |
| Persistencia | **Supabase** (PostgreSQL 15 + **pg_graphql**) | Base de datos moderna con GraphQL nativo (`/graphql/v1`), exigido por el enunciado (§3.3) |
| Sesiones / rate limiting | Redis 7 | Almacenamiento en memoria compartido por todas las réplicas del gateway; operaciones atómicas con Lua |
| Computación distribuida | **Dask** (1 scheduler + 2 workers) | Paraleliza el scraping, la limpieza y la carga sin bloquear los servicios principales |
| Orquestación / observabilidad | **Prefect 3** | Orquesta la ingesta y cada reserva (SAGA): flujos con reintentos declarativos, programación periódica y panel visual |
| Contenedores | **Docker + Docker Compose** | Todo el ecosistema se levanta con `docker compose up` |

Se eligió **Python para todo el backend** porque Dask y Prefect son librerías de Python: un solo lenguaje reduce la complejidad y permite compartir patrones entre servicios.

---

## 3. Arquitectura general

```mermaid
flowchart TB
    user([Usuario / Navegador])

    subgraph cliente[Cliente]
        fe["Frontend React<br/>:3000"]
    end

    subgraph entrada[Entrada única]
        gw["API Gateway GraphQL<br/>Strawberry · :8080"]
        redis[("Redis<br/>sesiones + rate limit")]
    end

    subgraph servicios[Microservicios de reservas]
        orders["Orders / Facturación<br/>órdenes y pagos · :8004"]
        saga["saga-pipeline<br/>flow booking-saga"]
        flights["Flights · :8001"]
        hotels["Hotels · :8002"]
        cars["Cars · :8003"]
    end

    subgraph datos[Persistencia - Supabase Cloud]
        pg[("PostgreSQL + pg_graphql<br/>/graphql/v1")]
    end

    subgraph orquesta[Orquestación y observabilidad]
        prefect["Prefect Server<br/>flows: ingesta + SAGA · :4200"]
    end

    subgraph ingesta[Ingesta distribuida]
        pipe["pipeline<br/>flow ingest-travel-inventory"]
        sched["Dask Scheduler<br/>:8787"]
        w1["Dask Worker 1"]
        w2["Dask Worker 2"]
        mock["Mock Provider<br/>Kayak / Booking / Rentalcars · :8090"]
    end

    user --> fe
    fe -- "GraphQL (cookie HttpOnly)" --> gw
    gw <--> redis
    gw -- "catálogo: GraphQL (solo campos pedidos)" --> pg
    gw -- "usuarios: SQL (esquema private)" --> pg
    gw -- "bookPackage / pedidos: REST" --> orders
    orders -- "cotización (GET)" --> flights & hotels & cars
    orders -- "crea la ejecución y espera el resultado" --> prefect
    prefect -- "entrega la ejecución" --> saga
    saga -- "reserve / cancel" --> flights & hotels & cars
    saga -- "payments, saga_steps, estado de la orden" --> pg
    orders -- "orders" --> pg
    flights & hotels & cars -- "reservas + inventario" --> pg
    pipe <--> prefect
    pipe -- "envía tareas" --> sched
    sched --> w1 & w2
    w1 & w2 -- "scraping HTML" --> mock
    w1 & w2 -- "UPSERT" --> pg
```

**Flujo principal:**
1. El frontend solo se comunica con el **API Gateway GraphQL** (requisito §4.2).
2. Las búsquedas leen el catálogo desde **Supabase GraphQL (pg_graphql)**, reenviando únicamente los campos solicitados.
3. Las reservas (`bookPackage`) se delegan al **servicio de Órdenes**, que crea la orden y dispara el flow **`booking-saga` en Prefect**: el flow ejecuta la **SAGA** contra Flights, Hotels y Cars y cada paso queda visible en el panel de Prefect.
4. En segundo plano, **Prefect** programa cada 5 minutos un flujo que reparte el scraping entre los **workers de Dask** y actualiza el catálogo en Supabase.

---

## 4. Componentes y propiedad de datos

Cada microservicio es dueño exclusivo de sus tablas: ningún servicio escribe en las tablas de otro, se comunican por HTTP.

| Componente | Responsabilidad | Tablas que posee | Puerto |
|---|---|---|---|
| **frontend** | Interfaz web: login, búsqueda, checkout, detalle con línea de tiempo SAGA | — | 3000 |
| **gateway** | API GraphQL, autenticación, sesiones, rate limiting, autorización | `private.users` | 8080 |
| **orders-service** | Órdenes y pagos (facturación): cotiza el paquete, **dispara el flow `booking-saga` y espera su resultado**, y cierra las órdenes que quedan a medias | `orders`, `payments`, `saga_steps` | 8004* |
| **saga-pipeline** | Sirve el deployment `booking-saga`: cada paso de la SAGA y cada compensación es una tarea de Prefect | escribe `payments`, `saga_steps` y el estado de `orders` en nombre de Órdenes | — |
| **flights-service** | Reserva y cancelación de asientos | `flight_reservations` (+ inventario de `flights`) | 8001* |
| **hotels-service** | Reserva y cancelación de habitaciones | `hotel_reservations` (+ inventario de `hotels`) | 8002* |
| **cars-service** | Reserva y cancelación de autos | `car_reservations` (+ inventario de `cars`) | 8003* |
| **pipeline** | Flujo Prefect de ingesta (corre al iniciar y cada 5 min) | `flights`, `hotels`, `cars`, `scrape_runs` | — |
| **dask-scheduler / dask-worker ×2** | Ejecución distribuida de las tareas del flujo | — | 8787 |
| **prefect-server** | API y panel de Prefect (ingesta y SAGA) | — | 4200 |
| **mock-provider** | Sitio web simulado con fallos inyectados | — | 8090 |
| **redis** | Sesiones y contadores de rate limiting | — | interno |

`pipeline`, `saga-pipeline`, `prefect-server`, `dask-scheduler` y `dask-worker` usan **la misma imagen** (`wandersync/pipeline:local`), de modo que Prefect, Dask y los flows tienen versiones idénticas.

\* Publicados solo en `127.0.0.1` para pruebas locales; el único punto de entrada público es el gateway.

---

## 5. Modelo de datos (Supabase)

Migración: [`supabase/migrations/20261003000000_wandersync_schema.sql`](../supabase/migrations/20261003000000_wandersync_schema.sql)

```mermaid
erDiagram
    flights ||--o{ flight_reservations : "se reserva en"
    hotels  ||--o{ hotel_reservations  : "se reserva en"
    cars    ||--o{ car_reservations    : "se reserva en"
    orders  ||--o| payments            : "se paga con"
    orders  ||--o| flight_reservations : "incluye"
    orders  ||--o| hotel_reservations  : "incluye"
    orders  ||--o| car_reservations    : "incluye"
    orders  ||--o{ saga_steps          : "registra"

    flights {
        uuid id PK
        text provider
        text external_id
        char origin
        char destination
        timestamptz departure_at
        numeric price
        int seats_available
    }
    hotels {
        uuid id PK
        text provider
        text external_id
        text name
        text city
        numeric price_per_night
        int rooms_available
    }
    cars {
        uuid id PK
        text provider
        text external_id
        text model
        text city
        numeric price_per_day
        int units_available
    }
    orders {
        uuid id PK
        uuid user_id
        text idempotency_key UK
        text status
        numeric total_amount
    }
    payments {
        uuid id PK
        uuid order_id FK
        text status
        numeric amount
    }
    flight_reservations {
        uuid id PK
        uuid order_id FK
        uuid flight_id FK
        text status
    }
    hotel_reservations {
        uuid id PK
        uuid order_id FK
        uuid hotel_id FK
        text status
    }
    car_reservations {
        uuid id PK
        uuid order_id FK
        uuid car_id FK
        text status
    }
    saga_steps {
        bigint id PK
        uuid order_id FK
        text step
        text action
        text status
        text error
    }
```

Además: `scrape_runs` (bitácora de cada ejecución de ingesta) y `private.users` (identidad, fuera de GraphQL).

**Decisiones del modelo:**

| Restricción | Propósito |
|---|---|
| `unique (provider, external_id)` en el catálogo | Permite **UPSERT**: cada scraping actualiza filas en vez de duplicarlas (y respeta los 500 MB del plan gratuito) |
| `unique (order_id)` en cada tabla de reservas y en `payments` | **Idempotencia**: si la SAGA reintenta un paso, nunca reserva dos veces |
| `unique (idempotency_key)` en `orders` | Un checkout enviado dos veces (doble clic, reintento de red) crea una sola orden |
| `check` de estados (`PENDING`, `CONFIRMED`, `COMPENSATING`, `CANCELLED`, `FAILED`) | Estados válidos garantizados por la base de datos |
| `check (password_hash like '$argon2id$%')` | La base de datos **rechaza** cualquier contraseña que no esté en Argon2id |
| Comentario `@graphql({"inflect_names": true})` | pg_graphql expone los campos en camelCase (`departureAt`), igual que el esquema del gateway |
| **RLS activado en todas las tablas, sin políticas**, y sin privilegios para `anon`/`authenticated` | La clave pública de Supabase no puede leer ni escribir nada; solo el backend (clave `service_role` o conexión directa) |
| Esquema `private` para `users` | pg_graphql nunca expone usuarios ni hashes de contraseña |

---

## 6. Ingesta distribuida: Scraping + Dask + Prefect

### 6.1 Fuente de datos

Se eligió una **fuente simulada** (`mock-provider`), permitida por el enunciado (§3.1), que reproduce la complejidad de Kayak, Booking y Rentalcars:

- Entrega **HTML renderizado en el servidor** (no JSON), por lo que el sistema realmente hace *scraping* con BeautifulSoup.
- Formatos inconsistentes a propósito: precios como `$1,234.50`, `USD 98.40` o `US$ 120.00 / night`; mayúsculas y espacios aleatorios; **filas duplicadas**.
- **Fallos de red inyectados**: 15 % de respuestas `503`/`429` y 5 % de respuestas más lentas que el *timeout* del scraper (8 s vs 5 s).
- Precios y disponibilidad cambian cada 5 minutos, mientras que el listado de vuelos/hoteles/autos es estable (permite el UPSERT).

### 6.2 Flujo Prefect sobre Dask

Código: [`pipeline/flows/ingest.py`](../pipeline/flows/ingest.py)

```mermaid
sequenceDiagram
    autonumber
    participant P as Prefect Flow<br/>ingest-travel-inventory
    participant S as Dask Scheduler
    participant W as Dask Workers (×2)
    participant M as Mock Provider
    participant DB as Supabase

    P->>DB: scrape_runs (RUNNING) por fuente
    P->>S: map(): 112 tareas de vuelos + 6 hoteles + 6 autos
    S->>W: distribuye las tareas en paralelo
    loop cada tarea (página)
        W->>M: GET /flights?origin=..&date=..
        alt 503 / 429 / timeout
            M-->>W: error
            Note over W: Prefect reintenta: 2 s, 5 s, 10 s (+ jitter)
            W->>M: reintento
        end
        M-->>W: HTML
        W->>W: parseo + limpieza + deduplicación
    end
    W-->>P: filas limpias
    P->>S: tarea load-to-supabase (por tabla)
    S->>W: ejecutar carga
    W->>DB: INSERT ... ON CONFLICT DO UPDATE (UPSERT)
    P->>DB: scrape_runs (SUCCEEDED/FAILED, intentos, filas)
```

| Aspecto | Implementación |
|---|---|
| Ejecución distribuida | `DaskTaskRunner(address="tcp://dask-scheduler:8786")`: cada página es una tarea en un worker |
| Reintentos | `retries=3`, `retry_delay_seconds=[2, 5, 10]`, `retry_jitter_factor=0.5` en cada tarea de scraping; la carga tiene `retries=2` |
| Tolerancia a fallos | Si una página falla tras todos los reintentos, el flujo continúa con las demás y lo registra en `scrape_runs.error` |
| Programación | `flow.serve(interval=300)`: el flujo corre al iniciar y luego cada 5 minutos |
| Observabilidad | Panel de Prefect (`:4200`): ejecuciones, estado de cada tarea, reintentos y logs. Panel de Dask (`:8787`): tareas por worker en tiempo real. Tabla `scrape_runs` en Supabase |
| Una sola imagen | Prefect server, scheduler, workers, el flujo de ingesta y el de la SAGA usan la misma imagen, garantizando versiones idénticas (requisito de Dask) |

**Evidencia de la primera ejecución:**

| Fuente | Páginas | Intentos | Filas guardadas |
|---|---|---|---|
| Vuelos | 112 | 135 | 516 |
| Hoteles | 6 | 8 | 144 |
| Autos | 6 | 8 | 36 |

Los 135 intentos para 112 páginas muestran los **reintentos en acción**: ninguna página se perdió pese a los fallos inyectados.

**Verificación final (9-oct-2026, stack reconstruido de cero):**

- Cada ejecución del flujo reparte **127 tareas** (112 páginas de vuelos + 6 de hoteles + 6 de autos + 3 cargas) y termina en ~40–65 s. Todas terminan `Completed`; entre el 20 % y el 25 % de las tareas necesitó reintentos (hasta 3) y aun así terminó bien.
- Los **2 workers de Dask** se repartieron el trabajo casi por mitad (122 y 131 tareas en la misma ventana de tiempo).
- Datos en Supabase: las 8 rutas con vuelos, 24 habitaciones de hotel y 6 autos por ciudad, sin duplicados `(provider, external_id)`, y toda la ventana de scraping actualizada en los últimos 12 minutos.
- Si una página se pierde tras agotar sus reintentos (≈1 de cada 112, por los fallos inyectados), la ingesta continúa y lo anota en `scrape_runs.error` (por ejemplo `1 of 112 pages failed after retries`).

---

## 7. API Gateway GraphQL

Código: [`gateway/app/schema.py`](../gateway/app/schema.py) · Explorador interactivo: `http://localhost:8080/graphql`

### 7.1 Esquema

| Tipo | Operación | Descripción |
|---|---|---|
| Query | `searchPackages(origin, destination, date, passengers)` | Vuelos en la fecha + hoteles y autos en el destino |
| Query | `me` | Usuario autenticado (o `null`) |
| Query | `myOrders` · `order(id)` | Órdenes del usuario, con pago y **línea de tiempo SAGA** |
| Mutation | `register` · `login` · `logout` | Gestión de cuenta y sesión |
| Mutation | `bookPackage(input)` | Ejecuta la SAGA de reserva (con `simulateFailure` opcional para la demo) |

### 7.2 Eliminación de over-fetching (de extremo a extremo)

1. El cliente pide solo lo que necesita, por ejemplo:
   ```graphql
   query {
     searchPackages(origin: "BOG", destination: "CTG", date: "2026-10-10") {
       flights { airline price }
     }
   }
   ```
2. Cada lista (`flights`, `hotels`, `cars`) tiene su propio *resolver*: si el cliente no pide `hotels`, **no se consulta** la tabla de hoteles.
3. El resolver lee los campos seleccionados (`info.selected_fields`) y construye la consulta a **pg_graphql pidiendo solo esas columnas**:
   ```graphql
   { flightsCollection(filter: {...}, orderBy: [{price: AscNullsLast}], first: 10) {
       edges { node { airline price } } } }
   ```
4. Los tres resolvers son asíncronos, así que vuelos, hoteles y autos se consultan **en paralelo**.

Los nombres de campos se validan contra una lista blanca derivada del esquema, y los valores se escapan como literales JSON: la entrada del usuario no puede alterar la consulta.

---

## 8. Patrón SAGA

Código: [`pipeline/flows/booking_saga.py`](../pipeline/flows/booking_saga.py) (el flow de Prefect) · [`services/orders/app/prefect_flow.py`](../services/orders/app/prefect_flow.py) (cómo Órdenes lo dispara y espera) · [`services/orders/app/saga.py`](../services/orders/app/saga.py) (cierre y recuperación de órdenes)

### 8.1 ¿Orquestación o coreografía?

Se eligió **orquestación**, y el orquestador es un **flow de Prefect** (`booking-saga`): el flow dirige los pasos y decide las compensaciones, mientras que el servicio de Órdenes solo crea la orden, dispara el flow y espera su resultado. Así cada reserva queda visible en el panel de Prefect, igual que la ingesta, y la SAGA y la ingesta comparten las mismas políticas de observabilidad.

| Criterio | Orquestación (elegida) | Coreografía |
|---|---|---|
| Visibilidad del flujo | Todo el flujo está en un solo lugar (`booking_saga.py`) y cada paso es una tarea visible en Prefect | Repartido entre servicios que reaccionan a eventos |
| Infraestructura | Solo HTTP y la API de Prefect, que ya existe por la ingesta | Requiere un *broker* de mensajes |
| Auditoría | Cada paso queda en `saga_steps` y en el historial de Prefect | Hay que reconstruir el flujo desde eventos |
| Demostración | La línea de tiempo y el grafo de tareas se muestran directamente | Más difícil de seguir |

### 8.2 Pasos y compensaciones

| Orden | Paso | Acción | Compensación |
|---|---|---|---|
| 1 | `PAYMENT` | Autorizar el pago (`payments.status = AUTHORIZED`) | Reembolso (`REFUNDED`) |
| 2 | `FLIGHT` | `POST flights-service/reservations` (descuenta asientos) | `POST .../reservations/{order_id}/cancel` (devuelve asientos) |
| 3 | `HOTEL` | `POST hotels-service/reservations` (descuenta habitaciones) | `POST .../cancel` (devuelve habitaciones) |
| 4 | `CAR` | `POST cars-service/reservations` (descuenta un auto) | `POST .../cancel` (devuelve el auto) |
| — | Éxito | Captura del pago (`CAPTURED`) y orden `CONFIRMED` | — |

Si un paso falla, se compensan **en orden inverso** solo los pasos ya completados.

En Prefect, cada ejecución es una tarea `execute-saga-step` y cada compensación una tarea `compensate-saga-step` (más `load-booking-order` y `capture-payment`). Cuando un paso falla, su tarea queda en rojo (`Failed`), las compensaciones en verde y el flow termina `Completed`: el fallo de negocio ya fue gestionado y la orden queda `CANCELLED`.

### 8.3 Diagrama de secuencia — camino exitoso (happy path)

```mermaid
sequenceDiagram
    autonumber
    actor U as Usuario
    participant GW as API Gateway
    participant O as Orders
    participant P as Prefect (API)
    participant S as Flow booking-saga (saga-pipeline)
    participant F as Flights
    participant H as Hotels
    participant C as Cars
    participant DB as Supabase

    U->>GW: mutation bookPackage(input)
    GW->>GW: valida sesión + rate limit (5/min por usuario)
    GW->>O: POST /orders (user_id tomado de la sesión)
    O->>F: GET /flights/{id} (cotización)
    O->>H: GET /hotels/{id}
    O->>C: GET /cars/{id}
    O->>DB: INSERT orders (PENDING)
    O->>P: crea la ejecución del deployment booking-saga
    P-->>S: entrega la ejecución (el ejecutor consulta cada 1 s)
    O->>P: consulta el estado cada 250 ms
    S->>DB: lee la orden y comprueba que sigue PENDING
    S->>DB: INSERT payments (AUTHORIZED)
    Note over S,DB: saga_steps: PAYMENT EXECUTE SUCCEEDED
    S->>F: POST /reservations
    F->>DB: descuenta asientos + INSERT flight_reservations
    F-->>S: 201 CONFIRMED
    S->>H: POST /reservations
    H->>DB: descuenta habitaciones + INSERT hotel_reservations
    H-->>S: 201 CONFIRMED
    S->>C: POST /reservations
    C->>DB: descuenta auto + INSERT car_reservations
    C-->>S: 201 CONFIRMED
    S->>DB: payments → CAPTURED · orders → CONFIRMED
    S-->>P: flow Completed
    P-->>O: estado COMPLETED
    O-->>GW: orden + sagaSteps
    GW-->>U: status CONFIRMED
```

### 8.4 Diagrama de secuencia — fallo y compensación (falla el auto)

```mermaid
sequenceDiagram
    autonumber
    actor U as Usuario
    participant GW as API Gateway
    participant O as Orders
    participant P as Prefect (API)
    participant S as Flow booking-saga (saga-pipeline)
    participant F as Flights
    participant H as Hotels
    participant C as Cars
    participant DB as Supabase

    U->>GW: bookPackage(input, simulateFailure: CAR)
    GW->>O: POST /orders
    O->>DB: orders (PENDING)
    O->>P: crea la ejecución de booking-saga
    P-->>S: entrega la ejecución
    S->>DB: payments (AUTHORIZED)
    S->>F: POST /reservations
    F-->>S: 201 CONFIRMED
    S->>H: POST /reservations
    H-->>S: 201 CONFIRMED
    S->>C: POST /reservations
    C-->>S: 503 Simulated failure in cars service
    Note over S,DB: tarea execute CAR → Failed · saga_steps: CAR EXECUTE FAILED · orders → COMPENSATING
    rect rgba(255, 170, 0, 0.12)
        Note over S: Compensación en orden inverso (tareas compensate-saga-step, cada una con hasta 3 intentos)
        S->>H: POST /reservations/{order_id}/cancel
        H->>DB: reserva CANCELLED + devuelve habitaciones
        H-->>S: CANCELLED
        S->>F: POST /reservations/{order_id}/cancel
        F->>DB: reserva CANCELLED + devuelve asientos
        F-->>S: CANCELLED
        S->>DB: payments → REFUNDED
    end
    S->>DB: orders → CANCELLED
    S-->>P: flow Completed (la orden quedó CANCELLED)
    P-->>O: estado COMPLETED
    O-->>GW: orden + sagaSteps (14 eventos)
    GW-->>U: status CANCELLED, motivo del fallo
```

### 8.5 Estados de una orden

```mermaid
stateDiagram-v2
    [*] --> PENDING: orden creada
    PENDING --> CONFIRMED: los 4 pasos tuvieron éxito
    PENDING --> COMPENSATING: un paso falló
    PENDING --> COMPENSATING: la ejecución en Prefect terminó mal
    PENDING --> CANCELLED: ningún ejecutor la recogió a tiempo, se cancela la ejecución
    COMPENSATING --> CANCELLED: todas las compensaciones tuvieron éxito
    COMPENSATING --> FAILED: una compensación falló tras 3 intentos (revisión manual)
    CONFIRMED --> [*]
    CANCELLED --> [*]
    FAILED --> [*]
```

### 8.6 Garantías de consistencia

| Mecanismo | Cómo funciona |
|---|---|
| **Compensación automática** | Sin intervención manual: el flow deshace los pasos completados en orden inverso |
| **Reintento de compensaciones** | Cada compensación se intenta hasta 3 veces con espera creciente (1 s, 2 s); si aun así falla, la orden queda `FAILED` para revisión |
| **Idempotencia de pasos** | `reserve` con un `order_id` ya confirmado devuelve la reserva existente; `cancel` sobre algo ya cancelado responde `NOTHING_TO_CANCEL`. Así los reintentos son seguros |
| **Idempotencia del checkout** | El frontend envía un `idempotencyKey` por intento de compra; repetirlo devuelve la misma orden sin ejecutar otra SAGA |
| **Operaciones atómicas de inventario** | `UPDATE ... SET seats_available = seats_available - n WHERE seats_available >= n` dentro de una transacción: nunca se sobrevende |
| **Recuperación tras caídas** | Al iniciar, el servicio de Órdenes busca órdenes `PENDING`/`COMPENSATING` con más de 1 minuto y compensa los pasos iniciados y no deshechos, eliminando las **reservas huérfanas** |
| **Auditoría** | Cada ejecución y compensación queda registrada en `saga_steps` (paso, acción, estado, error, hora) |
| **Fallos reales** | `simulateFailure` hace que el **servicio remoto** responda `503`; el flow no "finge" el fallo (solo el de `PAYMENT` se simula dentro del flow, porque el pago es un paso interno) |
| **Ejecución visible en Prefect** | Cada paso y cada compensación es una tarea del flow `booking-saga`: el panel muestra el grafo, los logs y el paso que falló |
| **Una conexión por reserva** | El flow reutiliza una sola conexión a la base, porque abrir una nueva cuesta ~1,5 s hacia Supabase: una reserva pasó de ~40 s a ~8–12 s |
| **Sin ejecuciones tardías** | Si ningún ejecutor recoge la ejecución en 45 s, Órdenes la cancela en Prefect, cierra la orden como `CANCELLED` con el motivo y responde 503; además, el flow no ejecuta nada si la orden ya no está `PENDING` |
| **Cierre inmediato ante fallos del flow** | Si la ejecución en Prefect termina mal, Órdenes compensa lo que alcanzó a iniciar (`saga.fail_order`) en vez de esperar a un reinicio |

### 8.7 Qué pasa cuando falla el propio orquestador

Como la SAGA corre en Prefect, las reservas dependen de que Prefect y el contenedor `saga-pipeline` estén vivos. Estos son los casos probados (§11):

| Situación | Qué ve el usuario | Estado de la orden | Ejecución en Prefect |
|---|---|---|---|
| Ningún ejecutor recoge la ejecución en 45 s (`saga-pipeline` caído) | Error "The booking service failed" tras ~51 s | `CANCELLED` con el motivo; sin eventos, pagos ni reservas | `Cancelled`: no se ejecuta aunque el ejecutor vuelva |
| La API de Prefect no responde al crear la ejecución | Error inmediato | `CANCELLED` con el motivo | no se crea |
| La ejecución termina `FAILED` o `CRASHED` | Error 503 | `CANCELLED` tras compensar lo que se alcanzó a iniciar | `Failed` / `Crashed` |
| La ejecución sigue corriendo pasados los 45 s | La orden se muestra `PENDING` ("Booking in progress…") | `PENDING` hasta que el flow termine | `Running` |
| Se reinicia `orders-service` con órdenes a medias | — | `CANCELLED`: la recuperación al arrancar deshace pagos y reservas | — |

---

## 9. Ciberseguridad por diseño

### 9.1 Gestión segura de sesiones — mitigación de Session Fixation

Código: [`gateway/app/sessions.py`](../gateway/app/sessions.py)

- Sesiones **del lado del servidor** en Redis; el navegador solo guarda un identificador aleatorio de 256 bits (`secrets.token_urlsafe(32)`) en la cookie `wsid`.
- Todo visitante recibe primero una **sesión anónima**. Al hacer **login** (y también al registrarse o cerrar sesión), ese identificador **se destruye y se emite uno nuevo**.
- Un identificador "plantado" por un atacante antes del login queda inservible después.

```mermaid
sequenceDiagram
    autonumber
    actor A as Atacante
    actor V as Víctima
    participant GW as API Gateway
    participant R as Redis

    A->>GW: visita el sitio
    GW->>R: crea sesión anónima S0
    GW-->>A: Set-Cookie: wsid=S0
    A-->>V: logra que la víctima use wsid=S0
    V->>GW: login (cookie wsid=S0)
    GW->>R: DEL session:S0
    GW->>R: SET session:S1 {user_id}
    GW-->>V: Set-Cookie: wsid=S1 (nuevo)
    A->>GW: { me } con wsid=S0
    GW->>R: GET session:S0 → no existe
    GW-->>A: me = null (la fijación falló)
```

| Control de la cookie / sesión | Valor |
|---|---|
| `HttpOnly` | JavaScript no puede leer la cookie (mitiga robo por XSS) |
| `SameSite=Lax` | No se envía en peticiones POST entre sitios (mitiga CSRF) |
| `Secure` | Configurable (`COOKIE_SECURE=true` cuando se sirve por HTTPS) |
| Expiración por inactividad | 30 minutos (ventana deslizante) |
| Vida máxima | 8 horas |

### 9.2 Almacenamiento de contraseñas — Argon2id

Código: [`gateway/app/auth.py`](../gateway/app/auth.py)

- **Argon2id** con `argon2-cffi`: memoria 64 MiB, 3 iteraciones, paralelismo 4 (perfil de baja memoria del RFC 9106). Ejemplo almacenado: `$argon2id$v=19$m=65536,t=3,p=4$...`
- El hash se calcula en un hilo aparte para no bloquear el servidor.
- **Rehash automático** si en el futuro se suben los parámetros (`check_needs_rehash`).
- La base de datos rechaza hashes que no sean Argon2id (restricción `check`).
- Contraseñas de 10 a 128 caracteres.
- **Tiempo constante**: si el email no existe, igual se verifica un hash ficticio para que el tiempo de respuesta no revele qué cuentas existen.
- **Bloqueo de cuenta**: 5 contraseñas incorrectas bloquean la cuenta 15 minutos.

### 9.3 Rate limiting

Código: [`gateway/app/ratelimit.py`](../gateway/app/ratelimit.py)

Algoritmo de **ventana deslizante** en Redis, ejecutado como un único script Lua atómico (dos peticiones simultáneas no pueden superar el límite). A diferencia de una ventana fija, no permite ráfagas dobles en el cambio de minuto.

| Ruta sensible | Límite | Clave |
|---|---|---|
| `login` | 5 intentos / 60 s | IP |
| `login` | 10 intentos / 15 min | cuenta (email) |
| `register` | 3 registros / 10 min | IP |
| `bookPackage` (checkout y pago) | 5 / 60 s | usuario |
| Cualquier petición a `/graphql` | 120 / 60 s | IP (respuesta HTTP 429 + `Retry-After`) |

La IP se toma del socket (`--no-proxy-headers`), no de cabeceras que el cliente podría falsificar.

### 9.4 Otros controles

| Control | Implementación |
|---|---|
| Autorización (IDOR) | `user_id` siempre sale de la sesión; un usuario no puede ver órdenes ajenas (se responden igual que inexistentes) |
| Abuso de GraphQL | `QueryDepthLimiter(6)` y `MaxAliasesLimiter(15)` (evita, por ejemplo, 100 logins en una sola petición) |
| Enmascaramiento de errores | Los errores internos se muestran como "Unexpected error"; solo los errores de negocio llegan al cliente |
| CORS | Solo el origen del frontend, con credenciales |
| Cabeceras HTTP del frontend | `Content-Security-Policy`, `X-Frame-Options: DENY`, `X-Content-Type-Options: nosniff`, `Referrer-Policy`, `Permissions-Policy` |
| Contenedores | Todos los servicios propios corren con usuario **no root**; nginx en imagen *unprivileged* |
| Red | Puertos de microservicios solo en `127.0.0.1`; Redis sin puerto publicado y con contraseña |
| Base de datos | RLS en todas las tablas; usuarios en esquema `private`; la clave pública no tiene acceso |
| Secretos | En `.env`, ignorado por Git; se publica solo `.env.example` |

### 9.5 Seguridad de la cadena de suministro

Script: [`scripts/security-audit.sh`](../scripts/security-audit.sh) · Reportes: [`docs/security/`](security/README.md)

- **pip-audit** se ejecuta *dentro de cada imagen construida* (audita lo realmente instalado, no solo `requirements.txt`).
- **npm audit** sobre el `package-lock.json` del frontend.

| Componente | Primera auditoría | Corrección | Resultado final |
|---|---|---|---|
| 7 imágenes Python | `pip 25.0.1` (heredado de la imagen base): 12 entradas de vulnerabilidad | `pip install --upgrade pip` en cada Dockerfile | Sin vulnerabilidades conocidas |
| Frontend | 4 vulnerabilidades (1 alta, 3 moderadas): `react-router` (GHSA-wrjc-x8rr-h8h6, GHSA-337j-9hxr-rhxg) y `esbuild`/`vite` (GHSA-67mh-4wv8-2f99) | `react-router-dom` 7.18.4 y `vite` 8 | 0 vulnerabilidades |

Ninguna librería de la aplicación (FastAPI, Strawberry, psycopg, Prefect, Dask…) tenía vulnerabilidades conocidas.

---

## 10. Despliegue con Docker Compose

### 10.1 Requisitos

- Docker con Docker Compose v2.
- Un proyecto de Supabase con la migración ejecutada (SQL Editor → pegar `supabase/migrations/...sql` → Run).

### 10.2 Configuración

Copiar `.env.example` a `.env` y completar:

| Variable | Origen |
|---|---|
| `SUPABASE_URL` | Supabase → Project Settings → API |
| `SUPABASE_SERVICE_KEY` | Supabase → Project Settings → API (clave `service_role` / secreta) |
| `DATABASE_URL` | Supabase → Connect → **Transaction pooler**, puerto 6543 (IPv4; la conexión directa del plan gratuito no ofrece IPv4) |
| `REDIS_PASSWORD` | Cualquier cadena larga aleatoria, solo letras y números (va dentro de una URL) |

> **Por qué el Transaction pooler y no el Session pooler (puerto 5432).** El modo sesión del plan gratuito de Supabase admite solo **15 clientes simultáneos** (`EMAXCONNSESSION`). Los pools de los servicios (hasta 2 conexiones cada uno) más los procesos de los flows pueden superar ese tope, y una vez saturado puede tardar más de 20 minutos en liberarse. El modo transacción multiplexa las conexiones y aguantó 40 clientes simultáneos en las pruebas. Exige no usar sentencias preparadas, y todas las conexiones del proyecto ya las desactivan (`prepare_threshold=None`).

### 10.3 Ejecución

```bash
docker compose up --build -d
```

Se levantan **14 contenedores**:

| URL | Servicio |
|---|---|
| http://localhost:3000 | Frontend |
| http://localhost:8080/graphql | API Gateway (explorador GraphiQL) |
| http://localhost:4200 | Panel de Prefect |
| http://localhost:8787 | Panel de Dask |
| http://localhost:8090 | Sitio simulado (fuente de scraping) |

Los servicios tienen *healthchecks* y `depends_on` con `condition: service_healthy`, por lo que arrancan en el orden correcto sin intervención manual. `orders-service` espera además a que `saga-pipeline` haya registrado el deployment `booking-saga` en Prefect, y la primera ingesta deja el catálogo con datos en ~1 minuto.

---

## 11. Pruebas y evidencias

| Prueba | Cómo ejecutarla | Resultado |
|---|---|---|
| Scraper contra el mock (sin tocar la base) | `docker compose exec -T pipeline python - < scripts/test_scraper.py` | **Todo OK**: parsea precios en 4 formatos, elimina duplicados (25 filas crudas → 24 hoteles), valida cabinas, IATA y zonas horarias, y sobrevive a los fallos inyectados (de 40 peticiones: 28 OK, 6 × 503, 5 timeouts, 1 × 429) |
| Ingesta Dask + Prefect | Panel de Prefect / `select * from scrape_runs` | 127 tareas por ejecución (124 páginas + 3 cargas) en ~40–65 s, reintentos visibles, ~690 filas, 2 workers repartiendo el trabajo |
| SAGA por consola | `docker compose exec orders-service python -m app.demo [CAR\|HOTEL\|FLIGHT\|PAYMENT]` | Camino exitoso `CONFIRMED`; fallo `CANCELLED` con compensaciones; cada reserva tarda ~8–12 s |
| Fallo en cada paso | `docker compose exec -T gateway python - < scripts/test_extra.py` (T3) | PAYMENT → nada que deshacer (2 eventos) · FLIGHT → reembolso (6) · HOTEL → vuelo + reembolso (10) · CAR → hotel + vuelo + reembolso (14); ninguna reserva huérfana |
| Consistencia e idempotencia | `scripts/test_extra.py` (T1, T2, T7) | Checkout duplicado = 1 orden y 8 eventos, también con dos envíos simultáneos · pagos y reservas coherentes con el estado de la orden · 0 reservas huérfanas |
| Gateway de extremo a extremo | `docker compose exec -T gateway python - < scripts/test_gateway.py` | **21/21 verificaciones** (búsqueda, session fixation, Argon2id, SAGA, autorización, rate limits, alias) |
| Bloqueo de cuenta y límites | `scripts/test_extra.py` (T4–T6) | Cuenta bloqueada tras 5 contraseñas incorrectas · 4.º registro en 10 min bloqueado · 130 consultas seguidas → HTTP 429 con `Retry-After` |
| Ejecutor de la SAGA caído | Manual: `docker compose stop saga-pipeline` y reservar | Error a los ~51 s; la orden queda `CANCELLED` con el motivo, sin eventos ni pagos; la ejecución queda `Cancelled` en Prefect y no se ejecuta al volver el ejecutor |
| Recuperación tras caídas | Manual: crear una orden a medias (pago, vuelo y hotel hechos) y `docker compose restart orders-service` | La orden queda `CANCELLED`, el pago `REFUNDED` y las reservas `CANCELLED` |
| Frontend de extremo a extremo | Manual, en el navegador: crear cuenta, buscar, reservar, "Fail at car", cerrar y abrir sesión | `CONFIRMED` / `CANCELLED` con Payment, Flight y Hotel *Undone* y Car *Failed*; el frontend solo llama al gateway y la consola no muestra errores |
| Auditoría de dependencias | `./scripts/security-audit.sh` | 8/8 componentes sin vulnerabilidades conocidas (regenerada el 9-oct-2026) |

---

## 12. Decisiones técnicas

| # | Decisión | Alternativas consideradas | Motivo |
|---|---|---|---|
| D1 | Python en todo el backend | Node.js para el gateway | Dask y Prefect son de Python; un solo lenguaje |
| D2 | SAGA **orquestada y ejecutada como flow de Prefect** | Coreografía con eventos; orquestador dentro de `orders-service` (la primera versión) | Flujo visible, sin broker, auditoría directa y cada reserva observable en Prefect, como pide el enunciado (§8.1) |
| D3 | Supabase Cloud (plan gratuito) | Supabase autoalojado | El autoalojado requiere ~10 contenedores más; Cloud ofrece pg_graphql listo y panel para la demo |
| D4 | Fuente de datos simulada | Scraping de Kayak/Booking reales | Permitido por el enunciado; evita bloqueos y términos de uso, y permite inyectar fallos controlados para demostrar los reintentos |
| D5 | Gateway lee el catálogo vía pg_graphql y los usuarios vía SQL | Todo por pg_graphql | El catálogo aprovecha GraphQL nativo; los usuarios quedan fuera de GraphQL por seguridad |
| D6 | Sesiones en servidor (Redis) | JWT en el navegador | Permite destruir y regenerar el identificador (requisito de Session Fixation) y revocar sesiones al instante |
| D7 | Ventana deslizante en Lua | Ventana fija; librería `slowapi` | Las pruebas demostraron que la ventana fija dejaba pasar ráfagas en el cambio de minuto; `slowapi` limita por ruta y GraphQL tiene una sola ruta |
| D8 | Una imagen para Prefect + Dask + los flows | Imágenes separadas | Dask exige versiones idénticas en cliente, scheduler y workers |
| D9 | Pool de conexiones pequeño por servicio | Pools grandes | El *pooler* de Supabase gratuito tiene pocas conexiones |
| D10 | *Transaction pooler* (puerto 6543) | *Session pooler* (5432) | El modo sesión del plan gratuito admite solo 15 clientes y se saturó (`EMAXCONNSESSION`); el modo transacción aguantó 40 clientes simultáneos (§10.2) |
| D11 | Una sola conexión por ejecución del flow de la SAGA | Una conexión por sentencia | Abrir una conexión cuesta ~1,5 s hacia Supabase: una reserva pasó de ~40 s a ~8–12 s y deja de agotar el pooler |
| D12 | El ejecutor de la SAGA consulta cada 1 s (`PREFECT_RUNNER_POLL_FREQUENCY=1`) | Valor por defecto (10 s) | Recoge la ejecución en ~1 s en lugar de hasta 10 s |
| D13 | Cancelar la ejecución y cerrar la orden si la SAGA no arranca | Dejarla en cola | Evita que una reserva que el usuario vio fallida se ejecute al volver el ejecutor (§8.7) |
| D14 | Fijar `sqlalchemy>=2.0,<2.1` en el pipeline | Dejar la versión libre | Con SQLAlchemy 2.1.4, el programador de Prefect 3.8.8 fallaba (`Can't evaluate bulk DML statement`) y no se creaban las ejecuciones programadas |

---

## 13. Limitaciones conocidas y trabajo futuro

| Limitación | Impacto | Mejora propuesta |
|---|---|---|
| El scraper sobrescribe la disponibilidad (`seats_available`, etc.) cada 5 minutos | El inventario descontado por reservas se "repone" con el dato del proveedor (que se considera la fuente de verdad) | Llevar las reservas propias en una tabla aparte y restarlas de la disponibilidad del proveedor |
| La petición espera el resultado de la SAGA | Una reserva tarda ~8–12 s con la interfaz mostrando "Booking…" | Responder de inmediato y notificar al frontend (polling o WebSocket) |
| Las reservas dependen de Prefect y del contenedor `saga-pipeline` | Sin ejecutor, la reserva falla tras ~45 s (la orden queda `CANCELLED` con el motivo y no se ejecuta después, §8.7) | Varias réplicas del ejecutor y Prefect con alta disponibilidad |
| Si la ejecución sigue corriendo pasados los 45 s, la petición devuelve la orden `PENDING` | La interfaz muestra "Booking in progress…" y la página no se actualiza sola | Consultar el estado de la orden hasta que sea final |
| Si un `cancel` llega antes de que un `reserve` lento termine, la reserva puede quedar viva | Caso raro de carrera ante *timeouts* | Registrar una "lápida" de cancelación por `order_id` que bloquee reservas posteriores |
| Recuperación tras caídas y bloqueo de cuenta | Probados en la verificación final (§11), pero no hay pruebas automáticas continuas | Ejecutar `test_gateway.py` y `test_extra.py` en un pipeline de integración continua |
| El equipo anfitrión se suspende con el stack arriba | Se cortan las conexiones a Supabase: las ejecuciones atrasadas fallan (`SSL error: unexpected eof`) y dejan filas `RUNNING` en `scrape_runs` | Evitar la suspensión durante la demostración (`caffeinate -dims`) y marcar las filas colgadas como fallidas (`scripts/cleanup_test_data.sql`) |
| Cuando una página se pierde tras sus reintentos aparece un `CRITICAL ... Failed to deserialize` en los logs del pipeline | Cosmético: la ingesta termina `SUCCEEDED` y la página queda anotada en `scrape_runs.error` | Convertir `HTTPStatusError` en una excepción serializable por Dask |
| Dos stacks contra el mismo proyecto de Supabase | Duplican la ingesta y comparten el límite de conexiones del pooler | Usar un solo stack a la vez, o un proyecto de Supabase por integrante |
| Supabase gratuito pausa el proyecto tras ~7 días sin uso | La primera petición tras la pausa falla | Abrir el panel de Supabase antes de la demostración |
| HTTP local sin TLS | `COOKIE_SECURE=false` | En producción, servir por HTTPS y activar `COOKIE_SECURE=true` |

---

## 14. Guía de la demostración

**Antes de grabar:**

1. `docker compose up -d` y esperar a que todos los contenedores estén *healthy* (`docker compose ps`; son 14, y 3 de ellos no tienen *healthcheck*). La primera ingesta deja el catálogo con datos en ~1 minuto.
2. Evitar que el equipo se suspenda mientras el stack está arriba (`caffeinate -dims` en una terminal): al suspenderse se cortan las conexiones a Supabase y las ejecuciones atrasadas quedan `Failed` en el historial de Prefect.
3. Usar un solo stack contra el proyecto de Supabase (no tener otro levantado en otro equipo).
4. Buscar **Bogotá → Cartagena** con una fecha entre mañana y +13 días (son las rutas y fechas que scrapea el flujo). No crear más de 3 cuentas en 10 minutos desde la misma IP: el 4.º registro se bloquea por *rate limit*. Si hace falta repetir la toma, `docker compose exec -T redis redis-cli FLUSHALL` reinicia los contadores (y cierra todas las sesiones).
5. Opcional, para un historial de Prefect sin ejecuciones fallidas de pruebas anteriores: `docker compose down -v` y luego `docker compose up -d`.

| Parte | Qué mostrar | Dónde |
|---|---|---|
| **(a)** Prefect monitoreando flujos | Los dos deployments: `scheduled-ingestion` (ingesta) y `booking-saga` (SAGA). En la ingesta: una ejecución completada, tareas con **reintentos** (estado *AwaitingRetry*) y sus logs; lanzar otra con **Quick run**. En `booking-saga`: la ejecución de cada reserva con su grafo de tareas | http://localhost:4200 |
| **(b)** Tareas distribuidas en Dask | Durante esa ejecución: barras de tareas repartidas entre los 2 workers (*Status* y *Workers*) | http://localhost:8787 |
| **(c)** Frontend consumiendo GraphQL | Crear cuenta, buscar Bogotá → Cartagena, elegir vuelo + hotel + auto, reservar (la interfaz muestra "Booking…" unos ~8–12 s) → orden `CONFIRMED`. Opcional: pestaña *Network* del navegador mostrando que todas las peticiones van a `/graphql` | http://localhost:3000 |
| **(d)** Fallo transaccional con compensación SAGA | Reservar de nuevo con **"Demo: simulate a failure" → Fail at car**: Payment, Flight y Hotel aparecen como *Undone*, Car como *Failed*, y el registro de eventos muestra las compensaciones en orden inverso. Abrir en Prefect la ejecución `booking-<id>` de esa orden: `execute CAR` en rojo y las tres tareas `compensate` en verde. Mostrar también `saga_steps` en Supabase | Frontend + Prefect + Supabase |

Puntos extra para la sustentación: mostrar en el navegador (*DevTools → Application → Cookies*) que el valor de `wsid` **cambia** al hacer login (Session Fixation), y ejecutar `./scripts/security-audit.sh` o abrir `docs/security/README.md`. Para evidenciar las pruebas: `scripts/test_gateway.py` (21 verificaciones), `scripts/test_extra.py` (22) y `scripts/test_scraper.py` (§11).
