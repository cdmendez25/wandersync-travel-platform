-- =====================================================================
-- WanderSync Travel Platform - Initial schema
-- Target: Supabase (PostgreSQL 15+ with pg_graphql)
--
-- Layout:
--   public   -> exposed through pg_graphql (/graphql/v1)
--               catalog (flights, hotels, cars), orders, reservations,
--               payments, saga log, ingestion log
--   private  -> NOT exposed through GraphQL (users / credentials)
--
-- Access model:
--   * RLS is enabled on every table with no policies, and anon /
--     authenticated have no privileges -> the public anon key can read
--     nothing.
--   * Backend services use the service_role key (GraphQL) or the pooler
--     DATABASE_URL (direct SQL), both of which bypass RLS.
-- =====================================================================

-- ---------------------------------------------------------------------
-- pg_graphql naming: snake_case columns -> camelCase GraphQL fields
-- (departure_at -> departureAt). Collections stay as <table>Collection.
-- ---------------------------------------------------------------------
comment on schema public is e'@graphql({"inflect_names": true})';

-- ---------------------------------------------------------------------
-- Shared trigger: keep updated_at current
-- ---------------------------------------------------------------------
create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at = now();
  return new;
end;
$$;


-- =====================================================================
-- PRIVATE SCHEMA (identity) - hidden from GraphQL
-- =====================================================================
create schema if not exists private;
revoke all on schema private from public, anon, authenticated;
grant usage on schema private to service_role;

create table private.users (
  id             uuid primary key default gen_random_uuid(),
  email          text not null,
  password_hash  text not null,          -- Argon2id hash ($argon2id$...)
  full_name      text not null,
  failed_logins  integer not null default 0,
  locked_until   timestamptz,
  created_at     timestamptz not null default now(),
  updated_at     timestamptz not null default now(),
  constraint users_email_format check (email ~* '^[^@\s]+@[^@\s]+\.[^@\s]+$'),
  constraint users_hash_is_argon2id check (password_hash like '$argon2id$%')
);

create unique index users_email_lower_uk on private.users (lower(email));

create trigger users_set_updated_at
  before update on private.users
  for each row execute function public.set_updated_at();

alter table private.users enable row level security;
grant select, insert, update on private.users to service_role;


-- =====================================================================
-- CATALOG (filled by the Prefect + Dask ingestion pipeline)
-- (provider, external_id) is the natural key used for UPSERTs so that
-- repeated scraping updates rows instead of growing the table.
-- =====================================================================

create table public.flights (
  id               uuid primary key default gen_random_uuid(),
  provider         text not null,                 -- e.g. 'mock-kayak'
  external_id      text not null,                 -- id at the source
  airline          text not null,
  flight_number    text not null,
  origin           char(3) not null,              -- IATA code
  destination      char(3) not null,
  departure_at     timestamptz not null,
  arrival_at       timestamptz not null,
  cabin_class      text not null default 'ECONOMY',
  price            numeric(12,2) not null,
  currency         char(3) not null default 'USD',
  seats_available  integer not null,
  scraped_at       timestamptz not null default now(),
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  constraint flights_source_uk unique (provider, external_id),
  constraint flights_route_chk check (origin <> destination),
  constraint flights_times_chk check (arrival_at > departure_at),
  constraint flights_price_chk check (price >= 0),
  constraint flights_seats_chk check (seats_available >= 0),
  constraint flights_cabin_chk check (cabin_class in ('ECONOMY','PREMIUM','BUSINESS','FIRST'))
);

create index flights_search_idx on public.flights (origin, destination, departure_at);

create trigger flights_set_updated_at
  before update on public.flights
  for each row execute function public.set_updated_at();


create table public.hotels (
  id               uuid primary key default gen_random_uuid(),
  provider         text not null,                 -- e.g. 'mock-booking'
  external_id      text not null,
  name             text not null,
  city             text not null,
  country          char(2) not null,              -- ISO 3166-1 alpha-2
  address          text,
  stars            smallint,
  room_type        text not null default 'STANDARD',
  price_per_night  numeric(12,2) not null,
  currency         char(3) not null default 'USD',
  rooms_available  integer not null,
  rating           numeric(3,1),
  scraped_at       timestamptz not null default now(),
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  constraint hotels_source_uk unique (provider, external_id),
  constraint hotels_stars_chk check (stars between 1 and 5),
  constraint hotels_price_chk check (price_per_night >= 0),
  constraint hotels_rooms_chk check (rooms_available >= 0),
  constraint hotels_rating_chk check (rating between 0 and 10)
);

create index hotels_city_idx on public.hotels (lower(city));

create trigger hotels_set_updated_at
  before update on public.hotels
  for each row execute function public.set_updated_at();


create table public.cars (
  id               uuid primary key default gen_random_uuid(),
  provider         text not null,                 -- e.g. 'mock-rentalcars'
  external_id      text not null,
  company          text not null,
  model            text not null,
  category         text not null,
  city             text not null,
  seats            smallint not null default 5,
  transmission     text not null default 'AUTOMATIC',
  price_per_day    numeric(12,2) not null,
  currency         char(3) not null default 'USD',
  units_available  integer not null,
  scraped_at       timestamptz not null default now(),
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  constraint cars_source_uk unique (provider, external_id),
  constraint cars_category_chk check (category in ('ECONOMY','COMPACT','SUV','LUXURY','VAN')),
  constraint cars_transmission_chk check (transmission in ('AUTOMATIC','MANUAL')),
  constraint cars_price_chk check (price_per_day >= 0),
  constraint cars_units_chk check (units_available >= 0)
);

create index cars_city_idx on public.cars (lower(city));

create trigger cars_set_updated_at
  before update on public.cars
  for each row execute function public.set_updated_at();

-- Enable totalCount on catalog collections (pagination in the frontend)
comment on table public.flights is e'@graphql({"totalCount": {"enabled": true}})';
comment on table public.hotels  is e'@graphql({"totalCount": {"enabled": true}})';
comment on table public.cars    is e'@graphql({"totalCount": {"enabled": true}})';


-- =====================================================================
-- INGESTION LOG (one row per Prefect task run against a source)
-- =====================================================================
create table public.scrape_runs (
  id                uuid primary key default gen_random_uuid(),
  flow_run_id       text,                         -- Prefect flow run id
  source            text not null,                -- flights | hotels | cars
  provider          text not null,
  status            text not null default 'RUNNING',
  attempts          integer not null default 1,   -- Prefect retries used
  records_fetched   integer not null default 0,
  records_upserted  integer not null default 0,
  error             text,
  started_at        timestamptz not null default now(),
  finished_at       timestamptz,
  constraint scrape_runs_source_chk check (source in ('flights','hotels','cars')),
  constraint scrape_runs_status_chk check (status in ('RUNNING','SUCCEEDED','FAILED'))
);

create index scrape_runs_started_idx on public.scrape_runs (started_at desc);


-- =====================================================================
-- ORDERS / BILLING (owned by the Orders service = SAGA orchestrator)
-- =====================================================================
create table public.orders (
  id               uuid primary key default gen_random_uuid(),
  user_id          uuid not null,                 -- private.users.id (no FK across schemas on purpose)
  idempotency_key  text not null,                 -- sent by gateway; avoids double checkout
  status           text not null default 'PENDING',
  flight_id        uuid not null references public.flights(id),
  hotel_id         uuid not null references public.hotels(id),
  car_id           uuid not null references public.cars(id),
  passengers       smallint not null default 1,
  check_in         date not null,
  check_out        date not null,
  total_amount     numeric(12,2) not null,
  currency         char(3) not null default 'USD',
  failure_reason   text,
  created_at       timestamptz not null default now(),
  updated_at       timestamptz not null default now(),
  constraint orders_idempotency_uk unique (idempotency_key),
  constraint orders_status_chk check (status in
    ('PENDING','CONFIRMED','COMPENSATING','CANCELLED','FAILED')),
  constraint orders_dates_chk check (check_out > check_in),
  constraint orders_passengers_chk check (passengers between 1 and 9),
  constraint orders_amount_chk check (total_amount >= 0)
);

create index orders_user_idx on public.orders (user_id, created_at desc);

create trigger orders_set_updated_at
  before update on public.orders
  for each row execute function public.set_updated_at();


create table public.payments (
  id           uuid primary key default gen_random_uuid(),
  order_id     uuid not null references public.orders(id) on delete cascade,
  amount       numeric(12,2) not null,
  currency     char(3) not null default 'USD',
  status       text not null default 'AUTHORIZED',
  provider_ref text,                              -- mock payment gateway reference
  created_at   timestamptz not null default now(),
  updated_at   timestamptz not null default now(),
  constraint payments_order_uk unique (order_id),
  constraint payments_status_chk check (status in ('AUTHORIZED','CAPTURED','REFUNDED','FAILED')),
  constraint payments_amount_chk check (amount >= 0)
);

create trigger payments_set_updated_at
  before update on public.payments
  for each row execute function public.set_updated_at();


-- =====================================================================
-- RESERVATIONS (each table is owned by exactly one microservice)
-- unique(order_id) makes reserve/cancel idempotent when the SAGA retries.
-- =====================================================================
create table public.flight_reservations (
  id            uuid primary key default gen_random_uuid(),
  order_id      uuid not null references public.orders(id) on delete cascade,
  flight_id     uuid not null references public.flights(id),
  seats         smallint not null,
  unit_price    numeric(12,2) not null,
  status        text not null default 'CONFIRMED',
  created_at    timestamptz not null default now(),
  cancelled_at  timestamptz,
  constraint flight_res_order_uk unique (order_id),
  constraint flight_res_status_chk check (status in ('CONFIRMED','CANCELLED')),
  constraint flight_res_seats_chk check (seats > 0)
);

create table public.hotel_reservations (
  id            uuid primary key default gen_random_uuid(),
  order_id      uuid not null references public.orders(id) on delete cascade,
  hotel_id      uuid not null references public.hotels(id),
  rooms         smallint not null default 1,
  check_in      date not null,
  check_out     date not null,
  unit_price    numeric(12,2) not null,
  status        text not null default 'CONFIRMED',
  created_at    timestamptz not null default now(),
  cancelled_at  timestamptz,
  constraint hotel_res_order_uk unique (order_id),
  constraint hotel_res_status_chk check (status in ('CONFIRMED','CANCELLED')),
  constraint hotel_res_dates_chk check (check_out > check_in),
  constraint hotel_res_rooms_chk check (rooms > 0)
);

create table public.car_reservations (
  id            uuid primary key default gen_random_uuid(),
  order_id      uuid not null references public.orders(id) on delete cascade,
  car_id        uuid not null references public.cars(id),
  pickup_date   date not null,
  return_date   date not null,
  unit_price    numeric(12,2) not null,
  status        text not null default 'CONFIRMED',
  created_at    timestamptz not null default now(),
  cancelled_at  timestamptz,
  constraint car_res_order_uk unique (order_id),
  constraint car_res_status_chk check (status in ('CONFIRMED','CANCELLED')),
  constraint car_res_dates_chk check (return_date > pickup_date)
);


-- =====================================================================
-- SAGA LOG (append-only; one row per step execution or compensation)
-- This is what you show in the demo to prove compensations ran.
-- =====================================================================
create table public.saga_steps (
  id          bigint generated always as identity primary key,
  order_id    uuid not null references public.orders(id) on delete cascade,
  step        text not null,                      -- PAYMENT | FLIGHT | HOTEL | CAR
  action      text not null,                      -- EXECUTE | COMPENSATE
  status      text not null,                      -- STARTED | SUCCEEDED | FAILED
  error       text,
  created_at  timestamptz not null default now(),
  constraint saga_steps_step_chk check (step in ('PAYMENT','FLIGHT','HOTEL','CAR')),
  constraint saga_steps_action_chk check (action in ('EXECUTE','COMPENSATE')),
  constraint saga_steps_status_chk check (status in ('STARTED','SUCCEEDED','FAILED'))
);

create index saga_steps_order_idx on public.saga_steps (order_id, id);


-- =====================================================================
-- SECURITY: RLS everywhere, no policies, no anon/authenticated access
-- =====================================================================
alter table public.flights              enable row level security;
alter table public.hotels               enable row level security;
alter table public.cars                 enable row level security;
alter table public.scrape_runs          enable row level security;
alter table public.orders               enable row level security;
alter table public.payments             enable row level security;
alter table public.flight_reservations  enable row level security;
alter table public.hotel_reservations   enable row level security;
alter table public.car_reservations     enable row level security;
alter table public.saga_steps           enable row level security;

-- Defense in depth: also remove table privileges, which hides these
-- tables from GraphQL introspection for the public anon key.
revoke all on all tables in schema public from anon, authenticated;
revoke all on all sequences in schema public from anon, authenticated;
alter default privileges in schema public revoke all on tables from anon, authenticated;
alter default privileges in schema public revoke all on sequences from anon, authenticated;
