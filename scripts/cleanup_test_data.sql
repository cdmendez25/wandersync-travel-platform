-- =====================================================================
-- WanderSync - Limpieza de datos de prueba (para el SQL Editor de Supabase)
--
-- NO se ejecuta solo: lo ejecutas tú, por partes y en este orden.
--   PARTE 1  Vista previa. Solo SELECT, no modifica nada. Revisa los números.
--   PARTE 2  Borrado. IRREVERSIBLE. Ejecútala solo si la vista previa es lo esperado.
--   PARTE 3  Marca como FALLIDAS las ejecuciones de la ingesta que quedaron "RUNNING"
--            para siempre (no las borra, conserva la evidencia).
--   PARTE 4  Comprobación final.
--
-- En el SQL Editor, selecciona solo el bloque que quieres correr y pulsa Run.
--
-- Qué se considera "dato de prueba":
--   * usuarios cuyo email termina en @test.com (los crean test_gateway.py, test_extra.py
--     y las pruebas manuales);
--   * las órdenes de esos usuarios y las órdenes cuyo usuario ya no existe (las crea
--     `python -m app.demo` con un user_id aleatorio).
--   Los pagos, reservas y eventos de la SAGA se borran solos: sus llaves foráneas hacia
--   orders son ON DELETE CASCADE.
--
-- Borrar órdenes NO devuelve el inventario (asientos, habitaciones, autos). No hace falta:
-- el scraper lo reescribe cada 5 minutos con los datos del proveedor.
-- =====================================================================


-- ---------------------------------------------------------------------
-- PARTE 1 - VISTA PREVIA (solo lectura)
-- ---------------------------------------------------------------------

-- 1a) Cuántas filas se borrarían, tabla por tabla
with a_borrar as (
  select id from public.orders
  where user_id in (select id from private.users where email like '%@test.com')
     or user_id not in (select id from private.users)
)
select 'orders'              as tabla, count(*) as filas from a_borrar
union all select 'payments',            count(*) from public.payments            where order_id in (select id from a_borrar)
union all select 'saga_steps',          count(*) from public.saga_steps          where order_id in (select id from a_borrar)
union all select 'flight_reservations', count(*) from public.flight_reservations where order_id in (select id from a_borrar)
union all select 'hotel_reservations',  count(*) from public.hotel_reservations  where order_id in (select id from a_borrar)
union all select 'car_reservations',    count(*) from public.car_reservations    where order_id in (select id from a_borrar)
union all select 'usuarios @test.com (se borran)',       count(*) from private.users where email like '%@test.com'
union all select 'usuarios que NO son de prueba (se conservan)', count(*) from private.users where email not like '%@test.com';

-- 1b) Los usuarios, por si quieres conservar alguno (cámbiale el email o excluye su fila en la PARTE 2)
select email, to_char(created_at at time zone 'America/Bogota', 'YYYY-MM-DD HH24:MI') as creado
from private.users
order by created_at;

-- 1c) Ejecuciones de la ingesta que quedaron "RUNNING" hace más de 15 minutos
select id, source, status, to_char(started_at at time zone 'America/Bogota', 'YYYY-MM-DD HH24:MI:SS') as inicio
from public.scrape_runs
where status = 'RUNNING' and started_at < now() - interval '15 minutes'
order by started_at;


-- ---------------------------------------------------------------------
-- PARTE 2 - BORRADO (IRREVERSIBLE)
-- El orden importa: primero las órdenes (su condición consulta a los usuarios) y luego los usuarios.
-- ---------------------------------------------------------------------
begin;

delete from public.orders
where user_id in (select id from private.users where email like '%@test.com')
   or user_id not in (select id from private.users);

delete from private.users
where email like '%@test.com';

commit;


-- ---------------------------------------------------------------------
-- PARTE 3 - Filas colgadas de scrape_runs: se marcan FAILED (no se borran)
-- El umbral de 15 minutos evita tocar la ejecución que esté corriendo ahora (dura ~1 minuto).
-- ---------------------------------------------------------------------
update public.scrape_runs
set status      = 'FAILED',
    finished_at = coalesce(finished_at, now()),
    error       = coalesce(error, 'Interrumpida: el equipo se suspendió o el contenedor se detuvo antes de terminar')
where status = 'RUNNING'
  and started_at < now() - interval '15 minutes';


-- ---------------------------------------------------------------------
-- PARTE 4 - Comprobación final
-- ---------------------------------------------------------------------
select (select count(*) from private.users)                               as usuarios,
       (select count(*) from public.orders)                                as ordenes,
       (select count(*) from public.saga_steps)                            as eventos_saga,
       (select count(*) from public.scrape_runs where status = 'RUNNING')  as scrape_runs_en_running;
