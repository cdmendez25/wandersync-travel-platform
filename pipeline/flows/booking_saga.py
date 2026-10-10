"""Prefect flow that orchestrates the travel-booking SAGA.

Each execution and compensation is a Prefect task, so the booking timeline is
available in the Prefect UI as well as in the existing ``saga_steps`` audit
table.
"""
import os
import time
from contextlib import contextmanager
from uuid import UUID, uuid4

import httpx
import psycopg
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from psycopg.rows import dict_row

DATABASE_URL = os.environ["DATABASE_URL"]
FLIGHTS_URL = os.getenv("FLIGHTS_URL", "http://flights-service:8000")
HOTELS_URL = os.getenv("HOTELS_URL", "http://hotels-service:8000")
CARS_URL = os.getenv("CARS_URL", "http://cars-service:8000")
SERVICE_TIMEOUT = float(os.getenv("SERVICE_TIMEOUT_SECONDS", "5"))

STEPS = ("PAYMENT", "FLIGHT", "HOTEL", "CAR")
COMPENSATION_ATTEMPTS = 3


class StepError(Exception):
    """A SAGA action that requires compensation has failed."""


# Prefect's runner cloudpickles this module's globals to start each flow run in a subprocess,
# so they must stay picklable (no locks here); psycopg already serializes access to a connection.
_conn: psycopg.Connection | None = None


@contextmanager
def _connect():
    """Yield the one connection shared by the whole flow run (statements run in autocommit).

    Opening a connection costs ~1.5 s against Supabase (TLS + auth through the pooler), so
    one per statement made a booking take ~40 s and could exhaust the pooler's client limit.
    """
    global _conn
    if _conn is None or _conn.closed or _conn.broken:
        _conn = psycopg.connect(
            DATABASE_URL,
            prepare_threshold=None,
            sslmode="require",
            row_factory=dict_row,
            connect_timeout=10,
            autocommit=True,
        )
    yield _conn


def _close_connection() -> None:
    global _conn
    if _conn is not None and not _conn.closed:
        _conn.close()
    _conn = None


def _call_service(method: str, url: str, payload: dict | None = None) -> dict:
    """Call an idempotent booking service, retrying one transient failure."""
    for attempt in (1, 2):
        try:
            response = httpx.request(method, url, json=payload, timeout=SERVICE_TIMEOUT)
        except httpx.TransportError as exc:
            if attempt == 2:
                raise StepError(f"{url} unreachable: {exc!r}") from exc
            time.sleep(0.5)
            continue
        if response.is_error:
            try:
                detail = response.json().get("detail", response.text)
            except ValueError:
                detail = response.text
            raise StepError(f"HTTP {response.status_code}: {detail}")
        return response.json()
    raise AssertionError("unreachable")


def _log_step(order_id: str, step: str, action: str, status: str, error: str | None = None) -> None:
    with _connect() as conn:
        conn.execute(
            "insert into public.saga_steps (order_id, step, action, status, error) values (%s, %s, %s, %s, %s)",
            (order_id, step, action, status, error),
        )


def _set_status(order_id: str, status: str, reason: str | None = None) -> None:
    with _connect() as conn:
        conn.execute(
            "update public.orders set status = %s, failure_reason = coalesce(%s, failure_reason) where id = %s",
            (status, reason, order_id),
        )


def _execute_payment(order: dict, fail: bool) -> None:
    status = "FAILED" if fail else "AUTHORIZED"
    with _connect() as conn:
        conn.execute(
            "insert into public.payments (order_id, amount, currency, status, provider_ref) "
            "values (%s, %s, %s, %s, %s) on conflict (order_id) do nothing",
            (order["id"], order["total_amount"], order["currency"], status, f"MOCKPAY-{uuid4().hex[:10]}"),
        )
    if fail:
        raise StepError("Payment declined (simulated)")


def _execute_flight(order: dict, fail: bool) -> None:
    _call_service("POST", f"{FLIGHTS_URL}/reservations", {
        "order_id": str(order["id"]),
        "flight_id": str(order["flight_id"]),
        "seats": order["passengers"],
        "simulate_failure": fail,
    })


def _execute_hotel(order: dict, fail: bool) -> None:
    _call_service("POST", f"{HOTELS_URL}/reservations", {
        "order_id": str(order["id"]),
        "hotel_id": str(order["hotel_id"]),
        "rooms": (order["passengers"] + 1) // 2,
        "check_in": order["check_in"].isoformat(),
        "check_out": order["check_out"].isoformat(),
        "simulate_failure": fail,
    })


def _execute_car(order: dict, fail: bool) -> None:
    _call_service("POST", f"{CARS_URL}/reservations", {
        "order_id": str(order["id"]),
        "car_id": str(order["car_id"]),
        "pickup_date": order["check_in"].isoformat(),
        "return_date": order["check_out"].isoformat(),
        "simulate_failure": fail,
    })


def _refund_payment(order: dict) -> None:
    with _connect() as conn:
        conn.execute(
            "update public.payments set status = 'REFUNDED' "
            "where order_id = %s and status in ('AUTHORIZED', 'CAPTURED')",
            (order["id"],),
        )


def _cancel_flight(order: dict) -> None:
    _call_service("POST", f"{FLIGHTS_URL}/reservations/{order['id']}/cancel")


def _cancel_hotel(order: dict) -> None:
    _call_service("POST", f"{HOTELS_URL}/reservations/{order['id']}/cancel")


def _cancel_car(order: dict) -> None:
    _call_service("POST", f"{CARS_URL}/reservations/{order['id']}/cancel")


EXECUTE = {
    "PAYMENT": _execute_payment,
    "FLIGHT": _execute_flight,
    "HOTEL": _execute_hotel,
    "CAR": _execute_car,
}
COMPENSATE = {
    "PAYMENT": _refund_payment,
    "FLIGHT": _cancel_flight,
    "HOTEL": _cancel_hotel,
    "CAR": _cancel_car,
}


@task(name="load-booking-order", cache_policy=NO_CACHE, persist_result=False)
def load_order(order_id: str) -> dict:
    with _connect() as conn:
        order = conn.execute("select * from public.orders where id = %s", (order_id,)).fetchone()
    if not order:
        raise ValueError(f"Order {order_id} does not exist")
    return order


@task(
    name="execute-saga-step",
    task_run_name="execute {step}",
    cache_policy=NO_CACHE,
    persist_result=False,
)
def execute_step(order: dict, step: str, fail_at: str | None) -> None:
    order_id = str(order["id"])
    logger = get_run_logger()
    _log_step(order_id, step, "EXECUTE", "STARTED")
    try:
        EXECUTE[step](order, fail_at == step)
    except Exception as exc:
        _log_step(order_id, step, "EXECUTE", "FAILED", str(exc))
        logger.exception("order=%s step=%s execution failed", order_id, step)
        raise
    _log_step(order_id, step, "EXECUTE", "SUCCEEDED")
    logger.info("order=%s step=%s execution succeeded", order_id, step)


@task(
    name="compensate-saga-step",
    task_run_name="compensate {step}",
    cache_policy=NO_CACHE,
    persist_result=False,
)
def compensate_step(order: dict, step: str) -> None:
    order_id = str(order["id"])
    logger = get_run_logger()
    _log_step(order_id, step, "COMPENSATE", "STARTED")
    for attempt in range(1, COMPENSATION_ATTEMPTS + 1):
        try:
            COMPENSATE[step](order)
        except Exception as exc:
            if attempt == COMPENSATION_ATTEMPTS:
                _log_step(order_id, step, "COMPENSATE", "FAILED", str(exc))
                logger.exception("order=%s step=%s compensation failed", order_id, step)
                raise
            logger.warning(
                "order=%s step=%s compensation attempt %s/%s failed: %s",
                order_id, step, attempt, COMPENSATION_ATTEMPTS, exc,
            )
            time.sleep(0.5 * 2 ** attempt)
            continue
        _log_step(order_id, step, "COMPENSATE", "SUCCEEDED")
        logger.info("order=%s step=%s compensation succeeded", order_id, step)
        return


@task(name="capture-payment", cache_policy=NO_CACHE, persist_result=False)
def capture_payment(order_id: str) -> None:
    with _connect() as conn:
        conn.execute(
            "update public.payments set status = 'CAPTURED' where order_id = %s and status = 'AUTHORIZED'",
            (order_id,),
        )


@flow(name="booking-saga", flow_run_name="booking-{order_id}", log_prints=True)
def booking_saga(order_id: str, fail_at: str | None = None) -> str:
    """Execute a booking SAGA and expose every action in Prefect."""
    try:
        return _run_saga(order_id, fail_at)
    finally:
        _close_connection()


def _run_saga(order_id: str, fail_at: str | None) -> str:
    UUID(order_id)  # fail early with a clear invalid-parameter error
    if fail_at is not None and fail_at not in STEPS:
        raise ValueError(f"Unknown SAGA step {fail_at!r}")

    order = load_order(order_id)
    if order["status"] != "PENDING":
        # The orders service already settled it (for example after its timeout): never book it late
        get_run_logger().warning("order=%s is %s, not PENDING: nothing to execute", order_id, order["status"])
        return order["status"]
    completed: list[str] = []
    for step in STEPS:
        try:
            execute_step(order, step, fail_at)
        except Exception as exc:
            _set_status(order_id, "COMPENSATING", f"{step} failed: {exc}")
            compensated = True
            for completed_step in reversed(completed):
                try:
                    compensate_step(order, completed_step)
                except Exception:
                    compensated = False
            final = "CANCELLED" if compensated else "FAILED"
            _set_status(order_id, final)
            return final
        completed.append(step)

    capture_payment(order_id)
    _set_status(order_id, "CONFIRMED")
    return "CONFIRMED"


if __name__ == "__main__":
    booking_saga.serve(name="booking-saga", tags=["orders", "saga"])
