"""SAGA orchestrator for booking a travel package.

    PAYMENT -> FLIGHT -> HOTEL -> CAR      (execute, in order)
    if a step fails: undo the completed steps in reverse order (compensate)

Every execution and compensation is written to `saga_steps`, so the outcome
of each order can be audited (and shown in the demo).
"""
import logging
import os
import time
from uuid import uuid4

import httpx

logger = logging.getLogger("saga")

FLIGHTS_URL = os.getenv("FLIGHTS_URL", "http://flights-service:8000")
HOTELS_URL = os.getenv("HOTELS_URL", "http://hotels-service:8000")
CARS_URL = os.getenv("CARS_URL", "http://cars-service:8000")
SERVICE_TIMEOUT = float(os.getenv("SERVICE_TIMEOUT_SECONDS", "5"))

STEPS = ["PAYMENT", "FLIGHT", "HOTEL", "CAR"]
COMPENSATION_ATTEMPTS = 3


class StepError(Exception):
    pass


def call_service(method: str, url: str, payload: dict | None = None) -> dict:
    """HTTP call to another service. Retries once on network errors; reservations
    are idempotent per order_id, so a retry can never double-book."""
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


# ---------------------------------------------------------------------------
# Step actions and their compensations
# ---------------------------------------------------------------------------

def _execute_payment(pool, order: dict, fail: bool) -> None:
    """Billing lives in this service: authorize the full amount with a mock gateway."""
    status = "FAILED" if fail else "AUTHORIZED"
    with pool.connection() as conn:
        conn.execute(
            "insert into public.payments (order_id, amount, currency, status, provider_ref) "
            "values (%s, %s, %s, %s, %s) on conflict (order_id) do nothing",
            (order["id"], order["total_amount"], order["currency"], status, f"MOCKPAY-{uuid4().hex[:10]}"),
        )
    if fail:
        raise StepError("Payment declined (simulated)")


def _refund_payment(pool, order: dict) -> None:
    with pool.connection() as conn:
        conn.execute(
            "update public.payments set status = 'REFUNDED' "
            "where order_id = %s and status in ('AUTHORIZED', 'CAPTURED')",
            (order["id"],),
        )


def _execute_flight(pool, order: dict, fail: bool) -> None:
    call_service("POST", f"{FLIGHTS_URL}/reservations", {
        "order_id": str(order["id"]),
        "flight_id": str(order["flight_id"]),
        "seats": order["passengers"],
        "simulate_failure": fail,
    })


def _execute_hotel(pool, order: dict, fail: bool) -> None:
    call_service("POST", f"{HOTELS_URL}/reservations", {
        "order_id": str(order["id"]),
        "hotel_id": str(order["hotel_id"]),
        "rooms": (order["passengers"] + 1) // 2,
        "check_in": order["check_in"].isoformat(),
        "check_out": order["check_out"].isoformat(),
        "simulate_failure": fail,
    })


def _execute_car(pool, order: dict, fail: bool) -> None:
    call_service("POST", f"{CARS_URL}/reservations", {
        "order_id": str(order["id"]),
        "car_id": str(order["car_id"]),
        "pickup_date": order["check_in"].isoformat(),
        "return_date": order["check_out"].isoformat(),
        "simulate_failure": fail,
    })


EXECUTE = {
    "PAYMENT": _execute_payment,
    "FLIGHT": _execute_flight,
    "HOTEL": _execute_hotel,
    "CAR": _execute_car,
}

COMPENSATE = {
    "PAYMENT": _refund_payment,
    "FLIGHT": lambda pool, order: call_service("POST", f"{FLIGHTS_URL}/reservations/{order['id']}/cancel"),
    "HOTEL": lambda pool, order: call_service("POST", f"{HOTELS_URL}/reservations/{order['id']}/cancel"),
    "CAR": lambda pool, order: call_service("POST", f"{CARS_URL}/reservations/{order['id']}/cancel"),
}


# ---------------------------------------------------------------------------
# Bookkeeping
# ---------------------------------------------------------------------------

def _log_step(pool, order_id, step: str, action: str, status: str, error: str | None = None) -> None:
    with pool.connection() as conn:
        conn.execute(
            "insert into public.saga_steps (order_id, step, action, status, error) values (%s, %s, %s, %s, %s)",
            (order_id, step, action, status, error),
        )
    logger.info("order=%s %s %s %s %s", order_id, step, action, status, error or "")


def _set_status(pool, order_id, status: str, reason: str | None = None) -> None:
    with pool.connection() as conn:
        conn.execute(
            "update public.orders set status = %s, failure_reason = coalesce(%s, failure_reason) where id = %s",
            (status, reason, order_id),
        )


def _compensate(pool, order: dict, steps: list[str]) -> bool:
    """Undo steps in the given order. Each compensation is retried with backoff;
    returns False if any compensation still fails (needs manual attention)."""
    all_ok = True
    for step in steps:
        _log_step(pool, order["id"], step, "COMPENSATE", "STARTED")
        for attempt in range(1, COMPENSATION_ATTEMPTS + 1):
            try:
                COMPENSATE[step](pool, order)
            except Exception as exc:
                if attempt == COMPENSATION_ATTEMPTS:
                    _log_step(pool, order["id"], step, "COMPENSATE", "FAILED", str(exc))
                    all_ok = False
                else:
                    time.sleep(0.5 * 2 ** attempt)
                continue
            _log_step(pool, order["id"], step, "COMPENSATE", "SUCCEEDED")
            break
    return all_ok


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_saga(pool, order: dict, fail_at: str | None = None) -> str:
    """Run the booking saga for a PENDING order and return its final status."""
    completed: list[str] = []
    for step in STEPS:
        _log_step(pool, order["id"], step, "EXECUTE", "STARTED")
        try:
            EXECUTE[step](pool, order, fail_at == step)
        except Exception as exc:
            _log_step(pool, order["id"], step, "EXECUTE", "FAILED", str(exc))
            _set_status(pool, order["id"], "COMPENSATING", f"{step} failed: {exc}")
            compensated = _compensate(pool, order, list(reversed(completed)))
            final = "CANCELLED" if compensated else "FAILED"
            _set_status(pool, order["id"], final)
            return final
        _log_step(pool, order["id"], step, "EXECUTE", "SUCCEEDED")
        completed.append(step)

    with pool.connection() as conn:
        conn.execute(
            "update public.payments set status = 'CAPTURED' where order_id = %s and status = 'AUTHORIZED'",
            (order["id"],),
        )
    _set_status(pool, order["id"], "CONFIRMED")
    return "CONFIRMED"


def _undo_started_steps(pool, order: dict, reason: str) -> str:
    """Undo every step that started and was not yet compensated, then close the order
    (CANCELLED, or FAILED if an undo never succeeded). Returns the final status."""
    with pool.connection() as conn:
        rows = conn.execute(
            "select step, action, status from public.saga_steps where order_id = %s", (order["id"],)
        ).fetchall()
    started = {r["step"] for r in rows if r["action"] == "EXECUTE"}
    undone = {r["step"] for r in rows if r["action"] == "COMPENSATE" and r["status"] == "SUCCEEDED"}
    pending = [s for s in reversed(STEPS) if s in started and s not in undone]
    logger.warning("Settling order %s (%s), compensating %s", order["id"], reason, pending)
    _set_status(pool, order["id"], "COMPENSATING", reason)
    final = "CANCELLED" if _compensate(pool, order, pending) else "FAILED"
    _set_status(pool, order["id"], final)
    return final


def fail_order(pool, order_id, reason: str) -> str | None:
    """Settle an order whose Prefect SAGA run is known to have stopped (it never started, crashed
    or failed): undo whatever it started so nothing stays reserved. Returns the final status."""
    with pool.connection() as conn:
        order = conn.execute("select * from public.orders where id = %s", (order_id,)).fetchone()
    if order is None or order["status"] not in ("PENDING", "COMPENSATING"):
        return order["status"] if order else None
    return _undo_started_steps(pool, order, reason)


def recover_stuck_orders(pool) -> None:
    """On startup, finish orders left half-done by a crash (the "orphan bookings"
    problem): every step that started and was not yet compensated is undone."""
    with pool.connection() as conn:
        stuck = conn.execute(
            "select * from public.orders where status in ('PENDING', 'COMPENSATING') "
            "and updated_at < now() - interval '1 minute'"
        ).fetchall()
    for order in stuck:
        _undo_started_steps(pool, order, "Recovered after orchestrator restart")
