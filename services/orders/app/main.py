"""Orders / Billing service: creates orders and triggers the booking SAGA Flow."""
import logging
import threading
from contextlib import asynccontextmanager
from datetime import date
from decimal import Decimal
from typing import Literal
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from app import prefect_flow, saga
from common.db import create_pool

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("orders")

pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = create_pool()
    threading.Thread(target=saga.recover_stuck_orders, args=(pool,), daemon=True).start()
    yield
    pool.close()


app = FastAPI(title="Orders Service", lifespan=lifespan)


class CreateOrderRequest(BaseModel):
    user_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=100)
    flight_id: UUID
    hotel_id: UUID
    car_id: UUID
    passengers: int = Field(default=1, ge=1, le=9)
    check_in: date
    check_out: date
    # Demo only: force this step to fail to show the compensations
    simulate_failure: Literal["PAYMENT", "FLIGHT", "HOTEL", "CAR"] | None = None

    @model_validator(mode="after")
    def check_dates(self):
        if self.check_out <= self.check_in:
            raise ValueError("check_out must be after check_in")
        return self


def _order_view(order_id) -> dict:
    with pool.connection() as conn:
        order = conn.execute("select * from public.orders where id = %s", (order_id,)).fetchone()
        if not order:
            raise HTTPException(status_code=404, detail="Order not found")
        order["payment"] = conn.execute(
            "select status, amount, currency, provider_ref from public.payments where order_id = %s", (order_id,)
        ).fetchone()
        order["saga_steps"] = conn.execute(
            "select step, action, status, error, created_at from public.saga_steps where order_id = %s order by id",
            (order_id,),
        ).fetchall()
    return order


def _quote(req: CreateOrderRequest) -> Decimal:
    """Ask each service for the current price (each service owns its catalog table)."""
    try:
        flight = saga.call_service("GET", f"{saga.FLIGHTS_URL}/flights/{req.flight_id}")
        hotel = saga.call_service("GET", f"{saga.HOTELS_URL}/hotels/{req.hotel_id}")
        car = saga.call_service("GET", f"{saga.CARS_URL}/cars/{req.car_id}")
    except saga.StepError as exc:
        raise HTTPException(status_code=422, detail=f"Could not quote package: {exc}") from exc
    nights = (req.check_out - req.check_in).days
    total = (
        Decimal(str(flight["price"])) * req.passengers
        + Decimal(str(hotel["price_per_night"])) * nights * ((req.passengers + 1) // 2)
        + Decimal(str(car["price_per_day"])) * nights
    )
    return total.quantize(Decimal("0.01"))


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/orders", status_code=201)
def create_order(req: CreateOrderRequest):
    with pool.connection() as conn:
        existing = conn.execute(
            "select id from public.orders where idempotency_key = %s", (req.idempotency_key,)
        ).fetchone()
    if existing:
        return _order_view(existing["id"])  # same checkout submitted twice

    total = _quote(req)
    with pool.connection() as conn:
        order = conn.execute(
            "insert into public.orders (user_id, idempotency_key, flight_id, hotel_id, car_id, passengers, "
            "check_in, check_out, total_amount) values (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "on conflict (idempotency_key) do nothing returning *",
            (req.user_id, req.idempotency_key, req.flight_id, req.hotel_id, req.car_id, req.passengers,
             req.check_in, req.check_out, total),
        ).fetchone()
    if not order:  # a concurrent request with the same key won the race
        with pool.connection() as conn:
            existing = conn.execute(
                "select id from public.orders where idempotency_key = %s", (req.idempotency_key,)
            ).fetchone()
        return _order_view(existing["id"])

    try:
        prefect_flow.run_booking_saga(str(order["id"]), req.simulate_failure)
    except prefect_flow.SagaFlowError as exc:
        if not exc.stopped:
            # The run may still finish: report the order as it is (PENDING) instead of an error
            # that a late completion would contradict.
            logger.warning("Order %s: %s; leaving it to the SAGA run", order["id"], exc)
            return _order_view(order["id"])
        saga.fail_order(pool, order["id"], f"Booking could not be completed: {exc}")
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return _order_view(order["id"])


@app.get("/orders/{order_id}")
def get_order(order_id: UUID):
    return _order_view(order_id)


@app.get("/orders")
def list_orders(user_id: UUID, limit: int = 20):
    with pool.connection() as conn:
        return conn.execute(
            "select id, status, total_amount, currency, check_in, check_out, failure_reason, created_at "
            "from public.orders where user_id = %s order by created_at desc limit %s",
            (user_id, min(limit, 100)),
        ).fetchall()
