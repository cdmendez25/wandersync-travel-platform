"""Cars service: owns `car_reservations` and the unit stock in `cars`.

reserve -> takes one car and creates the reservation (SAGA step)
cancel  -> releases the car (SAGA compensation)
Both are idempotent per order_id, so the orchestrator can safely retry them.
"""
from contextlib import asynccontextmanager
from datetime import date
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, model_validator

from common.db import create_pool

pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = create_pool()
    yield
    pool.close()


app = FastAPI(title="Cars Service", lifespan=lifespan)


class ReserveRequest(BaseModel):
    order_id: UUID
    car_id: UUID
    pickup_date: date
    return_date: date
    simulate_failure: bool = False

    @model_validator(mode="after")
    def check_dates(self):
        if self.return_date <= self.pickup_date:
            raise ValueError("return_date must be after pickup_date")
        return self


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/cars/{car_id}")
def get_car(car_id: UUID):
    with pool.connection() as conn:
        car = conn.execute(
            "select id, company, model, category, city, seats, transmission, price_per_day, "
            "currency, units_available from public.cars where id = %s",
            (car_id,),
        ).fetchone()
    if not car:
        raise HTTPException(status_code=404, detail="Car not found")
    return car


@app.post("/reservations", status_code=201)
def reserve(req: ReserveRequest):
    if req.simulate_failure:
        raise HTTPException(status_code=503, detail="Simulated failure in cars service")

    with pool.connection() as conn, conn.transaction():
        existing = conn.execute(
            "select * from public.car_reservations where order_id = %s", (req.order_id,)
        ).fetchone()
        if existing:
            if existing["status"] == "CONFIRMED":
                return existing
            raise HTTPException(status_code=409, detail="Reservation for this order was already cancelled")

        car = conn.execute(
            "update public.cars set units_available = units_available - 1 "
            "where id = %s and units_available >= 1 returning price_per_day",
            (req.car_id,),
        ).fetchone()
        if not car:
            raise HTTPException(status_code=409, detail="Car not found or no units available")

        return conn.execute(
            "insert into public.car_reservations (order_id, car_id, pickup_date, return_date, unit_price) "
            "values (%s, %s, %s, %s, %s) returning *",
            (req.order_id, req.car_id, req.pickup_date, req.return_date, car["price_per_day"]),
        ).fetchone()


@app.post("/reservations/{order_id}/cancel")
def cancel(order_id: UUID):
    with pool.connection() as conn, conn.transaction():
        reservation = conn.execute(
            "update public.car_reservations set status = 'CANCELLED', cancelled_at = now() "
            "where order_id = %s and status = 'CONFIRMED' returning car_id",
            (order_id,),
        ).fetchone()
        if not reservation:
            return {"order_id": order_id, "status": "NOTHING_TO_CANCEL"}
        conn.execute(
            "update public.cars set units_available = units_available + 1 where id = %s",
            (reservation["car_id"],),
        )
    return {"order_id": order_id, "status": "CANCELLED"}
