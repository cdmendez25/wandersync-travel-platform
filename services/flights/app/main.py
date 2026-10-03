"""Flights service: owns `flight_reservations` and the seat stock in `flights`.

reserve -> takes seats and creates the reservation (SAGA step)
cancel  -> releases the seats (SAGA compensation)
Both are idempotent per order_id, so the orchestrator can safely retry them.
"""
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from common.db import create_pool

pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = create_pool()
    yield
    pool.close()


app = FastAPI(title="Flights Service", lifespan=lifespan)


class ReserveRequest(BaseModel):
    order_id: UUID
    flight_id: UUID
    seats: int = Field(ge=1, le=9)
    simulate_failure: bool = False


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/flights/{flight_id}")
def get_flight(flight_id: UUID):
    with pool.connection() as conn:
        flight = conn.execute(
            "select id, airline, flight_number, origin, destination, departure_at, arrival_at, "
            "cabin_class, price, currency, seats_available from public.flights where id = %s",
            (flight_id,),
        ).fetchone()
    if not flight:
        raise HTTPException(status_code=404, detail="Flight not found")
    return flight


@app.post("/reservations", status_code=201)
def reserve(req: ReserveRequest):
    if req.simulate_failure:
        raise HTTPException(status_code=503, detail="Simulated failure in flights service")

    with pool.connection() as conn, conn.transaction():
        existing = conn.execute(
            "select * from public.flight_reservations where order_id = %s", (req.order_id,)
        ).fetchone()
        if existing:
            if existing["status"] == "CONFIRMED":
                return existing  # retry of a step that already succeeded
            raise HTTPException(status_code=409, detail="Reservation for this order was already cancelled")

        flight = conn.execute(
            "update public.flights set seats_available = seats_available - %s "
            "where id = %s and seats_available >= %s returning price",
            (req.seats, req.flight_id, req.seats),
        ).fetchone()
        if not flight:
            raise HTTPException(status_code=409, detail="Flight not found or not enough seats")

        return conn.execute(
            "insert into public.flight_reservations (order_id, flight_id, seats, unit_price) "
            "values (%s, %s, %s, %s) returning *",
            (req.order_id, req.flight_id, req.seats, flight["price"]),
        ).fetchone()


@app.post("/reservations/{order_id}/cancel")
def cancel(order_id: UUID):
    with pool.connection() as conn, conn.transaction():
        reservation = conn.execute(
            "update public.flight_reservations set status = 'CANCELLED', cancelled_at = now() "
            "where order_id = %s and status = 'CONFIRMED' returning flight_id, seats",
            (order_id,),
        ).fetchone()
        if not reservation:
            return {"order_id": order_id, "status": "NOTHING_TO_CANCEL"}
        conn.execute(
            "update public.flights set seats_available = seats_available + %s where id = %s",
            (reservation["seats"], reservation["flight_id"]),
        )
    return {"order_id": order_id, "status": "CANCELLED"}
