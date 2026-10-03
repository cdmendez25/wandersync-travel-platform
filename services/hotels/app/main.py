"""Hotels service: owns `hotel_reservations` and the room stock in `hotels`.

reserve -> takes rooms and creates the reservation (SAGA step)
cancel  -> releases the rooms (SAGA compensation)
Both are idempotent per order_id, so the orchestrator can safely retry them.
"""
from contextlib import asynccontextmanager
from datetime import date
from uuid import UUID

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, model_validator

from common.db import create_pool

pool = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global pool
    pool = create_pool()
    yield
    pool.close()


app = FastAPI(title="Hotels Service", lifespan=lifespan)


class ReserveRequest(BaseModel):
    order_id: UUID
    hotel_id: UUID
    rooms: int = Field(default=1, ge=1, le=5)
    check_in: date
    check_out: date
    simulate_failure: bool = False

    @model_validator(mode="after")
    def check_dates(self):
        if self.check_out <= self.check_in:
            raise ValueError("check_out must be after check_in")
        return self


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/hotels/{hotel_id}")
def get_hotel(hotel_id: UUID):
    with pool.connection() as conn:
        hotel = conn.execute(
            "select id, name, city, country, stars, room_type, price_per_night, currency, "
            "rooms_available, rating from public.hotels where id = %s",
            (hotel_id,),
        ).fetchone()
    if not hotel:
        raise HTTPException(status_code=404, detail="Hotel not found")
    return hotel


@app.post("/reservations", status_code=201)
def reserve(req: ReserveRequest):
    if req.simulate_failure:
        raise HTTPException(status_code=503, detail="Simulated failure in hotels service")

    with pool.connection() as conn, conn.transaction():
        existing = conn.execute(
            "select * from public.hotel_reservations where order_id = %s", (req.order_id,)
        ).fetchone()
        if existing:
            if existing["status"] == "CONFIRMED":
                return existing
            raise HTTPException(status_code=409, detail="Reservation for this order was already cancelled")

        hotel = conn.execute(
            "update public.hotels set rooms_available = rooms_available - %s "
            "where id = %s and rooms_available >= %s returning price_per_night",
            (req.rooms, req.hotel_id, req.rooms),
        ).fetchone()
        if not hotel:
            raise HTTPException(status_code=409, detail="Hotel not found or no rooms available")

        return conn.execute(
            "insert into public.hotel_reservations (order_id, hotel_id, rooms, check_in, check_out, unit_price) "
            "values (%s, %s, %s, %s, %s, %s) returning *",
            (req.order_id, req.hotel_id, req.rooms, req.check_in, req.check_out, hotel["price_per_night"]),
        ).fetchone()


@app.post("/reservations/{order_id}/cancel")
def cancel(order_id: UUID):
    with pool.connection() as conn, conn.transaction():
        reservation = conn.execute(
            "update public.hotel_reservations set status = 'CANCELLED', cancelled_at = now() "
            "where order_id = %s and status = 'CONFIRMED' returning hotel_id, rooms",
            (order_id,),
        ).fetchone()
        if not reservation:
            return {"order_id": order_id, "status": "NOTHING_TO_CANCEL"}
        conn.execute(
            "update public.hotels set rooms_available = rooms_available + %s where id = %s",
            (reservation["rooms"], reservation["hotel_id"]),
        )
    return {"order_id": order_id, "status": "CANCELLED"}
