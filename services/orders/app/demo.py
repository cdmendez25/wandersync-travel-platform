"""SAGA demo: one successful booking and one with a simulated failure.

    docker compose exec orders-service python -m app.demo          # failure at CAR
    docker compose exec orders-service python -m app.demo HOTEL    # failure at HOTEL
"""
import sys
import uuid
from datetime import timedelta

import httpx

from common.db import create_pool

ORDERS_URL = "http://localhost:8000"
CITY_BY_AIRPORT = {"BOG": "Bogotá", "MDE": "Medellín", "CTG": "Cartagena",
                   "CLO": "Cali", "SMR": "Santa Marta", "ADZ": "San Andrés"}


def pick_package(conn) -> dict:
    flight = conn.execute(
        "select id, origin, destination, departure_at::date as day from public.flights "
        "where seats_available >= 1 and departure_at > now() + interval '1 day' order by random() limit 1"
    ).fetchone()
    city = CITY_BY_AIRPORT[flight["destination"]]
    hotel = conn.execute(
        "select id from public.hotels where city = %s and rooms_available >= 1 order by random() limit 1", (city,)
    ).fetchone()
    car = conn.execute(
        "select id from public.cars where city = %s and units_available >= 1 order by random() limit 1", (city,)
    ).fetchone()
    return {"flight": flight, "hotel_id": hotel["id"], "car_id": car["id"], "city": city}


def book(package: dict, fail_at: str | None) -> dict:
    day = package["flight"]["day"]
    payload = {
        "user_id": str(uuid.uuid4()),
        "idempotency_key": f"demo-{uuid.uuid4()}",
        "flight_id": str(package["flight"]["id"]),
        "hotel_id": str(package["hotel_id"]),
        "car_id": str(package["car_id"]),
        "passengers": 2,
        "check_in": day.isoformat(),
        "check_out": (day + timedelta(days=3)).isoformat(),
        "simulate_failure": fail_at,
    }
    response = httpx.post(f"{ORDERS_URL}/orders", json=payload, timeout=60)
    response.raise_for_status()
    return response.json()


def print_order(title: str, order: dict) -> None:
    print(f"\n=== {title} ===")
    print(f"Order {order['id']}  ->  {order['status']}   total {order['currency']} {order['total_amount']}")
    if order.get("failure_reason"):
        print(f"Reason: {order['failure_reason']}")
    if order.get("payment"):
        print(f"Payment: {order['payment']['status']}")
    print(f"{'#':>3}  {'STEP':8} {'ACTION':11} {'STATUS':10} ERROR")
    for i, s in enumerate(order["saga_steps"], start=1):
        print(f"{i:>3}  {s['step']:8} {s['action']:11} {s['status']:10} {s['error'] or ''}")


def main() -> None:
    fail_at = (sys.argv[1] if len(sys.argv) > 1 else "CAR").upper()
    pool = create_pool()
    with pool.connection() as conn:
        package = pick_package(conn)
        happy = book(package, None)
        print_order("Happy path", happy)

        failed = book(pick_package(conn), fail_at)
        print_order(f"Simulated failure at {fail_at}", failed)

        print("\n=== Reservation status for the failed order ===")
        for table in ("flight_reservations", "hotel_reservations", "car_reservations"):
            row = conn.execute(f"select status from public.{table} where order_id = %s", (failed["id"],)).fetchone()
            print(f"{table:20} {row['status'] if row else '(never created)'}")
    pool.close()


if __name__ == "__main__":
    main()
