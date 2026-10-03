"""Deterministic fake inventory.

The *list* of flights/hotels/cars is stable (seeded by route, date or city),
so repeated scraping upserts the same rows. Prices and availability change
every PRICE_BUCKET_SECONDS, so each scrape actually updates data.
"""
import os
import time
from datetime import date, datetime, time as dtime, timedelta, timezone
from random import Random

PRICE_BUCKET_SECONDS = int(os.getenv("PRICE_BUCKET_SECONDS", "300"))
COT = timezone(timedelta(hours=-5))  # Colombia time
COUNTRY = "CO"

CITIES = {
    "BOG": "Bogotá",
    "MDE": "Medellín",
    "CTG": "Cartagena",
    "CLO": "Cali",
    "SMR": "Santa Marta",
    "ADZ": "San Andrés",
}

AIRLINES = [("Avianca", "AV"), ("LATAM", "LA"), ("Wingo", "P5"), ("JetSMART", "JA"), ("Clic", "VE")]
CABIN_FACTORS = {"economy": 1.0, "premium": 1.6, "business": 2.8, "first": 4.5}

HOTEL_PREFIXES = ["Casa", "Hotel", "Hostal", "Posada", "Gran Hotel", "Boutique"]
HOTEL_NAMES = ["del Mar", "Colonial", "La Candelaria", "El Prado", "Las Palmas", "San Pedro",
               "Aurora", "Los Andes", "Bahía", "Real", "Esmeralda", "Mirador"]
ROOM_FACTORS = {"Standard": 1.0, "Deluxe": 1.5, "Suite": 2.4}

CAR_COMPANIES = ["Andes Rent", "Caribe Cars", "Ruta Rent a Car", "Llanos Motors"]
CAR_FLEET = [
    ("Kia Picanto", "economy", 4, "manual"),
    ("Renault Logan", "economy", 5, "manual"),
    ("Mazda 3", "compact", 5, "automatic"),
    ("Chevrolet Onix", "compact", 5, "automatic"),
    ("Toyota Fortuner", "suv", 7, "automatic"),
    ("Renault Duster", "suv", 5, "manual"),
    ("BMW X3", "luxury", 5, "automatic"),
    ("Mercedes C200", "luxury", 5, "automatic"),
    ("Toyota Hiace", "van", 12, "manual"),
]


def _bucket() -> int:
    return int(time.time() // PRICE_BUCKET_SECONDS)


def flights_for(origin: str, destination: str, day: date) -> list[dict]:
    route_key = "".join(sorted((origin, destination)))
    duration = Random(f"duration:{route_key}").randint(55, 115)
    base_fare = Random(f"fare:{route_key}").randint(60, 220)
    weekend = 1.2 if day.weekday() >= 4 else 1.0
    day_rng = Random(f"flights:{origin}{destination}:{day.isoformat()}")
    bucket = _bucket()

    flights = []
    for i in range(day_rng.randint(3, 6)):
        airline, code = day_rng.choice(AIRLINES)
        cabin = day_rng.choices(list(CABIN_FACTORS), weights=[70, 15, 12, 3])[0]
        departure = datetime.combine(
            day, dtime(day_rng.randint(5, 21), day_rng.choice((0, 15, 30, 45))), tzinfo=COT
        )
        external_id = f"{origin}{destination}-{day:%Y%m%d}-{i + 1}"
        live = Random(f"{external_id}:{bucket}")
        flights.append({
            "external_id": external_id,
            "airline": airline,
            "code": code,
            "number": day_rng.randint(100, 9899),
            "origin": origin,
            "destination": destination,
            "departure_at": departure,
            "arrival_at": departure + timedelta(minutes=duration + day_rng.randint(-5, 10)),
            "cabin": cabin,
            "price": round(base_fare * CABIN_FACTORS[cabin] * weekend * live.uniform(0.85, 1.35), 2),
            "seats": live.randint(0, 40),
        })
    return sorted(flights, key=lambda f: f["departure_at"])


def hotels_for(city: str) -> list[dict]:
    rng = Random(f"hotels:{city}")
    bucket = _bucket()

    rooms = []
    for j, suffix in enumerate(rng.sample(HOTEL_NAMES, 8), start=1):
        name = f"{rng.choice(HOTEL_PREFIXES)} {suffix}"
        stars = rng.randint(2, 5)
        base = rng.randint(35, 80) * stars / 2
        rating = round(rng.uniform(6.5, 9.8), 1)
        address = f"Calle {rng.randint(1, 120)} # {rng.randint(1, 99)}-{rng.randint(1, 99)}"
        for room, factor in ROOM_FACTORS.items():
            external_id = f"{city}-H{j:02d}-{room.upper()}"
            live = Random(f"{external_id}:{bucket}")
            rooms.append({
                "external_id": external_id,
                "name": name,
                "city": CITIES[city],
                "country": COUNTRY,
                "address": address,
                "stars": stars,
                "room": room,
                "price": round(base * factor * live.uniform(0.9, 1.25), 2),
                "rating": rating,
                "available": live.randint(0, 15),
            })
    return rooms


def cars_for(city: str) -> list[dict]:
    rng = Random(f"cars:{city}")
    bucket = _bucket()

    cars = []
    for k, (model, category, seats, transmission) in enumerate(rng.sample(CAR_FLEET, 6), start=1):
        external_id = f"{city}-C{k:02d}"
        live = Random(f"{external_id}:{bucket}")
        base = {"economy": 28, "compact": 38, "suv": 65, "luxury": 120, "van": 90}[category]
        cars.append({
            "external_id": external_id,
            "company": rng.choice(CAR_COMPANIES),
            "model": model,
            "category": category,
            "city": CITIES[city],
            "seats": seats,
            "transmission": transmission,
            "price": round(base * live.uniform(0.85, 1.3), 2),
            "available": live.randint(0, 8),
        })
    return cars
