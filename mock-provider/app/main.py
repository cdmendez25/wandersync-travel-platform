"""Mock travel provider that simulates Kayak / Booking / Rentalcars result pages.

It serves server-rendered HTML (not JSON), so the pipeline really has to
scrape it, and it injects network problems so Prefect retries get exercised:
  * FAIL_RATE  -> HTTP 503 / 429 responses
  * SLOW_RATE  -> responses slower than the scraper's timeout
"""
import asyncio
import os
import random
from datetime import date, timedelta

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, PlainTextResponse

from app import inventory, render

FAIL_RATE = float(os.getenv("FAIL_RATE", "0.15"))
SLOW_RATE = float(os.getenv("SLOW_RATE", "0.05"))
SLOW_SECONDS = float(os.getenv("SLOW_SECONDS", "8"))
MIN_LATENCY = float(os.getenv("MIN_LATENCY", "0.1"))
MAX_LATENCY = float(os.getenv("MAX_LATENCY", "0.6"))

UNTOUCHED_PATHS = {"/", "/health"}

app = FastAPI(title="WanderSync Mock Provider", docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def inject_network_chaos(request: Request, call_next):
    if request.url.path in UNTOUCHED_PATHS:
        return await call_next(request)

    roll = random.random()
    if roll < FAIL_RATE:
        if random.random() < 0.5:
            return PlainTextResponse("Service temporarily unavailable", status_code=503)
        return PlainTextResponse("Too many requests", status_code=429, headers={"Retry-After": "2"})
    if roll < FAIL_RATE + SLOW_RATE:
        await asyncio.sleep(SLOW_SECONDS)
    else:
        await asyncio.sleep(random.uniform(MIN_LATENCY, MAX_LATENCY))
    return await call_next(request)


def _city(code: str) -> str:
    code = code.upper()
    if code not in inventory.CITIES:
        raise HTTPException(status_code=404, detail=f"Unknown city code {code}")
    return code


def _check_date(day: date) -> None:
    today = date.today()
    if not today <= day <= today + timedelta(days=90):
        raise HTTPException(status_code=400, detail="date must be within the next 90 days")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def index():
    return render.index_page(inventory.CITIES)


@app.get("/flights", response_class=HTMLResponse)
def flights(
    origin: str = Query(..., min_length=3, max_length=3),
    destination: str = Query(..., min_length=3, max_length=3),
    day: date = Query(..., alias="date"),
):
    origin, destination = _city(origin), _city(destination)
    if origin == destination:
        raise HTTPException(status_code=400, detail="origin and destination must differ")
    _check_date(day)
    return render.flights_page(origin, destination, day, inventory.flights_for(origin, destination, day))


@app.get("/hotels", response_class=HTMLResponse)
def hotels(city: str = Query(..., min_length=3, max_length=3)):
    city = _city(city)
    return render.hotels_page(inventory.CITIES[city], inventory.hotels_for(city))


@app.get("/cars", response_class=HTMLResponse)
def cars(city: str = Query(..., min_length=3, max_length=3)):
    city = _city(city)
    return render.cars_page(inventory.CITIES[city], inventory.cars_for(city))
