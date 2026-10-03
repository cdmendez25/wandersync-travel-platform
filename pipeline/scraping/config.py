import os
from datetime import date, timedelta

MOCK_PROVIDER_URL = os.getenv("MOCK_PROVIDER_URL", "http://mock-provider:8000")
DASK_SCHEDULER_ADDRESS = os.getenv("DASK_SCHEDULER_ADDRESS", "tcp://dask-scheduler:8786")
DATABASE_URL = os.getenv("DATABASE_URL", "")

HTTP_TIMEOUT_SECONDS = float(os.getenv("HTTP_TIMEOUT_SECONDS", "5"))
DAYS_AHEAD = int(os.getenv("SCRAPE_DAYS_AHEAD", "14"))
INTERVAL_SECONDS = int(os.getenv("SCRAPE_INTERVAL_SECONDS", "300"))
RUN_ON_START = os.getenv("SCRAPE_RUN_ON_START", "true").lower() == "true"

# Value stored in the `provider` column of each catalog table
PROVIDERS = {"flights": "mock-kayak", "hotels": "mock-booking", "cars": "mock-rentalcars"}

CITIES = ["BOG", "MDE", "CTG", "CLO", "SMR", "ADZ"]
ROUTES = [
    ("BOG", "CTG"), ("CTG", "BOG"),
    ("BOG", "MDE"), ("MDE", "BOG"),
    ("BOG", "ADZ"), ("MDE", "CTG"),
    ("CLO", "CTG"), ("BOG", "SMR"),
]


def travel_dates(days_ahead: int) -> list[date]:
    start = date.today() + timedelta(days=1)
    return [start + timedelta(days=i) for i in range(days_ahead)]
