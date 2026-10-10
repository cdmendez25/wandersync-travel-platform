"""Scraper check against the mock provider. It fetches real pages, parses them with the pipeline's
own code and validates the cleaned rows. It does NOT touch the database.

Run inside the pipeline container:
    docker compose exec -T pipeline python - < scripts/test_scraper.py
"""
import re
import time
from collections import Counter
from datetime import date, timedelta

from bs4 import BeautifulSoup

from scraping import config, fetcher, parsers

ok = True


def check(name, cond, detail=""):
    global ok
    ok &= bool(cond)
    print(("PASS  " if cond else "FAIL  ") + name + (f"   ({detail})" if detail else ""), flush=True)


def get(path, params, tries=6):
    last = None
    for i in range(1, tries + 1):
        try:
            return fetcher.fetch_html(path, params), i
        except Exception as exc:  # the mock injects 503 / 429 / slow responses on purpose
            last = exc
            time.sleep(0.4)
    raise last


day = (date.today() + timedelta(days=2)).isoformat()
print(f"mock provider: {config.MOCK_PROVIDER_URL} | test date: {day}\n")

# ---------------------------------------------------------------- flights
html, tries = get("/flights", {"origin": "BOG", "destination": "CTG", "date": day})
raw = BeautifulSoup(html, "html.parser").select("tr.flight-row")
rows = parsers.parse_flights(html, config.PROVIDERS["flights"])
print(f"FLIGHTS  HTML {len(html)} bytes | {len(raw)} raw rows -> {len(rows)} clean | network attempts: {tries}")
print("  raw prices (mixed formats):", [r.select_one(".price").get_text(" ").strip() for r in raw][:4])
print("  raw cabins:", [r.select_one(".cabin").get_text(" ") for r in raw][:4])
print("  clean row:", {k: (v if isinstance(v, (int, str)) else str(v)) for k, v in rows[0].items()})
check("flights: there are results", len(rows) >= 3, f"{len(rows)}")
check("flights: duplicated rows removed", len(rows) == len({r["external_id"] for r in rows}) <= len(raw))
check("flights: price is a positive number with 2 decimals", all(r["price"] > 0 and r["price"].as_tuple().exponent == -2 for r in rows))
check("flights: valid flight number, IATA code and cabin",
      all(re.fullmatch(r"[A-Z0-9]{2}\d{1,4}", r["flight_number"]) and re.fullmatch(r"[A-Z]{3}", r["origin"])
          and r["cabin_class"] in parsers.CABINS for r in rows))
check("flights: arrival after departure, both with a time zone",
      all(r["arrival_at"] > r["departure_at"] and r["departure_at"].tzinfo for r in rows))
check("flights: route is BOG -> CTG", all((r["origin"], r["destination"]) == ("BOG", "CTG") for r in rows))

# ----------------------------------------------------------------- hotels
html, tries = get("/hotels", {"city": "CTG"})
raw = BeautifulSoup(html, "html.parser").select("tr.hotel-row")
rows_h = parsers.parse_hotels(html, config.PROVIDERS["hotels"])
print(f"\nHOTELS   {len(raw)} raw rows -> {len(rows_h)} clean | network attempts: {tries}")
print("  raw prices:", [r.select_one(".price").get_text(" ").strip() for r in raw][:4])
print("  raw ratings:", [r.select_one(".rating").get_text(" ").strip() for r in raw][:4])
print("  clean row:", {k: (v if isinstance(v, (int, str)) else str(v)) for k, v in rows_h[0].items()})
check("hotels: 24 unique rooms in Cartagena", len(rows_h) == 24, f"{len(rows_h)}")
check("hotels: city is normalized to the name the gateway filters on", {r["city"] for r in rows_h} == {"Cartagena"})
check("hotels: 1-5 stars, rating 0-10, price > 0",
      all((r["stars"] is None or 1 <= r["stars"] <= 5) and 0 <= r["rating"] <= 10 and r["price_per_night"] > 0 for r in rows_h))

# ------------------------------------------------------------------- cars
html, tries = get("/cars", {"city": "CTG"})
raw = BeautifulSoup(html, "html.parser").select("tr.car-row")
rows_c = parsers.parse_cars(html, config.PROVIDERS["cars"])
print(f"\nCARS     {len(raw)} raw rows -> {len(rows_c)} clean | network attempts: {tries}")
print("  clean row:", {k: (v if isinstance(v, (int, str)) else str(v)) for k, v in rows_c[0].items()})
check("cars: 6 unique vehicles in Cartagena", len(rows_c) == 6, f"{len(rows_c)}")
check("cars: valid category and transmission, price > 0",
      all(r["category"] in parsers.CAR_CATEGORIES and r["transmission"] in ("AUTOMATIC", "MANUAL") and r["price_per_day"] > 0
          for r in rows_c))

# --------------------------------------------- resilience to the injected faults
codes = Counter()
for _ in range(40):
    try:
        fetcher.fetch_html("/hotels", {"city": "BOG"})
        codes["200 OK"] += 1
    except Exception as exc:
        status = getattr(getattr(exc, "response", None), "status_code", None)
        codes[f"{type(exc).__name__}{' ' + str(status) if status else ''}"] += 1
print("\n40 requests in a row to the mock:", dict(codes))
check("the mock injects faults (503 / 429 / timeout) and also answers correctly",
      codes["200 OK"] > 0 and sum(v for k, v in codes.items() if k != "200 OK") > 0)

print("\nSCRAPER RESULT:", "ALL OK" if ok else "THERE ARE FAILURES")
