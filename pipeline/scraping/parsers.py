"""Extract + clean: HTML results pages -> rows ready for the catalog tables.

Each parser skips rows it cannot validate and removes duplicated rows
(same external id), so only clean data reaches Supabase.
"""
import re
from datetime import datetime
from decimal import Decimal

from bs4 import BeautifulSoup

_AMOUNT = re.compile(r"\d[\d,]*(?:\.\d+)?")
_INTEGER = re.compile(r"\d+")
_IATA = re.compile(r"\b[A-Z]{3}\b")
_FLIGHT_NUMBER = re.compile(r"[A-Z0-9]{2}\d{1,4}")

CABINS = {"ECONOMY", "PREMIUM", "BUSINESS", "FIRST"}
CAR_CATEGORIES = {"ECONOMY", "COMPACT", "SUV", "LUXURY", "VAN"}


def _text(row, css_class: str) -> str:
    cell = row.select_one(f".{css_class}")
    return " ".join(cell.get_text(" ").split()) if cell else ""


def _timestamp(row, css_class: str) -> datetime:
    return datetime.fromisoformat(row.select_one(f".{css_class}")["data-ts"])


def parse_money(text: str) -> Decimal:
    """'$1,234.50', 'USD 98.40', '120.00 USD / night' -> Decimal('1234.50')"""
    match = _AMOUNT.search(text)
    if not match:
        raise ValueError(f"no amount in {text!r}")
    return Decimal(match.group().replace(",", "")).quantize(Decimal("0.01"))


def parse_count(text: str) -> int:
    """'12 seats left', 'Only 2 left!', 'Sold out' -> 12, 2, 0"""
    if "sold out" in text.lower():
        return 0
    match = _INTEGER.search(text)
    if not match:
        raise ValueError(f"no count in {text!r}")
    return int(match.group())


def parse_flights(html: str, provider: str) -> list[dict]:
    rows = {}
    for tr in BeautifulSoup(html, "html.parser").select("tr.flight-row"):
        try:
            external_id = tr["data-id"].strip()
            origin, destination = _IATA.findall(_text(tr, "route").upper())[:2]
            flight_number = re.sub(r"[^A-Za-z0-9]", "", _text(tr, "code")).upper()
            if not _FLIGHT_NUMBER.fullmatch(flight_number):
                raise ValueError(f"bad flight number {flight_number!r}")
            departure_at, arrival_at = _timestamp(tr, "depart"), _timestamp(tr, "arrive")
            if arrival_at <= departure_at:
                raise ValueError("arrival before departure")
            cabin = _text(tr, "cabin").upper().split()[0]
            if cabin not in CABINS:
                raise ValueError(f"unknown cabin {cabin!r}")
            rows[external_id] = {
                "provider": provider,
                "external_id": external_id,
                "airline": _text(tr, "airline"),
                "flight_number": flight_number,
                "origin": origin,
                "destination": destination,
                "departure_at": departure_at,
                "arrival_at": arrival_at,
                "cabin_class": cabin,
                "price": parse_money(_text(tr, "price")),
                "currency": "USD",
                "seats_available": parse_count(_text(tr, "seats")),
            }
        except (KeyError, IndexError, TypeError, ValueError):
            continue
    return list(rows.values())


def parse_hotels(html: str, provider: str) -> list[dict]:
    rows = {}
    for tr in BeautifulSoup(html, "html.parser").select("tr.hotel-row"):
        try:
            external_id = tr["data-id"].strip()
            country = tr["data-country"].strip().upper()
            if len(country) != 2:
                raise ValueError(f"bad country {country!r}")
            rating = Decimal(_AMOUNT.search(_text(tr, "rating")).group()).quantize(Decimal("0.1"))
            if not 0 <= rating <= 10:
                raise ValueError(f"bad rating {rating}")
            stars = _text(tr, "stars").count("★")
            rows[external_id] = {
                "provider": provider,
                "external_id": external_id,
                "name": _text(tr, "name"),
                "city": _text(tr, "city").title(),
                "country": country,
                "address": _text(tr, "address") or None,
                "stars": stars if 1 <= stars <= 5 else None,
                "room_type": _text(tr, "room").upper().split()[0],
                "price_per_night": parse_money(_text(tr, "price")),
                "currency": "USD",
                "rooms_available": parse_count(_text(tr, "availability")),
                "rating": rating,
            }
        except (KeyError, IndexError, TypeError, AttributeError, ValueError):
            continue
    return list(rows.values())


def parse_cars(html: str, provider: str) -> list[dict]:
    rows = {}
    for tr in BeautifulSoup(html, "html.parser").select("tr.car-row"):
        try:
            external_id = tr["data-id"].strip()
            category = _text(tr, "category").upper()
            if category not in CAR_CATEGORIES:
                raise ValueError(f"unknown category {category!r}")
            transmission = "AUTOMATIC" if _text(tr, "transmission").upper().startswith("AUTO") else "MANUAL"
            rows[external_id] = {
                "provider": provider,
                "external_id": external_id,
                "company": _text(tr, "company"),
                "model": _text(tr, "model"),
                "category": category,
                "city": _text(tr, "city").title(),
                "seats": parse_count(_text(tr, "seats")),
                "transmission": transmission,
                "price_per_day": parse_money(_text(tr, "price")),
                "currency": "USD",
                "units_available": parse_count(_text(tr, "availability")),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return list(rows.values())
