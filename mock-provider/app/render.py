"""HTML rendering with deliberately messy formatting.

Real travel sites mix price formats, casing, whitespace and repeat rows.
The pipeline's cleaning step has to normalize all of this.
"""
import random
from html import escape


def _page(title: str, body: str) -> str:
    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>{escape(title)}</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; }}
    table {{ border-collapse: collapse; }}
    td, th {{ border: 1px solid #ccc; padding: .3rem .6rem; }}
  </style>
</head>
<body>
  <h1>{escape(title)}</h1>
  {body}
</body>
</html>"""


def _messy(text: str) -> str:
    return random.choice([text, text.upper(), text.lower(), f"  {text}  "])


def _money(amount: float, unit: str = "") -> str:
    fmt = random.choice(["${:,.2f}", "USD {:.2f}", "{:,.2f} USD", "US$ {:,.2f}"])
    return fmt.format(amount) + unit


def _availability(count: int, noun: str) -> str:
    if count == 0:
        return "Sold out"
    if count <= 3:
        return f"Only {count} left!"
    return f"{count} {noun}"


def _with_duplicates(rows: list[str]) -> list[str]:
    if rows and random.random() < 0.3:
        rows.insert(random.randrange(len(rows)), random.choice(rows))
    return rows


def _table(headers: list[str], rows: list[str]) -> str:
    head = "".join(f"<th>{escape(h)}</th>" for h in headers)
    return f'<table id="results"><thead><tr>{head}</tr></thead><tbody>{"".join(rows)}</tbody></table>'


def index_page(cities: dict[str, str]) -> str:
    items = "".join(f"<li>{code}: {escape(name)}</li>" for code, name in cities.items())
    return _page("WanderSync Mock Provider", f"""
  <p>Simulated travel sources for scraping:</p>
  <ul>
    <li><code>/flights?origin=BOG&amp;destination=CTG&amp;date=YYYY-MM-DD</code> (mock Kayak)</li>
    <li><code>/hotels?city=CTG</code> (mock Booking)</li>
    <li><code>/cars?city=CTG</code> (mock Rentalcars)</li>
  </ul>
  <p>Cities:</p><ul>{items}</ul>""")


def flights_page(origin: str, destination: str, day, flights: list[dict]) -> str:
    rows = []
    for f in flights:
        code = random.choice([f"{f['code']} {f['number']}", f"{f['code'].lower()}{f['number']}",
                              f"{f['code']}-{f['number']}"])
        rows.append(
            f'<tr class="flight-row" data-id="{escape(f["external_id"])}">'
            f'<td class="airline">  {escape(f["airline"])} </td>'
            f'<td class="code">{escape(code)}</td>'
            f'<td class="route">{f["origin"]} → {f["destination"]}</td>'
            f'<td class="depart" data-ts="{f["departure_at"].isoformat()}">{f["departure_at"]:%H:%M}</td>'
            f'<td class="arrive" data-ts="{f["arrival_at"].isoformat()}">{f["arrival_at"]:%H:%M}</td>'
            f'<td class="cabin">{escape(_messy(f["cabin"]))}</td>'
            f'<td class="price">{_money(f["price"])}</td>'
            f'<td class="seats">{_availability(f["seats"], "seats left")}</td>'
            "</tr>"
        )
    return _page(
        f"Flights {origin} → {destination} on {day.isoformat()}",
        _table(["Airline", "Flight", "Route", "Departs", "Arrives", "Cabin", "Price", "Seats"],
               _with_duplicates(rows)),
    )


def hotels_page(city_name: str, hotels: list[dict]) -> str:
    rows = []
    for h in hotels:
        stars = "★" * h["stars"] + "☆" * (5 - h["stars"])
        rating = random.choice([f"{h['rating']}/10", f"Superb {h['rating']}", f"Rated {h['rating']}"])
        rows.append(
            f'<tr class="hotel-row" data-id="{escape(h["external_id"])}" data-country="{h["country"].lower()}">'
            f'<td class="name">{escape(h["name"])}</td>'
            f'<td class="city">{escape(_messy(h["city"]))}</td>'
            f'<td class="address">{escape(h["address"])}</td>'
            f'<td class="stars">{stars}</td>'
            f'<td class="room">{escape(h["room"])} room</td>'
            f'<td class="price">{_money(h["price"], " / night")}</td>'
            f'<td class="rating">{rating}</td>'
            f'<td class="availability">{_availability(h["available"], "rooms left")}</td>'
            "</tr>"
        )
    return _page(
        f"Hotels in {city_name}",
        _table(["Hotel", "City", "Address", "Stars", "Room", "Price", "Rating", "Availability"],
               _with_duplicates(rows)),
    )


def cars_page(city_name: str, cars: list[dict]) -> str:
    rows = []
    for c in cars:
        rows.append(
            f'<tr class="car-row" data-id="{escape(c["external_id"])}">'
            f'<td class="company">{escape(c["company"])}</td>'
            f'<td class="model">{escape(c["model"])}</td>'
            f'<td class="category">{escape(_messy(c["category"]))}</td>'
            f'<td class="city">{escape(_messy(c["city"]))}</td>'
            f'<td class="seats">{c["seats"]} seats</td>'
            f'<td class="transmission">{escape(_messy(c["transmission"]))}</td>'
            f'<td class="price">{_money(c["price"], "/day")}</td>'
            f'<td class="availability">{_availability(c["available"], "available")}</td>'
            "</tr>"
        )
    return _page(
        f"Car rentals in {city_name}",
        _table(["Company", "Model", "Category", "City", "Seats", "Transmission", "Price", "Availability"],
               _with_duplicates(rows)),
    )
