import { useMemo, useState } from "react";
import { useLazyQuery, useMutation } from "@apollo/client";
import { useNavigate } from "react-router-dom";
import { BOOK_PACKAGE, SEARCH_PACKAGES } from "../graphql.js";
import { AIRPORTS, addDays, errorMessage, time, today, usd } from "../format.js";

const FAILURE_OPTIONS = [
  { value: "", label: "No failure (normal booking)" },
  { value: "PAYMENT", label: "Fail at payment" },
  { value: "FLIGHT", label: "Fail at flight" },
  { value: "HOTEL", label: "Fail at hotel" },
  { value: "CAR", label: "Fail at car" },
];

const stars = (n) => (n ? "★".repeat(n) : "");

export default function Search() {
  const navigate = useNavigate();
  const [form, setForm] = useState({ origin: "BOG", destination: "CTG", date: addDays(today(), 2), passengers: 1 });
  const [nights, setNights] = useState(3);
  const [picked, setPicked] = useState({ flight: null, hotel: null, car: null });
  const [simulate, setSimulate] = useState("");
  // One key per checkout attempt: retries of the same attempt are booked only once
  const [idempotencyKey, setIdempotencyKey] = useState(() => crypto.randomUUID());

  const [search, { data, loading, error }] = useLazyQuery(SEARCH_PACKAGES, { fetchPolicy: "network-only" });
  const [book, booking] = useMutation(BOOK_PACKAGE);
  const result = data?.searchPackages;

  const update = (field) => (e) => setForm({ ...form, [field]: field === "passengers" ? Number(e.target.value) : e.target.value });

  const pick = (kind, item) => {
    setPicked({ ...picked, [kind]: item });
    setIdempotencyKey(crypto.randomUUID());
  };

  const submitSearch = (e) => {
    e.preventDefault();
    setPicked({ flight: null, hotel: null, car: null });
    search({ variables: form });
  };

  const rooms = Math.ceil(form.passengers / 2);
  const total = useMemo(() => {
    const { flight, hotel, car } = picked;
    return (
      (flight ? Number(flight.price) * form.passengers : 0) +
      (hotel ? Number(hotel.pricePerNight) * nights * rooms : 0) +
      (car ? Number(car.pricePerDay) * nights : 0)
    );
  }, [picked, nights, form.passengers, rooms]);

  const ready = picked.flight && picked.hotel && picked.car;

  const submitBooking = async () => {
    try {
      const res = await book({
        variables: {
          input: {
            flightId: picked.flight.id,
            hotelId: picked.hotel.id,
            carId: picked.car.id,
            checkIn: form.date,
            checkOut: addDays(form.date, nights),
            passengers: form.passengers,
            idempotencyKey,
            simulateFailure: simulate || null,
          },
        },
      });
      navigate(`/orders/${res.data.bookPackage.id}`);
    } catch {
      /* shown in the summary */
    }
  };

  return (
    <div className="search-page">
      <form className="card search-bar" onSubmit={submitSearch}>
        <label>
          From
          <select id="origin" value={form.origin} onChange={update("origin")}>
            {Object.entries(AIRPORTS).map(([code, city]) => <option key={code} value={code}>{city} ({code})</option>)}
          </select>
        </label>
        <label>
          To
          <select id="destination" value={form.destination} onChange={update("destination")}>
            {Object.entries(AIRPORTS).map(([code, city]) => <option key={code} value={code}>{city} ({code})</option>)}
          </select>
        </label>
        <label>
          Departure
          <input id="date" type="date" min={addDays(today(), 1)} value={form.date} onChange={update("date")} required />
        </label>
        <label>
          Travelers
          <input id="passengers" type="number" min={1} max={9} value={form.passengers} onChange={update("passengers")} />
        </label>
        <button className="primary" disabled={loading || form.origin === form.destination}>
          {loading ? "Searching…" : "Search"}
        </button>
      </form>

      {form.origin === form.destination && <p className="hint">Choose a different destination.</p>}
      {error && <p className="alert">{errorMessage(error)}</p>}
      {!result && !loading && !error && (
        <p className="hint">Pick a route and a date. Prices come from the scraped catalog, refreshed every 5 minutes.</p>
      )}

      {result && (
        <div className="results-layout">
          <div className="results">
            <section>
              <h2>1. Flight <span className="count">{result.flights.length} options</span></h2>
              {result.flights.length === 0 && <p className="hint">No flights with free seats on this date. Try another day.</p>}
              <div className="options">
                {result.flights.map((f) => (
                  <button key={f.id} type="button" className={`option ${picked.flight?.id === f.id ? "selected" : ""}`} onClick={() => pick("flight", f)}>
                    <span className="opt-main">{time(f.departureAt)} → {time(f.arrivalAt)}</span>
                    <span className="opt-sub">{f.airline} {f.flightNumber} · {f.cabinClass.toLowerCase()} · {f.seatsAvailable} seats</span>
                    <span className="opt-price">{usd(f.price)}<small>/person</small></span>
                  </button>
                ))}
              </div>
            </section>
            <section>
              <h2>2. Hotel in {result.destinationCity} <span className="count">{result.hotels.length} options</span></h2>
              <div className="options">
                {result.hotels.map((h) => (
                  <button key={h.id} type="button" className={`option ${picked.hotel?.id === h.id ? "selected" : ""}`} onClick={() => pick("hotel", h)}>
                    <span className="opt-main">{h.name} <span className="stars">{stars(h.stars)}</span></span>
                    <span className="opt-sub">{h.roomType.toLowerCase()} room · rated {h.rating} · {h.roomsAvailable} left</span>
                    <span className="opt-price">{usd(h.pricePerNight)}<small>/night</small></span>
                  </button>
                ))}
              </div>
            </section>
            <section>
              <h2>3. Car in {result.destinationCity} <span className="count">{result.cars.length} options</span></h2>
              <div className="options">
                {result.cars.map((c) => (
                  <button key={c.id} type="button" className={`option ${picked.car?.id === c.id ? "selected" : ""}`} onClick={() => pick("car", c)}>
                    <span className="opt-main">{c.model}</span>
                    <span className="opt-sub">{c.company} · {c.category.toLowerCase()} · {c.transmission.toLowerCase()}</span>
                    <span className="opt-price">{usd(c.pricePerDay)}<small>/day</small></span>
                  </button>
                ))}
              </div>
            </section>
          </div>

          <aside className="card summary">
            <h2>Your trip</h2>
            <dl>
              <dt>Flight</dt><dd>{picked.flight ? `${picked.flight.airline} ${picked.flight.flightNumber}` : "Not chosen"}</dd>
              <dt>Hotel</dt><dd>{picked.hotel ? picked.hotel.name : "Not chosen"}</dd>
              <dt>Car</dt><dd>{picked.car ? picked.car.model : "Not chosen"}</dd>
            </dl>
            <label>
              Nights
              <input id="nights" type="number" min={1} max={30} value={nights} onChange={(e) => setNights(Number(e.target.value))} />
            </label>
            <p className="dates">{form.date} → {addDays(form.date, nights)} · {form.passengers} traveler{form.passengers > 1 ? "s" : ""}{rooms > 1 ? ` · ${rooms} rooms` : ""}</p>
            <label className="demo">
              Demo: simulate a failure
              <select id="simulate" value={simulate} onChange={(e) => setSimulate(e.target.value)}>
                {FAILURE_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
              </select>
            </label>
            <p className="total"><span>Estimated total</span><strong>{usd(total)}</strong></p>
            {booking.error && <p className="alert">{errorMessage(booking.error)}</p>}
            <button className="primary wide" disabled={!ready || booking.loading} onClick={submitBooking}>
              {booking.loading ? "Booking… (payment, flight, hotel, car)" : "Book trip"}
            </button>
          </aside>
        </div>
      )}
    </div>
  );
}
