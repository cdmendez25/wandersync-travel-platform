"""GraphQL schema: the single entry point for the frontend.

Queries   me, searchPackages, myOrders, order
Mutations register, login, logout, bookPackage
"""
import datetime as dt
import re
import typing
import uuid
from decimal import Decimal
from enum import Enum

import strawberry
from strawberry.types import Info
from strawberry.types.nodes import FragmentSpread, InlineFragment, SelectedField

from app import auth, config, orders, ratelimit, supabase
from app.errors import AppError

COT = dt.timezone(dt.timedelta(hours=-5))  # flights are searched by local Colombian date
_IATA = re.compile(r"^[A-Z]{3}$")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_camel(name: str) -> str:
    head, *tail = name.split("_")
    return head + "".join(part.title() for part in tail)


def _coerce(hint, value):
    """Convert JSON values (strings from pg_graphql / services) to the Python type of the field."""
    if value is None:
        return None
    if isinstance(value, uuid.UUID):  # DB rows carry UUID objects; GraphQL IDs are strings
        return str(value)
    args = [a for a in typing.get_args(hint) if a is not type(None)]
    base = args[0] if args else hint
    if isinstance(base, type) and isinstance(value, base):  # already the right type (DB rows)
        return value
    if base is Decimal:
        return Decimal(str(value))
    if base is dt.datetime:
        return dt.datetime.fromisoformat(value)
    if base is dt.date:
        return dt.date.fromisoformat(str(value)[:10])
    if base is int:
        return int(value)
    return value


def _build(cls, data: dict, camel_keys: bool = False):
    hints = typing.get_type_hints(cls)
    kwargs = {}
    for name, hint in hints.items():
        key = _to_camel(name) if camel_keys else name
        if key in data:
            kwargs[name] = _coerce(hint, data[key])
    return cls(**kwargs)


def _gql_fields(cls) -> set[str]:
    return {_to_camel(name) for name in typing.get_type_hints(cls)}


def _selected(info: Info, allowed: set[str]) -> list[str]:
    """Names of the sub-fields the client asked for (fragments included)."""
    names: set[str] = set()

    def walk(selections):
        for sel in selections:
            if isinstance(sel, SelectedField):
                names.add(sel.name)
            elif isinstance(sel, (FragmentSpread, InlineFragment)):
                walk(sel.selections)

    walk(info.selected_fields[0].selections)
    return sorted(names & allowed) or ["id"]


def _client_ip(info: Info) -> str:
    request = info.context["request"]
    return request.client.host if request.client else "unknown"


def _require_user(info: Info) -> str:
    user_id = info.context["request"].state.session.user_id
    if not user_id:
        raise AppError("You need to log in first.", "UNAUTHENTICATED")
    return user_id


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------

@strawberry.type
class User:
    id: strawberry.ID
    email: str
    full_name: str
    created_at: dt.datetime


@strawberry.type
class Flight:
    id: strawberry.ID | None = None
    airline: str | None = None
    flight_number: str | None = None
    origin: str | None = None
    destination: str | None = None
    departure_at: dt.datetime | None = None
    arrival_at: dt.datetime | None = None
    cabin_class: str | None = None
    price: Decimal | None = None
    currency: str | None = None
    seats_available: int | None = None


@strawberry.type
class Hotel:
    id: strawberry.ID | None = None
    name: str | None = None
    city: str | None = None
    address: str | None = None
    stars: int | None = None
    room_type: str | None = None
    price_per_night: Decimal | None = None
    currency: str | None = None
    rooms_available: int | None = None
    rating: Decimal | None = None


@strawberry.type
class Car:
    id: strawberry.ID | None = None
    company: str | None = None
    model: str | None = None
    category: str | None = None
    city: str | None = None
    seats: int | None = None
    transmission: str | None = None
    price_per_day: Decimal | None = None
    currency: str | None = None
    units_available: int | None = None


FLIGHT_FIELDS, HOTEL_FIELDS, CAR_FIELDS = _gql_fields(Flight), _gql_fields(Hotel), _gql_fields(Car)


@strawberry.type(description="Flights, hotels and cars for one trip. Each list is fetched only if requested.")
class PackageSearch:
    origin: str
    destination: str
    destination_city: str
    date: dt.date
    passengers: int
    limit: strawberry.Private[int] = 10

    @strawberry.field
    async def flights(self, info: Info) -> list[Flight]:
        start = dt.datetime.combine(self.date, dt.time.min, tzinfo=COT)
        nodes = await supabase.fetch_collection(
            info.context["http"], "flightsCollection",
            {
                "origin": {"eq": self.origin},
                "destination": {"eq": self.destination},
                "departureAt": {"gte": start.isoformat(), "lt": (start + dt.timedelta(days=1)).isoformat()},
                "seatsAvailable": {"gte": self.passengers},
            },
            {"price": supabase.GqlEnum("AscNullsLast")},
            self.limit, _selected(info, FLIGHT_FIELDS),
        )
        return [_build(Flight, n, camel_keys=True) for n in nodes]

    @strawberry.field
    async def hotels(self, info: Info) -> list[Hotel]:
        nodes = await supabase.fetch_collection(
            info.context["http"], "hotelsCollection",
            {"city": {"eq": self.destination_city}, "roomsAvailable": {"gte": (self.passengers + 1) // 2}},
            {"pricePerNight": supabase.GqlEnum("AscNullsLast")},
            self.limit, _selected(info, HOTEL_FIELDS),
        )
        return [_build(Hotel, n, camel_keys=True) for n in nodes]

    @strawberry.field
    async def cars(self, info: Info) -> list[Car]:
        nodes = await supabase.fetch_collection(
            info.context["http"], "carsCollection",
            {"city": {"eq": self.destination_city}, "unitsAvailable": {"gte": 1}},
            {"pricePerDay": supabase.GqlEnum("AscNullsLast")},
            self.limit, _selected(info, CAR_FIELDS),
        )
        return [_build(Car, n, camel_keys=True) for n in nodes]


@strawberry.enum
class SagaStepName(Enum):
    PAYMENT = "PAYMENT"
    FLIGHT = "FLIGHT"
    HOTEL = "HOTEL"
    CAR = "CAR"


@strawberry.type
class SagaStep:
    step: str
    action: str
    status: str
    error: str | None = None
    created_at: dt.datetime | None = None


@strawberry.type
class Payment:
    status: str
    amount: Decimal
    currency: str
    provider_ref: str | None = None


@strawberry.type
class Order:
    id: strawberry.ID
    status: str
    total_amount: Decimal
    currency: str
    check_in: dt.date
    check_out: dt.date
    passengers: int | None = None
    failure_reason: str | None = None
    created_at: dt.datetime | None = None
    payment: Payment | None = None
    saga_steps: list[SagaStep] = strawberry.field(default_factory=list)


def _order(data: dict) -> Order:
    order = _build(Order, {k: v for k, v in data.items() if k not in ("payment", "saga_steps")})
    order.payment = _build(Payment, data["payment"]) if data.get("payment") else None
    order.saga_steps = [_build(SagaStep, s) for s in data.get("saga_steps", [])]
    return order


@strawberry.input
class BookPackageInput:
    flight_id: strawberry.ID
    hotel_id: strawberry.ID
    car_id: strawberry.ID
    check_in: dt.date
    check_out: dt.date
    passengers: int = 1
    idempotency_key: str | None = strawberry.field(
        default=None, description="Send the same key when retrying a checkout so it is booked only once."
    )
    simulate_failure: SagaStepName | None = strawberry.field(
        default=None, description="Demo only: force this SAGA step to fail to show the compensations."
    )


# ---------------------------------------------------------------------------
# Root types
# ---------------------------------------------------------------------------

@strawberry.type
class Query:
    @strawberry.field(description="The logged-in user, or null.")
    async def me(self, info: Info) -> User | None:
        user_id = info.context["request"].state.session.user_id
        if not user_id:
            return None
        user = await auth.get_user(info.context["db"], user_id)
        return _build(User, user) if user else None

    @strawberry.field(description="Search a trip package: flights on the date plus hotels and cars at the destination.")
    def search_packages(
        self, origin: str, destination: str, date: dt.date, passengers: int = 1, limit: int = 10
    ) -> PackageSearch:
        origin, destination = origin.upper(), destination.upper()
        if not (_IATA.match(origin) and destination in config.CITY_BY_AIRPORT) or origin == destination:
            raise AppError("Use two different airport codes, e.g. BOG and CTG.", "BAD_USER_INPUT")
        if not 1 <= passengers <= 9 or not 1 <= limit <= 50:
            raise AppError("passengers must be 1-9 and limit 1-50.", "BAD_USER_INPUT")
        return PackageSearch(
            origin=origin, destination=destination, destination_city=config.CITY_BY_AIRPORT[destination],
            date=date, passengers=passengers, limit=limit,
        )

    @strawberry.field(description="Orders of the logged-in user, newest first.")
    async def my_orders(self, info: Info, limit: int = 20) -> list[Order]:
        user_id = _require_user(info)
        rows = await orders.list_orders(info.context["http"], user_id, max(1, min(limit, 100)))
        return [_order(r) for r in rows]

    @strawberry.field(description="One order with its payment and SAGA timeline.")
    async def order(self, info: Info, id: strawberry.ID) -> Order | None:
        user_id = _require_user(info)
        try:
            uuid.UUID(str(id))
        except ValueError:
            return None
        data = await orders.get_order(info.context["http"], str(id))
        if not data or str(data["user_id"]) != user_id:
            return None  # other users' orders look exactly like missing ones
        return _order(data)


@strawberry.type
class Mutation:
    @strawberry.mutation(description="Create an account and log in.")
    async def register(self, info: Info, email: str, password: str, full_name: str) -> User:
        redis, request = info.context["redis"], info.context["request"]
        await ratelimit.enforce(redis, f"register:ip:{_client_ip(info)}", config.RATE_REGISTER_IP)
        user = await auth.register(info.context["db"], email, password, full_name)
        request.state.session = await info.context["sessions"].regenerate(request.state.session, str(user["id"]))
        return _build(User, user)

    @strawberry.mutation(description="Log in. The session id is replaced by a new one (prevents session fixation).")
    async def login(self, info: Info, email: str, password: str) -> User:
        redis, request = info.context["redis"], info.context["request"]
        await ratelimit.enforce(redis, f"login:ip:{_client_ip(info)}", config.RATE_LOGIN_IP)
        await ratelimit.enforce(redis, f"login:email:{auth.normalize_email(email)}", config.RATE_LOGIN_EMAIL)
        user = await auth.authenticate(info.context["db"], email, password)
        request.state.session = await info.context["sessions"].regenerate(request.state.session, str(user["id"]))
        return _build(User, user)

    @strawberry.mutation(description="Log out and discard the session.")
    async def logout(self, info: Info) -> bool:
        request = info.context["request"]
        request.state.session = await info.context["sessions"].regenerate(request.state.session, None)
        return True

    @strawberry.mutation(description="Book a package. Runs the SAGA: payment, flight, hotel, car.")
    async def book_package(self, info: Info, input: BookPackageInput) -> Order:
        user_id = _require_user(info)
        await ratelimit.enforce(info.context["redis"], f"checkout:user:{user_id}", config.RATE_CHECKOUT_USER)
        if input.simulate_failure and not config.ALLOW_FAILURE_SIMULATION:
            raise AppError("Failure simulation is disabled.", "FORBIDDEN")
        for name in ("flight_id", "hotel_id", "car_id"):
            try:
                uuid.UUID(str(getattr(input, name)))
            except ValueError:
                raise AppError(f"{_to_camel(name)} is not a valid id.", "BAD_USER_INPUT") from None

        data = await orders.create_order(info.context["http"], {
            "user_id": user_id,  # always taken from the session, never from the client
            "idempotency_key": input.idempotency_key or f"gw-{uuid.uuid4()}",
            "flight_id": str(input.flight_id),
            "hotel_id": str(input.hotel_id),
            "car_id": str(input.car_id),
            "passengers": input.passengers,
            "check_in": input.check_in.isoformat(),
            "check_out": input.check_out.isoformat(),
            "simulate_failure": input.simulate_failure.value if input.simulate_failure else None,
        })
        return _order(data)
