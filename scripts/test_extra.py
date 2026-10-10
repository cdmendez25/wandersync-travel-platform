"""Extra end-to-end checks that complement scripts/test_gateway.py: idempotency (including a
simultaneous double submit), a forced failure at every SAGA step, input validation, account
lockout, and the register / global rate limits.

Run inside the gateway container. It creates 3 test users and several orders, and takes ~2.5 min
(each booking runs the SAGA as a Prefect flow, ~8-12 s):
    docker compose exec -T redis redis-cli FLUSHALL     # reset the rate-limit counters first
    docker compose exec -T gateway python - < scripts/test_extra.py
"""
import concurrent.futures as cf
import datetime as dt
import os
import time
import uuid

import httpx
import psycopg
from psycopg.rows import dict_row

URL = "http://localhost:8000/graphql"
PW = "Correct-Horse-1"  # same test password as scripts/test_gateway.py
results = []


def check(name, ok, detail=""):
    results.append(bool(ok))
    print(("PASS  " if ok else "FAIL  ") + name + (f"   ({detail})" if detail else ""), flush=True)


def gql(client, query, variables=None, timeout=120):
    return client.post(URL, json={"query": query, "variables": variables or {}}, timeout=timeout).json()


def code(resp):
    return (resp.get("errors") or [{}])[0].get("extensions", {}).get("code")


def db():
    return psycopg.connect(os.environ["DATABASE_URL"], prepare_threshold=None, sslmode="require",
                           row_factory=dict_row, connect_timeout=15)


REG = "mutation($e: String!, $p: String!, $n: String!) { register(email: $e, password: $p, fullName: $n) { id email } }"
LOGIN = "mutation($e: String!, $p: String!) { login(email: $e, password: $p) { id } }"
BOOK = ("mutation($i: BookPackageInput!) { bookPackage(input: $i) { id status failureReason "
        "payment { status } sagaSteps { step action status } } }")
SEARCH = ('query($d: Date!) { searchPackages(origin: "BOG", destination: "CTG", date: $d, passengers: 1) '
          '{ flights { id seatsAvailable } hotels { id roomsAvailable } cars { id unitsAvailable } } }')

day = (dt.date.today() + dt.timedelta(days=2)).isoformat()
out = (dt.date.fromisoformat(day) + dt.timedelta(days=3)).isoformat()
run = uuid.uuid4().hex[:6]
emails = {n: f"extra.{n}.{run}@test.com" for n in ("u1", "u2", "u3", "u4")}

with httpx.Client() as anon:
    s = gql(anon, SEARCH, {"d": day})["data"]["searchPackages"]
assert s["flights"] and s["hotels"] and s["cars"], "the catalog has no data for BOG -> CTG"
pick = lambda items, key: max(items, key=lambda x: x[key])["id"]  # best-stocked option, so stock never gets in the way
base = {"flightId": pick(s["flights"], "seatsAvailable"), "hotelId": pick(s["hotels"], "roomsAvailable"),
        "carId": pick(s["cars"], "unitsAvailable"), "checkIn": day, "checkOut": out, "passengers": 1}

# ------------------------------------------------------------------ T1
print("== T1. Idempotency: the same key sent twice in a row ==")
u1 = httpx.Client()
check("register user u1", gql(u1, REG, {"e": emails["u1"], "p": PW, "n": "Extra One"}).get("data"))
key = f"idem-{run}"
t0 = time.time()
r1 = gql(u1, BOOK, {"i": {**base, "idempotencyKey": key}})["data"]["bookPackage"]
dur = time.time() - t0
r2 = gql(u1, BOOK, {"i": {**base, "idempotencyKey": key}})["data"]["bookPackage"]
check("the first booking is CONFIRMED", r1["status"] == "CONFIRMED", f"{dur:.1f} s end to end")
check("the second submit returns the SAME order", r2["id"] == r1["id"])
with db() as c:
    n_orders = c.execute("select count(*) as n from public.orders where idempotency_key = %s", (key,)).fetchone()["n"]
    n_steps = c.execute("select count(*) as n from public.saga_steps where order_id = %s", (r1["id"],)).fetchone()["n"]
check("1 order and 8 events: the SAGA did not run twice", n_orders == 1 and n_steps == 8, f"orders={n_orders} events={n_steps}")

# ------------------------------------------------------------------ T2
print("\n== T2. Double click: two SIMULTANEOUS submits with the same key ==")
key2 = f"idem-par-{run}"
wsid = u1.cookies.get("wsid")


def send(_):
    with httpx.Client(cookies={"wsid": wsid}) as cl:
        return gql(cl, BOOK, {"i": {**base, "idempotencyKey": key2}})


with cf.ThreadPoolExecutor(2) as ex:
    a, b = list(ex.map(send, range(2)))
ids = {x["data"]["bookPackage"]["id"] for x in (a, b) if x.get("data") and x["data"].get("bookPackage")}
check("both submits return the same order", len(ids) == 1, f"errors: {code(a)} {code(b)}")
with db() as c:
    row = c.execute("select id, status from public.orders where idempotency_key = %s", (key2,)).fetchall()
    n_steps = c.execute("select count(*) as n from public.saga_steps where order_id = %s", (row[0]["id"],)).fetchone()["n"] if row else -1
check("1 order, CONFIRMED and 8 events", len(row) == 1 and row[0]["status"] == "CONFIRMED" and n_steps == 8,
      f"orders={len(row)} status={row[0]['status'] if row else '-'} events={n_steps}")

# ------------------------------------------------------------------ T3
print("\n== T3. Validation and a forced failure at each SAGA step (user u2) ==")
u2 = httpx.Client()
gql(u2, REG, {"e": emails["u2"], "p": PW, "n": "Extra Two"})
r = gql(u2, BOOK, {"i": {**base, "checkOut": day}})
check("invalid dates (check-out = check-in) rejected", code(r) == "BAD_USER_INPUT", str(code(r)))
r = gql(u2, BOOK, {"i": {**base, "passengers": 10}})
check("10 travelers rejected (maximum 9)", code(r) == "BAD_USER_INPUT", str(code(r)))
expected = {"PAYMENT": ([], "FAILED", 2), "FLIGHT": (["PAYMENT"], "REFUNDED", 6), "HOTEL": (["FLIGHT", "PAYMENT"], "REFUNDED", 10)}
for step, (undone, pay, events) in expected.items():
    t0 = time.time()
    o = gql(u2, BOOK, {"i": {**base, "idempotencyKey": f"fail-{step}-{run}", "simulateFailure": step}})["data"]["bookPackage"]
    comp = [x["step"] for x in o["sagaSteps"] if x["action"] == "COMPENSATE" and x["status"] == "SUCCEEDED"]
    check(f"failure at {step}: CANCELLED, undoes {undone or 'nothing'}, payment {pay}, {events} events",
          o["status"] == "CANCELLED" and comp == undone and o["payment"]["status"] == pay and len(o["sagaSteps"]) == events,
          f"{time.time() - t0:.1f} s; undone={comp}; payment={o['payment']['status']}; events={len(o['sagaSteps'])}")
    with db() as c:
        res = [c.execute(f"select status from public.{t} where order_id = %s", (o["id"],)).fetchone()
               for t in ("flight_reservations", "hotel_reservations", "car_reservations")]
    check(f"failure at {step}: no reservation left CONFIRMED (no orphan bookings)",
          all(x is None or x["status"] == "CANCELLED" for x in res), str([x["status"] if x else None for x in res]))

# ------------------------------------------------------------------ T4
print("\n== T4. Account lockout after 5 wrong passwords (user u3) ==")
u3 = httpx.Client()
gql(u3, REG, {"e": emails["u3"], "p": PW, "n": "Extra Three"})
gql(u3, "mutation { logout }")
anon = httpx.Client()
codes = [code(gql(anon, LOGIN, {"e": emails["u3"], "p": "definitely-wrong-password"})) for _ in range(5)]
check("5 failed attempts -> INVALID_CREDENTIALS", codes == ["INVALID_CREDENTIALS"] * 5, str(codes))
with db() as c:
    row = c.execute("select locked_until > now() as locked from private.users where email = %s", (emails["u3"],)).fetchone()
check("the account is locked in the database (locked_until)", row["locked"] is True)
print("   waiting 62 s for the per-IP login window (5/min) to pass ...", flush=True)
time.sleep(62)
r = gql(anon, LOGIN, {"e": emails["u3"], "p": PW})
msg = (r.get("errors") or [{}])[0].get("message", "")
check("the CORRECT password is still refused (locked for 15 min)", code(r) == "INVALID_CREDENTIALS" and "locked" in msg, msg[:70])

# ------------------------------------------------------------------ T5
print("\n== T5. Register limit (3 per 10 min per IP) ==")
r = gql(httpx.Client(), REG, {"e": emails["u4"], "p": PW, "n": "Extra Four"})
check("the 4th sign-up within 10 min is blocked", code(r) == "RATE_LIMITED", str(code(r)))

# ------------------------------------------------------------------ T6
print("\n== T6. Global limit: 130 requests in a row from the same IP ==")
cl = httpx.Client()
statuses, retry, body429 = [], None, None
for _ in range(130):
    resp = cl.post(URL, json={"query": "{ me { id } }"}, timeout=30)
    statuses.append(resp.status_code)
    if resp.status_code == 429 and retry is None:
        retry, body429 = resp.headers.get("retry-after"), resp.json()
ok200, n429 = statuses.count(200), statuses.count(429)
check("after ~120 requests per minute the gateway answers HTTP 429", n429 > 0 and 100 <= ok200 <= 125, f"200: {ok200}, 429: {n429}")
check("the 429 carries Retry-After and the RATE_LIMITED code",
      retry is not None and body429["errors"][0]["extensions"]["code"] == "RATE_LIMITED", f"Retry-After={retry}")

# ------------------------------------------------------------------ T7
print("\n== T7. Database consistency of the orders created by this run ==")
with db() as c:
    rows = c.execute(
        "select o.status, p.status as pay, fr.status as f, hr.status as h, cr.status as c "
        "from public.orders o left join public.payments p on p.order_id = o.id "
        "left join public.flight_reservations fr on fr.order_id = o.id "
        "left join public.hotel_reservations hr on hr.order_id = o.id "
        "left join public.car_reservations cr on cr.order_id = o.id "
        "where o.idempotency_key like %s or o.idempotency_key like %s or o.idempotency_key like %s",
        (f"idem-{run}", f"idem-par-{run}", f"fail-%-{run}")).fetchall()
good = [r for r in rows if r["status"] == "CONFIRMED"]
bad = [r for r in rows if r["status"] == "CANCELLED"]
check("CONFIRMED orders: payment CAPTURED and flight/hotel/car CONFIRMED",
      bool(good) and all((r["pay"], r["f"], r["h"], r["c"]) == ("CAPTURED", "CONFIRMED", "CONFIRMED", "CONFIRMED") for r in good), f"{len(good)} orders")
check("CANCELLED orders: no reservation CONFIRMED and payment FAILED/REFUNDED",
      bool(bad) and all(all(x != "CONFIRMED" for x in (r["f"], r["h"], r["c"])) and r["pay"] in ("FAILED", "REFUNDED") for r in bad), f"{len(bad)} orders")

print(f"\n{sum(results)}/{len(results)} checks passed   (test users: extra.*.{run}@test.com)")
