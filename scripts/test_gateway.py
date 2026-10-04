"""End-to-end checks against the GraphQL gateway: search, sessions, Argon2id,
SAGA bookings, authorization, rate limits and GraphQL abuse protection.

Run inside the gateway container (it creates 2 test users and a few orders):
    docker compose exec -T redis redis-cli FLUSHALL     # optional: reset rate-limit counters
    docker compose exec -T gateway python - < scripts/test_gateway.py
"""
import datetime as dt
import os
import uuid

import httpx
import psycopg

URL = "http://localhost:8000/graphql"
results = []


def check(name, ok, detail=""):
    ok = bool(ok)
    results.append(ok)
    print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))


def gql(client, query, variables=None):
    return client.post(URL, json={"query": query, "variables": variables or {}}, timeout=60).json()


def code(resp):
    return (resp.get("errors") or [{}])[0].get("extensions", {}).get("code")


day = (dt.date.today() + dt.timedelta(days=2)).isoformat()
run = uuid.uuid4().hex[:6]
email_a, email_b = f"ana.{run}@test.com", f"bob.{run}@test.com"
pw_a, pw_b = "Correct-Horse-1", "Battery-Staple-2"
login = "mutation($e: String!, $p: String!) { login(email: $e, password: $p) { id } }"
register = "mutation($e: String!, $p: String!, $n: String!) { register(email: $e, password: $p, fullName: $n) { id email } }"

print("== 1. GraphQL search (no over-fetching) ==")
with httpx.Client() as c:
    r = gql(c, """query($d: Date!) { searchPackages(origin: "BOG", destination: "CTG", date: $d) {
        flights { airline price } } }""", {"d": day})
    f = r["data"]["searchPackages"]["flights"]
    check("flights-only query returns only the requested fields", f and set(f[0]) == {"airline", "price"},
          f"{len(f)} flights, keys={sorted(f[0]) if f else []}")
    r = gql(c, """query($d: Date!) { searchPackages(origin: "BOG", destination: "CTG", date: $d, passengers: 2) {
        destinationCity flights { id departureAt price } hotels { id name pricePerNight } cars { id model pricePerDay } } }""",
            {"d": day})
    s = r["data"]["searchPackages"]
    check("full package search", s["flights"] and s["hotels"] and s["cars"],
          f"{s['destinationCity']}: {len(s['flights'])} flights, {len(s['hotels'])} hotels, {len(s['cars'])} cars")
    package = {"flightId": s["flights"][0]["id"], "hotelId": s["hotels"][0]["id"], "carId": s["cars"][0]["id"]}
    package2 = {"flightId": s["flights"][-1]["id"], "hotelId": s["hotels"][1]["id"], "carId": s["cars"][1]["id"]}
    r = gql(c, '{ searchPackages(origin: "BOG", destination: "XXX", date: "2026-10-10") { origin } }')
    check("invalid airport rejected", code(r) == "BAD_USER_INPUT")

print("\n== 2. Session fixation ==")
a = httpx.Client()
gql(a, "{ me { id } }")
s0 = a.cookies.get("wsid")
check("anonymous session cookie issued before login", s0)
gql(a, register, {"e": email_a, "p": pw_a, "n": "Ana Test"})
s1 = a.cookies.get("wsid")
check("register: session id regenerated", s1 and s1 != s0)
gql(a, "mutation { logout }")
s2 = a.cookies.get("wsid")
gql(a, login, {"e": email_a, "p": pw_a})
s3 = a.cookies.get("wsid")
check("login: session id regenerated", s3 and s3 != s2, f"{s2[:8]}… -> {s3[:8]}…")
with httpx.Client(cookies={"wsid": s2}) as attacker:
    check("old (pre-login) session id is useless", gql(attacker, "{ me { id } }")["data"]["me"] is None)
with httpx.Client(cookies={"wsid": s1}) as attacker:
    check("session id from before logout is useless", gql(attacker, "{ me { id } }")["data"]["me"] is None)
check("new session id identifies the user", gql(a, "{ me { email } }")["data"]["me"]["email"] == email_a)
raw = a.post(URL, json={"query": "mutation { logout }"}).headers.get("set-cookie", "")
check("cookie flags HttpOnly + SameSite=Lax", "HttpOnly" in raw and "samesite=lax" in raw.lower(), raw.split(";", 1)[-1].strip())
gql(a, login, {"e": email_a, "p": pw_a})

print("\n== 3. Argon2id password storage ==")
with psycopg.connect(os.environ["DATABASE_URL"], prepare_threshold=None, sslmode="require") as conn:
    h = conn.execute("select password_hash from private.users where email = %s", (email_a,)).fetchone()[0]
check("password stored as Argon2id", h.startswith("$argon2id$"), h[:30] + "…")
r = gql(httpx.Client(), 'mutation { register(email: "weak@test.com", password: "short", fullName: "X") { id } }')
check("weak password rejected", code(r) == "BAD_USER_INPUT")

print("\n== 4. Booking through GraphQL (SAGA) ==")
book = """mutation($i: BookPackageInput!) { bookPackage(input: $i) {
    id status totalAmount failureReason payment { status } sagaSteps { step action status } } }"""
base = {**package, "checkIn": day, "checkOut": (dt.date.fromisoformat(day) + dt.timedelta(days=3)).isoformat(), "passengers": 2}
check("booking without login rejected", code(gql(httpx.Client(), book, {"i": base})) == "UNAUTHENTICATED")
ok = gql(a, book, {"i": {**base, "idempotencyKey": f"t-{uuid.uuid4()}"}})["data"]["bookPackage"]
check("happy path", ok["status"] == "CONFIRMED" and ok["payment"]["status"] == "CAPTURED", f"total {ok['totalAmount']}")
bad = gql(a, book, {"i": {**base, **package2, "idempotencyKey": f"t-{uuid.uuid4()}", "simulateFailure": "CAR"}})["data"]["bookPackage"]
comp = [s["step"] for s in bad["sagaSteps"] if s["action"] == "COMPENSATE" and s["status"] == "SUCCEEDED"]
check("CAR failure compensated", bad["status"] == "CANCELLED" and comp == ["HOTEL", "FLIGHT", "PAYMENT"],
      f"undone: {' -> '.join(comp)}; reason: {bad['failureReason']}")
mine = gql(a, "{ myOrders { id status } }")["data"]["myOrders"]
check("myOrders lists the user's orders", {ok["id"], bad["id"]} <= {o["id"] for o in mine}, f"{len(mine)} orders")
detail = gql(a, "query($id: ID!) { order(id: $id) { status sagaSteps { step } } }", {"id": bad["id"]})["data"]["order"]
check("order detail with SAGA timeline", detail and len(detail["sagaSteps"]) == 14, f"{len(detail['sagaSteps'])} steps")

print("\n== 5. Authorization between users ==")
b = httpx.Client()
gql(b, register, {"e": email_b, "p": pw_b, "n": "Bob Test"})
check("another user cannot read Ana's order", gql(b, "query($id: ID!) { order(id: $id) { id } }", {"id": ok["id"]})["data"]["order"] is None)

print("\n== 6. Rate limiting: checkout ==")
codes = [code(gql(a, book, {"i": {**base, "flightId": "not-a-uuid"}})) for _ in range(4)]
check("6th checkout in a minute blocked", codes[:3] == ["BAD_USER_INPUT"] * 3 and codes[3] == "RATE_LIMITED", " ".join(codes))

print("\n== 7. Rate limiting: login brute force ==")
anon = httpx.Client()
codes = [code(gql(anon, login, {"e": email_b, "p": "wrong-password"})) for _ in range(6)]
print("      attempts:", " ".join(codes))
check("brute force blocked by IP rate limit (max 5 per 60s)", codes[0] == "INVALID_CREDENTIALS" and codes[-1] == "RATE_LIMITED")

print("\n== 8. GraphQL abuse protection ==")
r = gql(httpx.Client(), "{ " + " ".join(f"a{i}: me {{ id }}" for i in range(20)) + " }")
check("alias batching blocked", r.get("errors"), r["errors"][0]["message"][:60] if r.get("errors") else "")

print(f"\n{sum(results)}/{len(results)} checks passed")
