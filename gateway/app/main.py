"""WanderSync API Gateway: GraphQL over FastAPI, the only public entry point."""
from contextlib import asynccontextmanager

import httpx
import strawberry
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from strawberry.extensions import MaskErrors, MaxAliasesLimiter, QueryDepthLimiter
from strawberry.fastapi import GraphQLRouter

from app import config, ratelimit
from app.errors import should_mask_error
from app.schema import Mutation, Query
from app.sessions import SessionStore


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.redis = Redis.from_url(config.REDIS_URL, decode_responses=True)
    app.state.sessions = SessionStore(app.state.redis)
    app.state.http = httpx.AsyncClient(timeout=10)
    app.state.db = AsyncConnectionPool(
        config.DATABASE_URL,
        min_size=1,
        max_size=2,
        kwargs={"prepare_threshold": None, "sslmode": "require", "row_factory": dict_row},
        check=AsyncConnectionPool.check_connection,
        open=False,
    )
    await app.state.db.open()
    yield
    await app.state.db.close()
    await app.state.http.aclose()
    await app.state.redis.aclose()


schema = strawberry.Schema(
    query=Query,
    mutation=Mutation,
    extensions=[
        QueryDepthLimiter(max_depth=6),           # blocks deeply nested abusive queries
        MaxAliasesLimiter(max_alias_count=15),    # blocks alias batching (e.g. 100 logins in one request)
        MaskErrors(should_mask_error=should_mask_error),
    ],
)


async def get_context(request: Request, response: Response) -> dict:
    state = request.app.state
    return {
        "request": request,
        "response": response,
        "redis": state.redis,
        "sessions": state.sessions,
        "http": state.http,
        "db": state.db,
    }


app = FastAPI(title="WanderSync API Gateway", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)


@app.middleware("http")
async def session_and_rate_limit(request: Request, call_next):
    if not request.url.path.startswith("/graphql"):
        return await call_next(request)

    client_ip = request.client.host if request.client else "unknown"
    limit, window = config.RATE_GLOBAL
    allowed, retry_after = await ratelimit.hit(request.app.state.redis, f"global:{client_ip}", limit, window)
    if not allowed:
        return JSONResponse(
            {"errors": [{"message": "Too many requests.", "extensions": {"code": "RATE_LIMITED"}}]},
            status_code=429,
            headers={"Retry-After": str(retry_after)},
        )

    store: SessionStore = request.app.state.sessions
    incoming_sid = request.cookies.get(config.SESSION_COOKIE)
    session = await store.load(incoming_sid) or await store.create()
    request.state.session = session

    response = await call_next(request)

    # Login / logout replace request.state.session; send the new id to the browser
    current = request.state.session
    if current.id != incoming_sid:
        response.set_cookie(
            config.SESSION_COOKIE,
            current.id,
            max_age=config.SESSION_ABSOLUTE_SECONDS,
            httponly=True,             # not readable from JavaScript
            secure=config.COOKIE_SECURE,
            samesite="lax",            # not sent on cross-site POSTs (CSRF)
            path="/",
        )
    return response


# Added last so it runs first: answers browser preflights for the frontend origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=config.FRONTEND_ORIGINS,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type"],
)

app.include_router(
    GraphQLRouter(schema, context_getter=get_context, graphql_ide="graphiql" if config.GRAPHIQL else None),
    prefix="/graphql",
)


@app.get("/health")
def health():
    return {"status": "ok"}
