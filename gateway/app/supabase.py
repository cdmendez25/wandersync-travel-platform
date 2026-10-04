"""Client for Supabase's native GraphQL endpoint (pg_graphql, /graphql/v1).

The gateway forwards only the fields the frontend actually selected, so no
column is read from the database unless the client asked for it.
"""
import json

import httpx

from app import config
from app.errors import AppError


class GqlEnum(str):
    """Marks a value that must be written as a GraphQL enum (unquoted)."""


def literal(value) -> str:
    """Python value -> GraphQL literal. Strings are JSON-escaped, so user input cannot break out."""
    if isinstance(value, GqlEnum):
        return str(value)
    if isinstance(value, dict):
        return "{" + ", ".join(f"{k}: {literal(v)}" for k, v in value.items()) + "}"
    if isinstance(value, list):
        return "[" + ", ".join(literal(v) for v in value) + "]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return json.dumps(str(value))


def _headers() -> dict:
    headers = {"apikey": config.SUPABASE_SERVICE_KEY}
    if config.SUPABASE_SERVICE_KEY.startswith("eyJ"):  # legacy JWT keys also go in Authorization
        headers["Authorization"] = f"Bearer {config.SUPABASE_SERVICE_KEY}"
    return headers


async def fetch_collection(
    http: httpx.AsyncClient, collection: str, filter: dict, order_by: dict, first: int, fields: list[str]
) -> list[dict]:
    query = "query { %s(filter: %s, orderBy: [%s], first: %d) { edges { node { %s } } } }" % (
        collection, literal(filter), literal(order_by), first, " ".join(fields),
    )
    try:
        response = await http.post(f"{config.SUPABASE_URL}/graphql/v1", json={"query": query}, headers=_headers())
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise AppError("The travel catalog is temporarily unavailable.", "SERVICE_UNAVAILABLE") from exc
    body = response.json()
    if body.get("errors"):
        raise AppError("The travel catalog query failed.", "SERVICE_UNAVAILABLE")
    return [edge["node"] for edge in body["data"][collection]["edges"]]
