"""Client for the Orders service (which runs the booking SAGA)."""
import httpx

from app import config
from app.errors import AppError


async def _request(http: httpx.AsyncClient, method: str, path: str, **kwargs):
    try:
        response = await http.request(method, f"{config.ORDERS_URL}{path}", timeout=60, **kwargs)
    except httpx.TransportError as exc:
        raise AppError("The booking service is temporarily unavailable.", "SERVICE_UNAVAILABLE") from exc
    if response.status_code == 404:
        return None
    if 400 <= response.status_code < 500:
        detail = response.json().get("detail", "Invalid booking request.")
        raise AppError(detail if isinstance(detail, str) else "Invalid booking request.", "BAD_USER_INPUT")
    if response.is_error:
        raise AppError("The booking service failed.", "SERVICE_UNAVAILABLE")
    return response.json()


async def create_order(http: httpx.AsyncClient, payload: dict) -> dict:
    return await _request(http, "POST", "/orders", json=payload)


async def get_order(http: httpx.AsyncClient, order_id: str) -> dict | None:
    return await _request(http, "GET", f"/orders/{order_id}")


async def list_orders(http: httpx.AsyncClient, user_id: str, limit: int) -> list[dict]:
    return await _request(http, "GET", "/orders", params={"user_id": user_id, "limit": limit}) or []
