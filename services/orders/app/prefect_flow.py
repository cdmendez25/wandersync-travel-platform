"""Trigger and wait for the Prefect deployment that runs a booking SAGA."""
import os
import time
from urllib.parse import quote

import httpx

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api").rstrip("/")
DEPLOYMENT_NAME = os.getenv("SAGA_DEPLOYMENT", "booking-saga/booking-saga")
FLOW_TIMEOUT = float(os.getenv("SAGA_FLOW_TIMEOUT_SECONDS", "55"))
POLL_INTERVAL = 0.25

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}


class SagaFlowError(Exception):
    """The Prefect SAGA deployment could not be started or did not complete."""


def _request(method: str, path: str, **kwargs) -> dict:
    try:
        response = httpx.request(method, f"{PREFECT_API_URL}{path}", timeout=5, **kwargs)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise SagaFlowError(f"Prefect request failed: {exc}") from exc
    return response.json()


def run_booking_saga(order_id: str, fail_at: str | None) -> str:
    """Create the deployment run and wait so the existing order API stays synchronous."""
    flow_name, deployment_name = DEPLOYMENT_NAME.split("/", maxsplit=1)
    deployment = _request(
        "GET",
        f"/deployments/name/{quote(flow_name, safe='')}/{quote(deployment_name, safe='')}",
    )
    flow_run = _request(
        "POST",
        f"/deployments/{deployment['id']}/create_flow_run",
        json={"parameters": {"order_id": order_id, "fail_at": fail_at}},
    )

    deadline = time.monotonic() + FLOW_TIMEOUT
    while time.monotonic() < deadline:
        flow_run = _request("GET", f"/flow_runs/{flow_run['id']}")
        state = flow_run["state_type"]
        if state in TERMINAL_STATES:
            if state != "COMPLETED":
                raise SagaFlowError(f"Prefect SAGA run {flow_run['id']} ended as {state}")
            return flow_run["id"]
        time.sleep(POLL_INTERVAL)
    raise SagaFlowError(f"Timed out waiting for Prefect SAGA run {flow_run['id']}")
