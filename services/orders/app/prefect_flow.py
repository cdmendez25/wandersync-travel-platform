"""Trigger and wait for the Prefect deployment that runs a booking SAGA."""
import os
import time
from urllib.parse import quote

import httpx

PREFECT_API_URL = os.getenv("PREFECT_API_URL", "http://prefect-server:4200/api").rstrip("/")
DEPLOYMENT_NAME = os.getenv("SAGA_DEPLOYMENT", "booking-saga/booking-saga")
FLOW_TIMEOUT = float(os.getenv("SAGA_FLOW_TIMEOUT_SECONDS", "45"))
POLL_INTERVAL = 0.25

TERMINAL_STATES = {"COMPLETED", "FAILED", "CANCELLED", "CRASHED"}
NOT_STARTED_STATES = {"SCHEDULED", "PENDING"}


class SagaFlowError(Exception):
    """The Prefect SAGA run could not be started or did not complete.

    `stopped` is True when the run is certainly not executing any more (it was never created, it
    never started and was cancelled, or it ended abnormally), so the order can be settled right
    away. It is False when the outcome is unknown, for example the run is still going after the
    timeout: then the order must be left alone because the run may still finish it.
    """

    def __init__(self, message: str, stopped: bool = False):
        super().__init__(message)
        self.stopped = stopped


def _request(method: str, path: str, **kwargs) -> dict:
    try:
        response = httpx.request(method, f"{PREFECT_API_URL}{path}", timeout=5, **kwargs)
        response.raise_for_status()
    except httpx.HTTPError as exc:
        raise SagaFlowError(f"Prefect request failed: {exc}") from exc
    return response.json()


def _cancel_unstarted_run(flow_run_id: str) -> bool:
    """Cancel a run nobody picked up so the runner cannot execute it later. True if it is CANCELLED."""
    try:
        _request("POST", f"/flow_runs/{flow_run_id}/set_state", json={
            "state": {
                "type": "CANCELLED",
                "name": "Cancelled",
                "message": "Booking abandoned: the SAGA runner did not start it in time",
            },
            "force": True,
        })
        return _request("GET", f"/flow_runs/{flow_run_id}")["state_type"] == "CANCELLED"
    except SagaFlowError:
        return False


def run_booking_saga(order_id: str, fail_at: str | None) -> str:
    """Create the deployment run and wait so the existing order API stays synchronous."""
    flow_name, deployment_name = DEPLOYMENT_NAME.split("/", maxsplit=1)
    try:
        deployment = _request(
            "GET",
            f"/deployments/name/{quote(flow_name, safe='')}/{quote(deployment_name, safe='')}",
        )
        flow_run = _request(
            "POST",
            f"/deployments/{deployment['id']}/create_flow_run",
            json={"parameters": {"order_id": order_id, "fail_at": fail_at}},
        )
    except SagaFlowError as exc:
        raise SagaFlowError(f"The SAGA run could not be created: {exc}", stopped=True) from exc  # nothing started

    run_id, state = flow_run["id"], flow_run["state_type"]
    deadline = time.monotonic() + FLOW_TIMEOUT
    while time.monotonic() < deadline:
        try:
            state = _request("GET", f"/flow_runs/{run_id}")["state_type"]
        except SagaFlowError:
            time.sleep(POLL_INTERVAL)  # transient Prefect API error: keep trying until the deadline
            continue
        if state == "COMPLETED":
            return run_id
        if state in TERMINAL_STATES:
            raise SagaFlowError(f"Prefect SAGA run {run_id} ended as {state}", stopped=True)
        time.sleep(POLL_INTERVAL)

    if state in NOT_STARTED_STATES and _cancel_unstarted_run(run_id):
        raise SagaFlowError(
            f"No SAGA runner picked up the booking within {FLOW_TIMEOUT:.0f} s; the run was cancelled", stopped=True
        )
    raise SagaFlowError(f"Timed out waiting for Prefect SAGA run {run_id} (state {state})")
