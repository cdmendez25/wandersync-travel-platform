import httpx

from scraping import config

HEADERS = {"User-Agent": "WanderSyncBot/1.0 (academic project)"}


def fetch_html(path: str, params: dict) -> str:
    """GET a results page. Raises on timeouts and non-2xx so Prefect retries the task."""
    response = httpx.get(
        f"{config.MOCK_PROVIDER_URL}{path}",
        params=params,
        headers=HEADERS,
        timeout=config.HTTP_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    return response.text
