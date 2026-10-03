"""Prefect flow: scrape travel inventory on a Dask cluster and upsert it into Supabase.

    scrape (one task per page, runs on Dask workers, retried on network errors)
      -> parse + clean (inside the same task, on the worker)
      -> load (one upsert task per catalog table)
      -> scrape_runs row per source with the outcome
"""
from prefect import flow, get_run_logger, task
from prefect.cache_policies import NO_CACHE
from prefect.runtime import flow_run, task_run
from prefect_dask import DaskTaskRunner

from scraping import config, db, fetcher, parsers

SCRAPE_RETRIES = 3
SCRAPE_POLICY = dict(
    retries=SCRAPE_RETRIES,
    retry_delay_seconds=[2, 5, 10],  # backoff between attempts
    retry_jitter_factor=0.5,
    cache_policy=NO_CACHE,
    persist_result=False,
)


@task(name="scrape-flights", task_run_name="flights {origin}-{destination} {day}", **SCRAPE_POLICY)
def scrape_flights(origin: str, destination: str, day: str) -> dict:
    html = fetcher.fetch_html("/flights", {"origin": origin, "destination": destination, "date": day})
    return {"rows": parsers.parse_flights(html, config.PROVIDERS["flights"]), "attempts": task_run.run_count}


@task(name="scrape-hotels", task_run_name="hotels {city}", **SCRAPE_POLICY)
def scrape_hotels(city: str) -> dict:
    html = fetcher.fetch_html("/hotels", {"city": city})
    return {"rows": parsers.parse_hotels(html, config.PROVIDERS["hotels"]), "attempts": task_run.run_count}


@task(name="scrape-cars", task_run_name="cars {city}", **SCRAPE_POLICY)
def scrape_cars(city: str) -> dict:
    html = fetcher.fetch_html("/cars", {"city": city})
    return {"rows": parsers.parse_cars(html, config.PROVIDERS["cars"]), "attempts": task_run.run_count}


@task(name="load-to-supabase", task_run_name="load {table}", retries=2, retry_delay_seconds=5,
      cache_policy=NO_CACHE, persist_result=False)
def load_rows(table: str, rows: list[dict]) -> int:
    return db.upsert_rows(table, rows)


def _collect(futures, logger) -> tuple[list[dict], int, int]:
    """Wait for page tasks; keep going when a page failed even after all retries."""
    rows, attempts, failed = [], 0, 0
    for future in futures:
        try:
            result = future.result()
        except Exception as exc:
            failed += 1
            attempts += SCRAPE_RETRIES + 1
            logger.warning("Page failed after %s attempts: %r", SCRAPE_RETRIES + 1, exc)
            continue
        rows.extend(result["rows"])
        attempts += result["attempts"]
    return rows, attempts, failed


@flow(
    name="ingest-travel-inventory",
    task_runner=DaskTaskRunner(address=config.DASK_SCHEDULER_ADDRESS),
    log_prints=True,
)
def ingest_inventory(days_ahead: int = config.DAYS_AHEAD) -> dict:
    logger = get_run_logger()
    days = [d.isoformat() for d in config.travel_dates(days_ahead)]
    jobs = [(o, d, day) for o, d in config.ROUTES for day in days]

    run_ids = {source: db.start_scrape_run(flow_run.id, source, provider)
               for source, provider in config.PROVIDERS.items()}

    # Fan out every page to the Dask cluster at once
    futures = {
        "flights": scrape_flights.map(
            origin=[j[0] for j in jobs], destination=[j[1] for j in jobs], day=[j[2] for j in jobs]
        ),
        "hotels": scrape_hotels.map(city=config.CITIES),
        "cars": scrape_cars.map(city=config.CITIES),
    }

    summary, load_errors = {}, []
    for source, source_futures in futures.items():
        rows, attempts, failed = _collect(source_futures, logger)
        error = f"{failed} of {len(source_futures)} pages failed after retries" if failed else None
        try:
            upserted = load_rows.submit(source, rows).result() if rows else 0
        except Exception as exc:
            upserted, error = 0, f"load failed: {exc!r}"
            load_errors.append(source)
        status = "FAILED" if (upserted == 0 and (failed or source in load_errors)) else "SUCCEEDED"
        db.finish_scrape_run(run_ids[source], status, attempts, len(rows), upserted, error)
        summary[source] = {"pages": len(source_futures), "failed_pages": failed,
                           "attempts": attempts, "rows": len(rows), "upserted": upserted}
        logger.info("%s: %s", source, summary[source])

    if load_errors:
        raise RuntimeError(f"Could not load: {', '.join(load_errors)}")
    return summary


if __name__ == "__main__":
    if config.RUN_ON_START:
        try:
            ingest_inventory()
        except Exception as exc:  # keep serving the schedule even if the first run fails
            print(f"Initial run failed: {exc!r}")
    ingest_inventory.serve(
        name="scheduled-ingestion",
        interval=config.INTERVAL_SECONDS,
        tags=["scraping", "dask"],
    )
