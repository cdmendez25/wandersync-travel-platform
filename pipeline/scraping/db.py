"""Supabase persistence over the Postgres session pooler (IPv4 friendly)."""
import psycopg
from psycopg import sql

from scraping import config

NATURAL_KEY = ("provider", "external_id")

TABLE_COLUMNS = {
    "flights": ["provider", "external_id", "airline", "flight_number", "origin", "destination",
                "departure_at", "arrival_at", "cabin_class", "price", "currency", "seats_available"],
    "hotels": ["provider", "external_id", "name", "city", "country", "address", "stars", "room_type",
               "price_per_night", "currency", "rooms_available", "rating"],
    "cars": ["provider", "external_id", "company", "model", "category", "city", "seats",
             "transmission", "price_per_day", "currency", "units_available"],
}


def connect() -> psycopg.Connection:
    # prepare_threshold=None keeps it compatible with the transaction pooler too
    return psycopg.connect(config.DATABASE_URL, prepare_threshold=None, sslmode="require", connect_timeout=10)


def _upsert_query(table: str) -> sql.Composed:
    columns = TABLE_COLUMNS[table]
    updates = [
        sql.SQL("{0} = excluded.{0}").format(sql.Identifier(c)) for c in columns if c not in NATURAL_KEY
    ]
    return sql.SQL(
        "insert into public.{table} ({columns}, scraped_at) values ({values}, now()) "
        "on conflict (provider, external_id) do update set {updates}, scraped_at = excluded.scraped_at"
    ).format(
        table=sql.Identifier(table),
        columns=sql.SQL(", ").join(map(sql.Identifier, columns)),
        values=sql.SQL(", ").join(map(sql.Placeholder, columns)),
        updates=sql.SQL(", ").join(updates),
    )


def upsert_rows(table: str, rows: list[dict]) -> int:
    """Insert new rows and update existing ones (matched by provider + external_id)."""
    if table not in TABLE_COLUMNS:
        raise ValueError(f"unknown table {table!r}")
    if not rows:
        return 0
    with connect() as conn, conn.cursor() as cur:
        cur.executemany(_upsert_query(table), rows)
    return len(rows)


def start_scrape_run(flow_run_id: str | None, source: str, provider: str):
    with connect() as conn:
        return conn.execute(
            "insert into public.scrape_runs (flow_run_id, source, provider) values (%s, %s, %s) returning id",
            (flow_run_id, source, provider),
        ).fetchone()[0]


def finish_scrape_run(run_id, status: str, attempts: int, fetched: int, upserted: int, error: str | None):
    with connect() as conn:
        conn.execute(
            "update public.scrape_runs set status = %s, attempts = %s, records_fetched = %s, "
            "records_upserted = %s, error = %s, finished_at = now() where id = %s",
            (status, attempts, fetched, upserted, error, run_id),
        )
