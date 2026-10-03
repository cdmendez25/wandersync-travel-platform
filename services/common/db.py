"""Shared Supabase connection pool for the microservices.

Each service only touches its own tables; this module just centralizes how
the connection is opened (session pooler, TLS, dict rows).
"""
import os

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


def create_pool() -> ConnectionPool:
    return ConnectionPool(
        os.environ["DATABASE_URL"],
        min_size=1,
        # Supabase free tier has few pooler slots; keep each service small
        max_size=int(os.getenv("DB_POOL_MAX", "2")),
        kwargs={"prepare_threshold": None, "sslmode": "require", "row_factory": dict_row},
        check=ConnectionPool.check_connection,
        open=True,
    )
