"""Registration and login against private.users (never exposed through GraphQL).

Passwords are hashed with Argon2id (argon2-cffi defaults: t=3, m=64 MiB, p=4,
the RFC 9106 low-memory profile). Hashing runs in a worker thread so it does
not block the event loop.
"""
import asyncio
import re

import psycopg
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from psycopg_pool import AsyncConnectionPool

from app import config
from app.errors import AppError

hasher = PasswordHasher()  # Argon2id by default
_DUMMY_HASH = hasher.hash("timing-equalizer-not-a-real-password")
_EMAIL = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")


def _invalid_credentials() -> AppError:
    return AppError("Invalid email or password.", "INVALID_CREDENTIALS")


def normalize_email(email: str) -> str:
    return email.strip().lower()


def _verify(password_hash: str, password: str) -> bool:
    try:
        return hasher.verify(password_hash, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def validate_registration(email: str, password: str, full_name: str) -> None:
    if not _EMAIL.match(email) or len(email) > 254:
        raise AppError("Enter a valid email address.", "BAD_USER_INPUT", field="email")
    if not 10 <= len(password) <= 128:
        raise AppError("Password must be between 10 and 128 characters.", "BAD_USER_INPUT", field="password")
    if not 1 <= len(full_name.strip()) <= 100:
        raise AppError("Enter your full name.", "BAD_USER_INPUT", field="fullName")


async def register(pool: AsyncConnectionPool, email: str, password: str, full_name: str) -> dict:
    email = normalize_email(email)
    validate_registration(email, password, full_name)
    password_hash = await asyncio.to_thread(hasher.hash, password)
    try:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "insert into private.users (email, password_hash, full_name) values (%s, %s, %s) "
                "returning id, email, full_name, created_at",
                (email, password_hash, full_name.strip()),
            )
            return await cur.fetchone()
    except psycopg.errors.UniqueViolation:
        raise AppError("An account with this email already exists.", "EMAIL_TAKEN") from None


async def authenticate(pool: AsyncConnectionPool, email: str, password: str) -> dict:
    email = normalize_email(email)
    async with pool.connection() as conn:
        cur = await conn.execute(
            "select id, email, full_name, created_at, password_hash, "
            "coalesce(locked_until > now(), false) as locked "
            "from private.users where lower(email) = %s",
            (email,),
        )
        user = await cur.fetchone()

    if user is None:
        # Spend the same time as a real check so response time does not reveal which emails exist
        await asyncio.to_thread(_verify, _DUMMY_HASH, password)
        raise _invalid_credentials()
    if user["locked"]:
        raise AppError("Invalid email or password, or the account is temporarily locked.", "INVALID_CREDENTIALS")

    if not await asyncio.to_thread(_verify, user["password_hash"], password):
        async with pool.connection() as conn:
            await conn.execute(
                "update private.users set "
                "locked_until = case when failed_logins + 1 >= %(max)s "
                "  then now() + make_interval(mins => %(mins)s) else locked_until end, "
                "failed_logins = case when failed_logins + 1 >= %(max)s then 0 else failed_logins + 1 end "
                "where id = %(id)s",
                {"max": config.MAX_FAILED_LOGINS, "mins": config.LOCKOUT_MINUTES, "id": user["id"]},
            )
        raise _invalid_credentials()

    async with pool.connection() as conn:
        await conn.execute(
            "update private.users set failed_logins = 0, locked_until = null where id = %s", (user["id"],)
        )
        if hasher.check_needs_rehash(user["password_hash"]):  # parameters were raised since this hash was made
            new_hash = await asyncio.to_thread(hasher.hash, password)
            await conn.execute("update private.users set password_hash = %s where id = %s", (new_hash, user["id"]))

    return {k: user[k] for k in ("id", "email", "full_name", "created_at")}


async def get_user(pool: AsyncConnectionPool, user_id: str) -> dict | None:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "select id, email, full_name, created_at from private.users where id = %s", (user_id,)
        )
        return await cur.fetchone()
