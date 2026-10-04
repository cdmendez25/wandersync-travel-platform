"""Sliding-window rate limiting backed by Redis (shared by all gateway replicas).

Each key keeps the timestamps of its accepted requests in a sorted set. A request
is allowed only if fewer than `limit` were accepted in the last `window` seconds.
Unlike a fixed window, bursts at a window boundary cannot double the limit.
The check-and-add runs as one Lua script, so concurrent requests cannot race past it.
"""
import secrets
import time

from redis.asyncio import Redis

from app.errors import AppError

_SLIDING_WINDOW = """
local key, now, window, limit = KEYS[1], tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
if redis.call('ZCARD', key) < limit then
  redis.call('ZADD', key, now, ARGV[4])
  redis.call('EXPIRE', key, math.ceil(window))
  return {1, 0}
end
local oldest = redis.call('ZRANGE', key, 0, 0, 'WITHSCORES')
return {0, math.ceil(tonumber(oldest[2]) + window - now)}
"""


async def hit(redis: Redis, key: str, limit: int, window: int) -> tuple[bool, int]:
    """Count one request for `key`. Returns (allowed, seconds until a slot frees up)."""
    now = time.time()
    allowed, retry_after = await redis.eval(
        _SLIDING_WINDOW, 1, f"rl:{key}", now, window, limit, f"{now}:{secrets.token_hex(4)}"
    )
    return bool(allowed), max(int(retry_after), 0)


async def enforce(redis: Redis, key: str, rule: tuple[int, int]) -> None:
    limit, window = rule
    allowed, retry_after = await hit(redis, key, limit, window)
    if not allowed:
        raise AppError(
            f"Too many attempts. Try again in {retry_after} seconds.",
            "RATE_LIMITED",
            retryAfter=retry_after,
        )
