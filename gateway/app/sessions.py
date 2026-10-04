"""Server-side sessions in Redis.

The browser only holds a random, opaque session id in an HttpOnly cookie.
Every visitor gets an anonymous session first; on login (and logout) that id
is destroyed and a brand-new one is issued. A session id an attacker planted
before login is therefore useless afterwards (Session Fixation mitigation).
"""
import json
import re
import secrets
import time
from dataclasses import dataclass

from redis.asyncio import Redis

from app import config

_SID_FORMAT = re.compile(r"^[A-Za-z0-9_-]{43}$")  # token_urlsafe(32)


@dataclass
class Session:
    id: str
    user_id: str | None
    created_at: float


class SessionStore:
    def __init__(self, redis: Redis):
        self.redis = redis

    @staticmethod
    def _key(sid: str) -> str:
        return f"session:{sid}"

    async def load(self, sid: str | None) -> Session | None:
        if not sid or not _SID_FORMAT.match(sid):
            return None
        raw = await self.redis.get(self._key(sid))
        if raw is None:
            return None
        data = json.loads(raw)
        if time.time() - data["created_at"] > config.SESSION_ABSOLUTE_SECONDS:
            await self.destroy(sid)
            return None
        await self.redis.expire(self._key(sid), config.SESSION_IDLE_SECONDS)  # sliding idle timeout
        return Session(id=sid, user_id=data["user_id"], created_at=data["created_at"])

    async def create(self, user_id: str | None = None) -> Session:
        session = Session(id=secrets.token_urlsafe(32), user_id=user_id, created_at=time.time())
        await self.redis.set(
            self._key(session.id),
            json.dumps({"user_id": user_id, "created_at": session.created_at}),
            ex=config.SESSION_IDLE_SECONDS,
        )
        return session

    async def destroy(self, sid: str) -> None:
        await self.redis.delete(self._key(sid))

    async def regenerate(self, old: Session, user_id: str | None) -> Session:
        """Throw away the old id and issue a new one (called on login and logout)."""
        await self.destroy(old.id)
        return await self.create(user_id)
