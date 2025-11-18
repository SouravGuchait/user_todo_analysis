import os
import asyncio
from urllib.parse import urlparse
import aiohttp
from redis.asyncio import Redis
from django.conf import settings
import logging
logger = logging.getLogger("analytics_app")

# Config from settings / env
EXTERNAL_TIMEOUT = getattr(settings, "EXTERNAL_API_TIMEOUT", int(os.getenv("EXTERNAL_API_TIMEOUT", "10")))
CACHE_TTL = getattr(settings, "EXTERNAL_CACHE_TTL", int(os.getenv("EXTERNAL_CACHE_TTL", "60")))
MAX_CONCURRENT = getattr(settings, "MAX_CONCURRENT_REQUESTS", int(os.getenv("MAX_CONCURRENT_REQUESTS", "20")))
REDIS_URL = getattr(settings, "REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))

BASE = "https://jsonplaceholder.typicode.com"
USERS_URL = f"{BASE}/users"
POSTS_URL = f"{BASE}/posts"
TODOS_URL = f"{BASE}/todos"
ALBUMS_URL = f"{BASE}/albums"

# redis_client = Redis(
#     host=os.getenv("REDIS_HOST", "localhost"),
#     port=int(os.getenv("REDIS_PORT", 6379)),
#     db=0,
#     decode_responses=True
# )

# class CacheService:
#     def __init__(self):
#         self.client = redis_client

#     async def get(self, key):
#         return await self.client.get(key)

#     async def set(self, key, value, ttl=60):
#         await self.client.set(key, value, ex=ttl)

class ExternalAPIClient:
    """
    Async client with aiohttp for external calls and aioredis for caching (TTL = CACHE_TTL seconds).
    """

    def __init__(self):
        self._redis = None
        self._session = None
        self._sem = asyncio.Semaphore(MAX_CONCURRENT)

    async def _ensure(self):
        if self._redis is None:
            self._redis = Redis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=EXTERNAL_TIMEOUT)
            self._session = aiohttp.ClientSession(timeout=timeout)

    async def close(self):
        if self._session:
            await self._session.close()
        if self._redis:
            await self._redis.close()

    async def _get_cached(self, key):
        await self._ensure()
        raw = await self._redis.get(key)
        if raw:
            import json
            try:
                return json.loads(raw)
            except Exception:
                return None
        return None

    async def _set_cached(self, key, value, ttl=CACHE_TTL):
        await self._ensure()
        import json
        await self._redis.set(key, json.dumps(value), ex=ttl)

    async def _fetch(self, url, params=None):
        logger.info(f"Fetching from external API: {url} params={params}")
        await self._ensure()
        key = f"external:{url}:{'' if not params else '&'.join(f'{k}={v}' for k,v in sorted((params or {}).items()))}"
        cached = await self._get_cached(key)
        if cached is not None:
            return cached

        async with self._sem:
            try:
                async with self._session.get(url, params=params) as resp:
                    if resp.status != 200:
                        return []
                    data = await resp.json()
                    await self._set_cached(key, data, ttl=CACHE_TTL)
                    return data
            except asyncio.TimeoutError:
                return []
            except Exception:
                return []

    async def get_users(self):
        return await self._fetch(USERS_URL)

    async def get_posts_for_user(self, user_id):
        return await self._fetch(POSTS_URL, params={"userId": user_id})

    async def get_todos_for_user(self, user_id):
        return await self._fetch(TODOS_URL, params={"userId": user_id})

    async def get_albums_for_user(self, user_id):
        return await self._fetch(ALBUMS_URL, params={"userId": user_id})


class AnalyticsService:
    """
    Class-based analytics computations.
    """

    @staticmethod
    def extract_domain(website: str) -> str:
        if not website:
            return ""
        if not website.startswith("http"):
            website = f"http://{website}"
        try:
            parsed = urlparse(website)
            return parsed.netloc
        except Exception:
            return ""

    @staticmethod
    def sentiment_score(posts_titles, album_titles):
        combined = "".join(posts_titles) + "".join(album_titles)
        return len(combined) % 7

    def __init__(self, client: ExternalAPIClient):
        self.client = client

    async def compute_for_user(self, user):
        uid = user.get("id")
        posts_coro = self.client.get_posts_for_user(uid)
        todos_coro = self.client.get_todos_for_user(uid)
        albums_coro = self.client.get_albums_for_user(uid)

        posts, todos, albums = await asyncio.gather(posts_coro, todos_coro, albums_coro)
        posts = posts or []
        todos = todos or []
        albums = albums or []

        total_posts = len(posts)
        completed_todos = sum(1 for t in todos if t.get("completed") in (True, "true", 1))
        total_todos = len(todos)
        completed_todo_rate = round((completed_todos / total_todos) * 100, 1) if total_todos else 0.0

        titles = [p.get("title", "") for p in posts]
        total_title_chars = sum(len(t) for t in titles)
        avg_post_title_length = round((total_title_chars / total_posts), 1) if total_posts else 0.0

        album_titles = [a.get("title", "") for a in albums]
        albums_count = len(albums)

        company_domain = self.extract_domain(user.get("website", ""))

        sentiment = self.sentiment_score(titles, album_titles)

        return {
            "id": uid,
            "name": user.get("name"),
            "email": user.get("email"),
            "total_posts": total_posts,
            "completed_todo_rate": completed_todo_rate,
            "avg_post_title_length": avg_post_title_length,
            "albums_count": albums_count,
            "company_domain": company_domain,
            "sentiment_score": sentiment,
        }

    async def compute_all(self, limit=20):
        users = await self.client.get_users() or []
        users = users[:limit]
        tasks = [self.compute_for_user(u) for u in users]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        final = []
        for r in results:
            if isinstance(r, Exception):
                final.append({"error": "failed"})
            else:
                final.append(r)
        return final


class AnalyticsPayloadBuilder:
    @staticmethod
    def build(data, limit):
        from django.utils import timezone
        return {"meta": {"limit": limit, "generated_at": timezone.now().isoformat()}, "data": data}
