import json
import os
import asyncio
from redis.asyncio import Redis
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.conf import settings
from django.utils import timezone

from .services import ExternalAPIClient, AnalyticsService, AnalyticsPayloadBuilder
from .models import AnalyticsArchive
from .serializers import AnalyticsArchiveSerializer
import logging
logger = logging.getLogger("analytics_app")

REDIS_URL = getattr(settings, "REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
REDIS_LATEST_KEY = getattr(settings, "REDIS_LATEST_KEY", "analytics:latest")
REDIS_LATEST_TTL = getattr(settings, "REDIS_LATEST_TTL", 360)

def parse_limit(request):
    try:
        v = int(request.query_params.get("limit", 50))
    except Exception:
        v = 20
    if v < 1:
        v = 1
    if v > 200:
        v = 200
    return v

class AsyncRedis:
    def __init__(self, url=REDIS_URL):
        self.url = url
    async def __aenter__(self):
        self._r = Redis.from_url(
                REDIS_URL,
                encoding="utf-8",
                decode_responses=True
            )
        return self._r
    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._r.close()
        except Exception:
            pass

async def compute_users_analytics_async(limit):
    key = f"{REDIS_LATEST_KEY}:{limit}"

    # Try Redis
    try:
        async with AsyncRedis() as redis:
            raw = await redis.get(key)
            if raw:
                return json.loads(raw)
    except:
        pass

    # Compute live
    client = ExternalAPIClient()
    service = AnalyticsService(client)

    try:
        data = await service.compute_all(limit=limit)
    finally:
        await client.close()

    payload = AnalyticsPayloadBuilder.build(data, limit)

    # Set redis
    try:
        async with AsyncRedis() as redis:
            await redis.set(key, json.dumps(payload), ex=REDIS_LATEST_TTL)
    except:
        pass

    return payload

async def _archive_async(limit):
    # Step 1 → Try latest from Redis
    key = f"{REDIS_LATEST_KEY}:{limit}"

    try:
        async with AsyncRedis() as redis:
            raw = await redis.get(key)
            if raw:
                data = json.loads(raw)["data"]
                return data
    except:
        pass

    # Step 2 → Compute live analytics
    client = ExternalAPIClient()
    service = AnalyticsService(client)
    try:
        data = await service.compute_all(limit)
    finally:
        await client.close()

    payload = AnalyticsPayloadBuilder.build(data, limit)

    try:
        async with AsyncRedis() as redis:
            await redis.set(key, json.dumps(payload), ex=REDIS_LATEST_TTL)
    except:
        pass

    return data


class UsersAnalyticsAPIView(APIView):
    def get(self, request):
        logger.info(f"Live analytics request - limit={parse_limit(request)}")
        limit = parse_limit(request)

        # Run the async function safely
        # payload = await compute_users_analytics_async(limit)
        payload = asyncio.run(compute_users_analytics_async(limit))

        return Response(payload, status=status.HTTP_200_OK)



class ArchiveCreateAPIView(APIView):
    def post(self, request):
        limit = parse_limit(request)
        #data = await _archive_async(limit)
        data = asyncio.run(_archive_async(limit))

        payload = AnalyticsPayloadBuilder.build(data, limit)

        archive = AnalyticsArchive.objects.create(
            payload=payload,
            timestamp=timezone.now()
        )
        serializer = AnalyticsArchiveSerializer(archive)
        return Response(serializer.data, status=201)



class ArchiveLatestAPIView(APIView):
    def get(self, request):
        latest = AnalyticsArchive.objects.order_by("-timestamp").first()
        if not latest:
            return Response({"detail": "No archives yet"}, status=status.HTTP_404_NOT_FOUND)
        serializer = AnalyticsArchiveSerializer(latest)
        return Response(serializer.data, status=status.HTTP_200_OK)

