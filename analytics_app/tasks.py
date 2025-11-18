import os
import json
import asyncio
from celery import shared_task
from django.conf import settings
from django.utils import timezone
import redis as redis_sync

from .services import ExternalAPIClient, AnalyticsService, AnalyticsPayloadBuilder
from .models import AnalyticsArchive
import logging
logger = logging.getLogger("analytics_app")

REDIS_URL = getattr(settings, "REDIS_URL", os.getenv("REDIS_URL", "redis://localhost:6379/0"))
REDIS_LATEST_KEY = getattr(settings, "REDIS_LATEST_KEY", "analytics:latest")
REDIS_LATEST_TTL = getattr(settings, "REDIS_LATEST_TTL", 360)
PREWARM_LIMITS = os.getenv("PREWARM_LIMITS", "20").split(",")
PREWARM_LIMITS = [int(x) for x in PREWARM_LIMITS if x.strip().isdigit()][:5]  # limit to 5 default

@shared_task(bind=True, autoretry_for=(Exception,), retry_kwargs={"max_retries": 3, "countdown": 60})
def fetch_and_archive_task(self):
    logger.info("Celery task started: refreshing analytics...")
    try:
        redis_client = redis_sync.from_url(REDIS_URL, decode_responses=True)
    except Exception:
        redis_client = None

    async def compute_limits(limits):
        client = ExternalAPIClient()
        service = AnalyticsService(client)
        try:
            results = {}
            for lm in limits:
                try:
                    res = await service.compute_all(limit=lm)
                    results[lm] = res
                except Exception:
                    results[lm] = []
            return results
        finally:
            await client.close()

    # FIX: do NOT use asyncio.run inside Celery (especially on Windows)
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        results_map = loop.run_until_complete(compute_limits(PREWARM_LIMITS))
        loop.close()
        logger.info(f"Computed limits: {PREWARM_LIMITS}")
        logger.info("Writing results to Redis & DB archive...")

    except Exception as exc:
        try:
            self.retry(exc=exc, countdown=60, max_retries=1)
        except Exception:
            return {"status": "failed"}

    # write to redis per-limit
    for lm, data in results_map.items():
        payload = AnalyticsPayloadBuilder.build(data, lm)
        if redis_client:
            try:
                redis_client.set(
                    f"{REDIS_LATEST_KEY}:limit:{lm}",
                    json.dumps(payload),
                    ex=REDIS_LATEST_TTL,
                )
            except Exception:
                pass

    # Save archive row for primary limit
    try:
        primary = PREWARM_LIMITS[0] if PREWARM_LIMITS else 20
        payload = AnalyticsPayloadBuilder.build(results_map.get(primary, []), primary,timestamp=timezone.now())
        AnalyticsArchive.objects.create(payload=payload)
    except Exception:
        pass
    logger.info("Celery task finished successfully")
    return {"status": "ok", "limits": PREWARM_LIMITS}

