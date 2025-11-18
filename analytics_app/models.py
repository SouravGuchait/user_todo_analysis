from django.db import models
from django.utils import timezone

class AnalyticsArchive(models.Model):
    timestamp = models.DateTimeField(default=timezone.now, db_index=True)
    payload = models.JSONField()

    class Meta:
        db_table = "analytics_archive"
        ordering = ["-timestamp"]

    def __str__(self):
        return f"AnalyticsArchive({self.timestamp.isoformat()})"
