from rest_framework import serializers
from .models import AnalyticsArchive

class AnalyticsArchiveSerializer(serializers.ModelSerializer):
    class Meta:
        model = AnalyticsArchive
        fields = ["id", "timestamp", "payload"]
