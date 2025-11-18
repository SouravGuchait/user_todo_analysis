import os
from celery import Celery

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "analytics_projects.settings")

app = Celery("analytics_projects")

app.config_from_object("django.conf:settings", namespace="CELERY")
app.autodiscover_tasks()
