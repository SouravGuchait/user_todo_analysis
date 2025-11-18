from django.urls import path
from .views import UsersAnalyticsAPIView, ArchiveCreateAPIView, ArchiveLatestAPIView

urlpatterns = [
    path("api/v1/users/analytics", UsersAnalyticsAPIView.as_view(), name="users_analytics"),
    path("api/v1/users/archive", ArchiveCreateAPIView.as_view(), name="archive_create"),
    path("api/v1/users/archive/latest", ArchiveLatestAPIView.as_view(), name="archive_latest"),
]
