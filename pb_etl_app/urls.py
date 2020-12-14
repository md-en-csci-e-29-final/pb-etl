from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework.schemas import get_schema_view
from .views import ResultsViewSet, render_aggregation

router = DefaultRouter()

router.register(r"results", ResultsViewSet, basename="results")

schema_view = get_schema_view(title="Forecast Backtest Result API")

urlpatterns = [
    path("api/", include(router.urls)),
    path("", render_aggregation, name="aggregation"),
]
