from django.urls import include, path
from rest_framework.routers import DefaultRouter
from rest_framework.schemas import get_schema_view
from .views import ResultsViewSet, render_aggregation
# from .views import DateViewSet, FactViewSet, ByYear, ByHoliday

router = DefaultRouter()

# Register some endpoints via "router.register(...)"
router.register(r'results', ResultsViewSet, basename='results')
# router.register(r'date', DateViewSet, basename='date')
# router.register(r'fact', FactViewSet, basename='fact')

# Extra
# router.register("by_year", ByYear, basename="by_year")
# router.register("by_holiday", ByHoliday, basename="by_holiday")

schema_view = get_schema_view(title="Forecast Backtest Result API")

urlpatterns = [
    path("api/", include(router.urls)),
    path("", render_aggregation, name="aggregation"),
]
