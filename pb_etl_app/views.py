from django.shortcuts import render
from django.db.models import Avg
from rest_framework.viewsets import ModelViewSet, ViewSet
from .models import ModelResults
from .serializers import ResultsSerializer


class ResultsViewSet(ModelViewSet):
    serializer_class = ResultsSerializer
    queryset = ModelResults.objects.all()


# class DateViewSet(ModelViewSet):
#     serializer_class = DateSerializer
#     queryset = DimDate.objects.all()
#
#
# class FactViewSet(ModelViewSet):
#     serializer_class = FactSerializer
#     queryset = FactReview.objects.all()
#
#
# # Extra Credit
# # Refactoring for CC
#
#
# def get_avgs(grp_by):
#     return DimDate.objects.values(grp_by).annotate(
#         avg_count=Avg("factreview__count"),
#         avg_stars=Avg("factreview__stars"),
#         avg_useful=Avg("factreview__useful"),
#         avg_funny=Avg("factreview__funny"),
#         avg_cool=Avg("factreview__cool"),
#     )
#
#
# class ByYear(ModelViewSet):
#     serializer_class = ByYearSerializer
#
#     def get_queryset(self):
#         # Group by year, sum all facts, calculate mean using the count
#         # Note this MUST be done on the DB side, not in pandas or dask!
#         return get_avgs("date__year")
#
#
# class ByHoliday(ModelViewSet):
#     serializer_class = ByHolidaySerializer
#
#     def get_queryset(self):
#         # Group by year, sum all facts, calculate mean using the count
#         # Note this MUST be done on the DB side, not in pandas or dask!
#         return get_avgs("is_holiday")
#
#
def render_aggregation(request):
    return render(request, "pb_etl_app/index.html", {})
