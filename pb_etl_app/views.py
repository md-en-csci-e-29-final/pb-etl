from django.shortcuts import render
from rest_framework.viewsets import ModelViewSet
from .models import ModelResults
from .serializers import ResultsSerializer


class ResultsViewSet(ModelViewSet):
    serializer_class = ResultsSerializer
    queryset = ModelResults.objects.all()


def render_aggregation(request):
    return render(request, "pb_etl_app/index.html", {})
