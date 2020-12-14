from rest_framework.serializers import ModelSerializer
from .models import ModelResults


class ResultsSerializer(ModelSerializer):
    """
    Serializer, for result model (expected: forecasted values)
    """

    class Meta:
        model = ModelResults
        fields = ["expected", "actual"]
