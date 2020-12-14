from rest_framework.serializers import ModelSerializer, Serializer
from .models import ModelResults
from rest_framework import serializers


class ResultsSerializer(ModelSerializer):
    class Meta:
        model = ModelResults
        fields = ["expected", "actual"]

#
# class FactSerializer(ModelSerializer):
#     class Meta:
#         model = FactReview
#         fields = ["date", "count", "stars", "useful", "funny", "cool"]
#
#
# class DateSerializer(ModelSerializer):
#     class Meta:
#         model = DimDate
#         fields = ["date"]
#
#
# # Extra Credit
# # Refactoring for CC
#
#
# class ByAvgSerializer(Serializer):
#     avg_count = serializers.IntegerField(read_only=True)
#     avg_stars = serializers.IntegerField(read_only=True)
#     avg_useful = serializers.IntegerField(read_only=True)
#     avg_funny = serializers.IntegerField(read_only=True)
#     avg_cool = serializers.IntegerField(read_only=True)
#
#
# class ByYearSerializer(ByAvgSerializer):
#     date__year = serializers.IntegerField(read_only=True)
#
#
# class ByHolidaySerializer(ByAvgSerializer):
#     # Return the *averages*, not the sum!
#     is_holiday = serializers.BooleanField(read_only=True)
