from django.db import models


class ModelResults(models.Model):

    expected = models.FloatField()
    actual = models.FloatField()

    class Meta:
        db_table = "results_table"


# class DimDate(models.Model):
#
#     date = models.DateField()
#     year = models.IntegerField(default=2020)
#     is_holiday = models.BooleanField(null=True, blank=True)
#
#     class Meta:
#         db_table = "dim_date_table"
#
#
# class FactReview(models.Model):
#
#     date = models.ForeignKey(DimDate, on_delete=models.CASCADE)
#     # Integer field; number of reviews on that date
#     count = models.IntegerField()
#     # Integer, sum of review.stars for all reviews on that date
#     stars = (
#         models.IntegerField()
#     )
#     useful = models.IntegerField()
#     funny = models.IntegerField()
#     cool = models.IntegerField()
#
#     class Meta:
#         db_table = "fact_review_table"
