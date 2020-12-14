from django.db import models


class ModelResults(models.Model):
    """
    Model that reflects predicting results
    """

    expected = models.FloatField()
    actual = models.FloatField()

    class Meta:
        db_table = "results_table"
