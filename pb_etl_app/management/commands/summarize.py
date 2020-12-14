from django.core.management import BaseCommand
from django.db.models import Sum

from ...models import DimDate, FactReview


class Command(BaseCommand):
    help = "Summarize review facts"

    def handle(self, *args, **options):
        print("Dimensions: {}".format(DimDate.objects.all().count()))
        print("Facts: {}".format(FactReview.objects.all().count()))
        print("Total Reviews: {}".format(FactReview.objects.aggregate(Sum("count"))))
