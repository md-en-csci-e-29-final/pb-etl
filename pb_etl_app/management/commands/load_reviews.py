import holidays
import pandas as pd
from luigi import build
from django.core.management import BaseCommand
from django.db import transaction
from ...models import DimDate, FactReview
from ext_modules.pset_5.tasks import CleanedReviews

### Adding Pset 5 as submodule
# Since pset_5 considered "stable" - it was already submitted


class Command(BaseCommand):

    help = "Load review facts"

    us_holidays = holidays.UnitedStates()

    def add_arguments(self, parser):
        parser.add_argument("-f", "--full", action="store_false")

    def handle(self, *args, **options):
        """Populates the DB with review aggregations"""
        # Load the data
        # Calculate daily aggregations
        # Store results into FactReview

        # We need only to download data - reusing Pset 5 logic here
        build(
            [CleanedReviews(subset=options["full"])],
            local_scheduler=True,
        )

        # Output of the Cleaned reviews is what we need to consume (either full or partial depending on full flag)
        input_path = CleanedReviews(subset=options["full"]).output().path

        # Everything is in here
        data_set = pd.read_parquet(input_path)

        # Extracting Dates only
        dates_df = pd.DataFrame(data_set.Date.unique(), columns=["Date"]).sort_values(
            by=["Date"]
        )

        # Extracting year from date and appending column to a dataframe
        dates_df["year"] = pd.DatetimeIndex(dates_df["Date"]).year

        # for each date checking if it is us holiday returns None or Name of the holiday
        # Need True or False
        dates_df["is_holiday"] = dates_df["Date"].apply(self.us_holidays.get).notnull()

        # Because this is a "Fact" table, we want all the facts to be summary statistics,
        # ie the sum of all properties within that group.
        facts_df = data_set.groupby("Date").agg(
            {
                "stars": "sum",
                "useful": "sum",
                "funny": "sum",
                "cool": "sum",
                "Date": "count",
            }
        )

        # Need this to rename last Date column
        facts_df.columns = ["stars", "useful", "funny", "cool", "count"]

        # Is there another way of loading dataframe?
        # Converting dataframe to a list of records
        # each record is a dictionary
        date_records = dates_df.to_dict("records")

        # to be able to bulk load
        date_dim_records = [
            DimDate(
                date=record["Date"],
                year=record["year"],
                is_holiday=record["is_holiday"],
            )
            for record in date_records
        ]

        # Cleaning tables before loading
        DimDate.objects.all().delete()
        FactReview.objects.all().delete()

        with transaction.atomic():
            DimDate.objects.bulk_create(date_dim_records)

            # for FK need special handling
            date_dim_fk = pd.DataFrame(list(DimDate.objects.all().values())).set_index(
                "date"
            )

            facts = date_dim_fk.join(facts_df)

            fact_records = facts.to_dict("records")

            bulk_fact_recs = [
                FactReview(
                    count=record["count"],
                    stars=record["stars"],
                    useful=record["useful"],
                    funny=record["funny"],
                    cool=record["cool"],
                    date_id=record["id"],
                )
                for record in fact_records
            ]

            FactReview.objects.bulk_create(bulk_fact_recs)
