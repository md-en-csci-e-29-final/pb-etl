from luigi import build
from django.core.management import BaseCommand
from pb_etl.tasks import FinalResults
from ...models import ModelResults


class Command(BaseCommand):

    help = "Load Forecast Results"

    def handle(self, *args, **options):

        # Run the model
        fin_rslt = FinalResults()

        build(
            [fin_rslt],
            local_scheduler=True,
        )

        # compute backtest results
        bcktst = fin_rslt.input().read_dask()
        ln = bcktst.count().compute()

        # Print final (backtesting) results
        print("=== FINAL RESULTS ===")
        print("=== Real Deletion Rate (TARGET) ===")
        print("=== Forecasted deletion rate (Y_hat) ===")
        res_df = bcktst.sum().compute() / ln
        print(res_df)
        print("==============================")

        # Storing data into Django database
        result_records = [
            ModelResults(expected=res_df["Y_hat"], actual=res_df["TARGET"])
        ]

        ModelResults.objects.all().delete()
        ModelResults.objects.bulk_create(result_records)
