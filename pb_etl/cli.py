from luigi import build
from .tasks import LoadData, LoadTest, FitNNModel, NNPredict, BackTest, NormalizationDenominators, FinalResults
import argparse


def main():
    parser = argparse.ArgumentParser(description="Full Subset")

    parser.add_argument("--full", action="store_false")

    args = parser.parse_args()
    # LoadData(), LoadTest(),
    build(
        [FinalResults()],
        local_scheduler=True,
    )