from luigi import build
from .tasks import LoadData, LoadTest, FitModel
import argparse


def main():
    parser = argparse.ArgumentParser(description="Full Subset")

    parser.add_argument("--full", action="store_false")

    args = parser.parse_args()
    # LoadData(), LoadTest(),
    build(
        [FitModel()],
        local_scheduler=True,
    )