#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `pb_etl` package."""

from unittest import TestCase
from moto import mock_s3
import boto3
from tempfile import TemporaryDirectory
import pandas as pd
import dask.dataframe as dd
from luigi import build
import os

from pb_etl.tasks import LoadData, LoadTest, FitNNModel, NNPredict, BackTest, NormalizationDenominators, FinalResults

class FakeFileFailure(IOError):
    pass


class TestsStub(TestCase):
    def test_1(self):
        pass


def fake_data():
    """
    Create fake data to be used for analysis
    :return: DataFrame
    """

    pb_etl_fake_train = {
        "TRANSACTION_ID": [109785, 109784, 109783],
        "TLD": ["TLD1", "TLD1", "TLD1"],
        "REN": [8, 8, 8],
        "REGISTRAR_NAME": ["ACC 012", "ACC 012", "ACC 012"],
        "GL_CODE_NAME": ["GL2", "GL2", "GL2"],
        "COUNTRY": ["CNTR 04", "CNTR 04", "CNTR 04"],
        "DOMAIN_LENGTH": [11, 17, 14],
        "HISTORY": ["/AR:1/AR:1/TR:1", "/AR:1/AR:1/TR:1", "/AR:1/AR:1/TR:1"],
        "TRANSFERS": [2, 2, 2],
        "TERM_LENGTH": ["TL01", "TL01", "TL01"],
        "RES30": [0, 0, 0],
        "RESTORES": [0, 0, 0],
        "REREG": ["Y", "Y", "Y"],
        "QTILE": ["Q2", "Q2", "Q2"],
        "HD": ["A", "A", "A"],
        "NS_V0": [0.590681846, 0.590681846, 0.590681846],
        "NS_V1": [0.791507201, 0.791507201, 0.791507201],
        "NS_V2": [0.693827386, 0.693827386, 0.693827386],
        "TARGET": [0, 0, 0],
    }

    pb_etl_fake_train_ts = {
        "TRANSACTION_ID": [109785, 109784, 109783],
        "TRAFFIC_SCORE": [
            0.0000417455279238821,
            0.0000449483234402741,
            0.0000718081312936524,
        ],
    }

    pb_etl_fake_test = {
        "TRANSACTION_ID": [275452, 275451, 275450],
        "TLD": ["TLD1", "TLD1", "TLD1"],
        "REN": [0, 2, 0],
        "REGISTRAR_NAME": ["ACC 012", "ACC 012", "ACC 012"],
        "GL_CODE_NAME": ["GL2", "GL2", "GL2"],
        "COUNTRY": ["CNTR 04", "CNTR 04", "CNTR 04"],
        "DOMAIN_LENGTH": [11, 17, 14],
        "HISTORY": ["/AR:1/AR:1/TR:1", "/AR:1/AR:1/TR:1", "/AR:1/AR:1/TR:1"],
        "TRANSFERS": [2, 2, 2],
        "TERM_LENGTH": ["TL01", "TL01", "TL01"],
        "RES30": [0, 0, 0],
        "RESTORES": [0, 0, 0],
        "REREG": ["Y", "Y", "Y"],
        "QTILE": ["Q2", "Q2", "Q2"],
        "HD": ["A", "A", "A"],
        "NS_V0": [0.590681846, 0.590681846, 0.590681846],
        "NS_V1": [0.791507201, 0.791507201, 0.791507201],
        "NS_V2": [0.693827386, 0.693827386, 0.693827386],
    }

    pb_etl_fake_test_ts = {
        "TRANSACTION_ID": [275452, 275451, 275450],
        "TRAFFIC_SCORE": [
            0.0000417455279238821,
            0.0000449483234402741,
            0.0000718081312936524,
        ],
    }

    pb_etl_fake_results = {
        "TRANSACTION_ID": [275452, 275451, 275450],
        "TARGET": [0, 0, 0],
    }

    fake_train_df = pd.DataFrame(pb_etl_fake_train)#.set_index("TRANSACTION_ID")
    # fake_train_daskdf = dd.from_pandas(fake_train_df, npartitions=1)

    fake_train_ts_df = pd.DataFrame(pb_etl_fake_train_ts)#.set_index("TRANSACTION_ID")
    # fake_train_ts_daskdf = dd.from_pandas(fake_train_ts_df, npartitions=1)

    fake_test_df = pd.DataFrame(pb_etl_fake_test)#.set_index("TRANSACTION_ID")
    # fake_test_daskdf = dd.from_pandas(fake_test_df, npartitions=1)

    fake_test_ts_df = pd.DataFrame(pb_etl_fake_test_ts)#.set_index("TRANSACTION_ID")
    # fake_test_ts_daskdf = dd.from_pandas(fake_test_ts_df, npartitions=1)

    fake_results_df = pd.DataFrame(pb_etl_fake_results)#.set_index("TRANSACTION_ID")
    # fake_results_daskdf = dd.from_pandas(fake_results_df, npartitions=1)

    return {
        "train/attr/trn": fake_train_df,
        "train/tscore/trn_ts": fake_train_ts_df,
        "test/attr/tst": fake_test_df,
        "test/tscore/tst_ts": fake_test_ts_df,
        "results/rslt": fake_results_df,
    }

    # return {
    #     "train/attr/trn": fake_train_daskdf,
    #     "train/tscore/trn_ts": fake_train_ts_daskdf,
    #     "test/attr/tst": fake_test_daskdf,
    #     "test/tscore/tst_ts": fake_test_ts_daskdf,
    #     "results/rslt": fake_results_daskdf,
    # }



class LuigiTester(TestCase):
    def test_by_build(self):

        # Now creating bunch of csv files
        curr_dir = os.getcwd()
        print(curr_dir)

        with TemporaryDirectory() as temp_dir:
            os.chdir(temp_dir)
            os.mkdir("data")
            fake_reviews = fake_data()
            for key in fake_reviews:

                f = os.path.join(temp_dir, key + "_0.csv")
                os.makedirs(os.path.dirname(f))

                print(fake_reviews[key])

                fake_reviews[key].to_csv(f, index=False)
                os.system("cat " + f)

            # because of an issue with moto
            # substituting S3 path with local path via environment variable
            os.environ["FINAL_PROJ_BUCKET"] = temp_dir + "/"
            print(os.listdir(temp_dir))
            print(os.listdir(temp_dir+"/results"))
            print(os.listdir(temp_dir + "/train/attr"))

            # print("==============")
            # print(os.getcwd())

            l_res = LoadData()

            build(
                [l_res],
                local_scheduler=True
            )
            #print(os.listdir(temp_dir))
            #print("l_res.output :" + l_res.output().path)
            #print(os.listdir(l_res.output().path))
            print("+++++++++++++++++++++++++++++++++")
            print(l_res.input())
            #print(os.listdir(l_res.input()))
            self.assertTrue(l_res.output().exists())



            # raise ValueError

            t_res = LoadTest()

            build(
                [t_res],
                local_scheduler=True
            )
            print(os.listdir(temp_dir))

            print(t_res.output().path)
            print(t_res.output().exists())

            print(os.listdir(t_res.output().path))
            self.assertTrue(t_res.output().exists())

            norm_res = NormalizationDenominators()

            build(
                [norm_res],
                local_scheduler=True
            )
            self.assertTrue(norm_res.output().exists())

            fit_res = FitNNModel()

            build(
                [fit_res],
                local_scheduler=True
            )
            self.assertTrue(fit_res.output().exists())

            norm_res = NormalizationDenominators()

            build(
                [norm_res],
                local_scheduler=True
            )
            self.assertTrue(norm_res.output().exists())

            pred_res = NNPredict()

            build(
                [pred_res],
                local_scheduler=True
            )
            self.assertTrue(pred_res.output().exists())

            bck_test_res = BackTest()

            build(
                [bck_test_res],
                local_scheduler=True
            )
            self.assertTrue(bck_test_res.output().exists())



            os.chdir(curr_dir)

    # def tearDown(self):
    #
    #     files = glob.glob('./local_*/*', recursive=True)
    #     for f in files:
    #         os.remove(f)
    #
    #     dirs = glob.glob('./local_*')
    #     for d in dirs:
    #         os.removedirs(d)






























#
# @mock_s3
# class TestTask(TestCase):
#     """
#     Set up data, upload fake data to S3, and test run all tasks
#     """
#
#     bucket = "mock_bucket"
#
#     def setUp(self):
#         # Create fake bucket in S3
#         self.client_s3 = boto3.client("s3", region_name="us-east-1")
#         self.client_s3.create_bucket(Bucket=self.bucket)
#
#         with tempfile.TemporaryDirectory() as tempdir:
#         # tempdir = "deleteme"
#             fake_reviews = fake_data()
#             for key in fake_reviews:
#                 f = os.path.join(tempdir, key + "/*.csv")
#                 fake_reviews[key].to_csv(f)
#                 dir_name = os.path.join(tempdir, key)
#                 for the_file in os.listdir(dir_name):
#                     self.client_s3.upload_file(os.path.join(dir_name, the_file), self.bucket, key + ".csv")
#
#             #     print(os.listdir(tempdir))
#             #     print(self.client_s3.list_objects(Bucket=self.bucket))
#             #
#             #
#             #
#             # # print(pd.read_csv("s3://" + self.bucket + "/train/attr/trn.csv").head())
#             #
#             #
#             # s3 = boto3.resource('s3')
#             # bucket = s3.Bucket('mock_bucket')
#             # # Iterates through all the objects, doing the pagination for you. Each obj
#             # # is an ObjectSummary, so it doesn't contain the body. You'll need to call
#             # # get to get the whole body.
#             # for obj in bucket.objects.all():
#             #     key = obj.key
#             #     print(key)
#             #     body = obj.get()['Body'].read()
#             #     print(body)
#             #
#             #
#             # raise ValueError
#             #
#             # fake_reviews[0].to_csv(fake_path)
#             # for f in os.listdir(tempdir):
#             #     self.client_s3.upload_file(
#             #         os.path.join(tempdir, f), self.bucket, tempdir
#             #     )
#
#     def test_1(self):
#         # raise ValueError
#
#         class MockFinalResults(FinalResults):
#             """
#             Create fake S3 Root that contains yelp reviews data
#             """
#
#             def test_yelp_review(self):
#                 mock_final_results = MockFinalResults()
#                 build([mock_final_results], local_scheduler=True)
#                 self.assertTrue(mock_final_results.output().exists())
#
#     # def test_cleaned_review(self):
#     #     """
#     #     Run MockCleanedReview task to return parquet output
#     #     :return: parquet file
#     #     """
#     #     with tempfile.TemporaryDirectory() as tmpdir:
#     #         target_path = os.path.join(tmpdir, "CleanedReviews/")
#     #
#     #         class MockCleanedReviews(CleanedReviews):
#     #             output = TargetOutput(
#     #                 file_pattern=target_path, target_class=ParquetTarget, ext=""
#     #             )
#     #
#     #         mock_cleaned_review = MockCleanedReviews(subset=False)
#     #         build([mock_cleaned_review], local_scheduler=True)
