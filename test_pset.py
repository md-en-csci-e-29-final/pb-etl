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















from pb_etl_app.models import ModelResults
from datetime import date

import json
from django.test import TestCase
from rest_framework import status
from pb_etl_app.serializers import ResultsSerializer



class ModelTestCase(TestCase):
    def setUp(self):
        ModelResults.objects.create(expected=0.5, actual=0.6)

    def test_ModelTestCase_django_case(self):
        """
        Test users being added to the database correctly + simple operations
        """
        results = ModelResults.objects.get()

        self.assertEqual(results.expected, 0.5)
        self.assertEqual(results.actual, 0.6)

    def test_get_views(self):
        """
        Test view urls are correct
        """
        self.assertEqual(self.client.get("/").status_code, status.HTTP_200_OK)
        self.assertEqual(self.client.get("/the_app/").status_code, status.HTTP_200_OK)
        self.assertEqual(
            self.client.get("/fake").status_code, status.HTTP_404_NOT_FOUND
        )

    def test_http(self):
        """
        Test for CRUD operations
        """
        response = self.client.get("/the_app/api/results/")
        # response.re
        self.assertEqual(response.status_code, status.HTTP_200_OK)
        response = json.loads(response.content.decode("utf-8"))
        self.assertEqual(response["results"][0]["expected"], 0.5)
        self.assertEqual(response["results"][0]["actual"], 0.6)


class SerializerTestCase(TestCase):
    def setUp(self):
        self.res = ModelResults.objects.create(expected=0.5, actual=0.6)

    def test_serializer(self):
        """
        Test serialization is correct (and should test deserialization, etc)
        """
        ps = ResultsSerializer(self.res)
        print(ps.data)
        self.assertEqual(ps.data["expected"], 0.5)
        self.assertEqual(ps.data["actual"], 0.6)


















