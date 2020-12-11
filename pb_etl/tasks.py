from luigi import ExternalTask, BoolParameter, Task, Parameter
import pb_etl.luigi.dask.target as lt
from pb_etl.luigi.task import Requirement, Requires, TargetOutput, SaltedOutput
import os

import tensorflow as tf
from tensorflow import feature_column
from tensorflow.keras import layers
from sklearn.model_selection import train_test_split
import pandas as pd

attr_type = {
    "TRANSACTION_ID": "int64",
    "TLD": "object",
    "REN": "int64",
    "REGISTRAR_NAME": "object",
    "GL_CODE_NAME": "object",
    "COUNTRY": "object",
    "DOMAIN_LENGTH": "int64",
    "HISTORY": "object",
    "TRANSFERS": "int64",
    "TERM_LENGTH": "object",
    "RES30": "int64",
    "RESTORES": "int64",
    "REREG": "object",
    "QTILE": "object",
    "HD": "object",
    "NS_V0": "float64",
    "NS_V1": "float64",
    "NS_V2": "float64",
    "TARGET": "int64",
}
ts_type = {"TRANSACTION_ID": "int64", "TRAFFIC_SCORE": "float64"}

comb_type = {
    "TRANSACTION_ID": "int64",
    "TLD": "object",
    "REN": "int64",
    "REGISTRAR_NAME": "object",
    "GL_CODE_NAME": "object",
    "COUNTRY": "object",
    "DOMAIN_LENGTH": "int64",
    "HISTORY": "object",
    "TRANSFERS": "int64",
    "TERM_LENGTH": "object",
    "RES30": "int64",
    "RESTORES": "int64",
    "REREG": "object",
    "QTILE": "object",
    "HD": "object",
    "NS_V0": "float64",
    "NS_V1": "float64",
    "NS_V2": "float64",
    "TARGET": "int64",
    "TRAFFIC_SCORE": "float64"
}

s3_source = "s3://md-en-csci-e-29-final/"


class ExtData(ExternalTask):
    __version__ = "0.0.0"

    data_source = ""

    def output(self):
        pth = os.getenv(
            "PSET5_PATH",
            default=s3_source + self.data_source,
        )
        return lt.CSVTarget(pth, storage_options=dict(requester_pays=True), flag=None)


class TrnAttr(ExtData):
    data_source = "train/attr/"


class TrnTscore(ExtData):
    data_source = "train/tscore/"


class TstAttr(ExtData):
    data_source = "test/attr/"


class TstTscore(ExtData):
    data_source = "test/tscore/"


class LoadData(Task):
    __version__ = "0.0.0"

    # subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    S3TrnAttr = Requirement(TrnAttr)
    S3TrnTscore = Requirement(TrnTscore)

    requires = Requires()

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./traindata")

    def run(self):
        trn_atr = self.input()["S3TrnAttr"].read_dask(dtype=attr_type).set_index("TRANSACTION_ID")
        trn_ts = self.input()["S3TrnTscore"].read_dask(dtype=ts_type).set_index("TRANSACTION_ID")

        trn = trn_atr.join(trn_ts)

        self.output().write_dask(trn, compression="gzip")

class LoadTest(Task):
    __version__ = "0.0.0"

    # subset = BoolParameter(default=True)

    # Output should be a local ParquetTarget in ./data, ideally a salted output,
    # and with the subset parameter either reflected via salted output or
    # as part of the directory structure

    S3TstAttr = Requirement(TstAttr)
    S3TstTscore = Requirement(TstTscore)

    requires = Requires()

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./testdata")

    def run(self):
        tst_atr = self.input()["S3TstAttr"].read_dask(dtype=attr_type).set_index("TRANSACTION_ID")
        tst_ts = self.input()["S3TstTscore"].read_dask(dtype=ts_type).set_index("TRANSACTION_ID")

        tst = tst_atr.join(tst_ts)

        self.output().write_dask(tst, compression="gzip")


# class BySomething(Task):
#     __version__ = "0.0.0"
#
#     ByWhat = Parameter()
#     subset = BoolParameter(default=True)
#
#     # Be sure to read from CleanedReviews locally
#
#     TheData = Requirement(LoadData)
#     requires = Requires()
#
#     output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./yelp_result")
#     # output = TargetOutput(target_class=lt.ParquetTarget,target_path='./yelp_result')
#
#     def run(self):
#         ddf = self.input().read_dask()
#         res = ddf[["lenn", self.ByWhat]].groupby(self.ByWhat).mean()
#         self.output().write_dask(res, compression="gzip")
#
#         self.print_results()
#
#     def print_results(self):
#         print(self.output().read_dask().compute())


class FitModel(Task):
    __version__ = "0.0.0"
    TheData = Requirement(LoadData)
    requires = Requires()

    def df_to_dataset(dataframe, shuffle=True, batch_size=32):
        dataframe = dataframe.copy()
        labels = dataframe.pop('target')
        ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))
        if shuffle:
            ds = ds.shuffle(buffer_size=len(dataframe))
        ds = ds.batch(batch_size)
        return ds

    def run(self):

        # trn = pd.read_parquet(self.input())
        # print("Training: " + str(len(trn)))
        trn_comb = self.input().read_dask(dtype=comb_type)#.set_index("TRANSACTION_ID")
        print(len(trn_comb))

        train, val = train_test_split(trn_comb, test_size=0.2)

        comb_type = {
            "TRANSACTION_ID": "int64",
            "TLD": "object",
            "REN": "int64",
            "REGISTRAR_NAME": "object",
            "GL_CODE_NAME": "object",
            "COUNTRY": "object",
            "DOMAIN_LENGTH": "int64",
            "HISTORY": "object",
            "TRANSFERS": "int64",
            "TERM_LENGTH": "object",
            "RES30": "int64",
            "RESTORES": "int64",
            "REREG": "object",
            "QTILE": "object",
            "HD": "object",
            "NS_V0": "float64",
            "NS_V1": "float64",
            "NS_V2": "float64",
            "TARGET": "int64",
            "TRAFFIC_SCORE": "float64"
        }



        num_col = ['TRANSACTION_ID', 'REN','DOMAIN_LENGTH', 'TRANSFERS', 'RES30', 'RESTORES', 'NS_V0', 'NS_V1', 'NS_V2', 'TARGET', 'TRAFFIC_SCORE']
        cat_col = ['CR_QTILE_ADJ', 'CR_QTILE', 'CR_WEEK_DAY', 'CR_HOLIDAY', 'DOMAIN_TYPE', 'GL_CODE_NAME', 'COUNTRY',
                   'RECENT_HIST', 'RECENT_RESTORED',
                   'EVER_TRANSFERRED', 'FAMILY']

        feature_columns = []

        for header in num_col:
            feature_columns.append(feature_column.numeric_column(header))

        for header in cat_col:
            categorical_column = feature_column.categorical_column_with_vocabulary_list(
                header, trn[header].unique())

            indicator_column = feature_column.indicator_column(categorical_column)

            feature_columns.append(indicator_column)



        tst_dd = tf.data.Dataset.from_tensor_slices(dict(tst))
        tst_dd = tst_dd.batch(32)


        tf.keras.backend.set_floatx('float64')

        feature_layer = tf.keras.layers.DenseFeatures(feature_columns)

        model = tf.keras.Sequential([
            feature_layer,
            layers.Dense(1024, activation='relu'),
            layers.Dropout(.2),
            layers.Dense(512, activation='relu'),
            layers.Dropout(.2),
            layers.Dense(256, activation='relu'),
            layers.Dropout(.2),
            layers.Dense(128, activation='relu'),
            layers.Dropout(.2),
            layers.Dense(64, activation='relu'),
            layers.Dropout(.1),
            layers.Dense(1, activation='sigmoid')
        ])

        model.compile(optimizer='adam',
                      loss=tf.keras.losses.BinaryCrossentropy(from_logits=False),
                      metrics=['accuracy'])



