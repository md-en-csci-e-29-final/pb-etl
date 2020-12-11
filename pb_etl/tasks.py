import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # or any {'0', '1', '2'}

import tensorflow as tf
from tensorflow.keras import layers
from luigi import ExternalTask, BoolParameter, Task, Parameter
import pb_etl.luigi.dask.target as lt
from pb_etl.luigi.task import Requirement, Requires, TargetOutput, SaltedOutput

import pandas as pd
from sklearn.model_selection import train_test_split

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
attr_norm = ["REN", "DOMAIN_LENGTH", "TRANSFERS", "RESTORES", "TRAFFIC_SCORE"]
cat_col = [
    "TLD",
    "REGISTRAR_NAME",
    "GL_CODE_NAME",
    "COUNTRY",
    "HISTORY",
    "TERM_LENGTH",
    "RES30",
    "REREG",
    "QTILE",
    "HD",
]
num_col = attr_norm.copy()
num_col.extend(["NS_V0", "NS_V1", "NS_V2"])
s3_source = "s3://md-en-csci-e-29-final/"


def df_to_dataset(dataframe, shuffle=True, batch_size=32):

    dataframe = dataframe.copy()

    labels = dataframe.pop("TARGET")

    ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))

    if shuffle:
        ds = ds.shuffle(buffer_size=len(dataframe))

    ds = ds.batch(batch_size)

    return ds


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

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/traindata")

    def run(self):
        trn_atr = (
            self.input()["S3TrnAttr"]
                .read_dask(dtype=attr_type)
                .set_index("TRANSACTION_ID")
        )
        trn_ts = (
            self.input()["S3TrnTscore"]
                .read_dask(dtype=ts_type)
                .set_index("TRANSACTION_ID")
        )

        trn = trn_atr.join(trn_ts)
        # print(type(trn[attr_norm].max().compute()))

        trn_max = pd.DataFrame(trn[attr_norm].max().compute())
        trn_max.columns = ["max_val"]
        trn_max.to_parquet("./data/trn_max.parquet", compression="gzip")

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

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/testdata")

    def run(self):
        tst_atr = (
            self.input()["S3TstAttr"]
                .read_dask(dtype=attr_type)
                .set_index("TRANSACTION_ID")
        )
        tst_ts = (
            self.input()["S3TstTscore"]
                .read_dask(dtype=ts_type)
                .set_index("TRANSACTION_ID")
        )

        tst = tst_atr.join(tst_ts)

        self.output().write_dask(tst, compression="gzip")


class BySomething(Task):
    __version__ = "0.0.0"

    ByWhat = Parameter()
    subset = BoolParameter(default=True)

    # Be sure to read from CleanedReviews locally

    TheData = Requirement(LoadData)
    requires = Requires()

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./yelp_result")

    # output = TargetOutput(target_class=lt.ParquetTarget,target_path='./yelp_result')

    def run(self):
        ddf = self.input().read_dask()
        res = ddf[["lenn", self.ByWhat]].groupby(self.ByWhat).mean()
        self.output().write_dask(res, compression="gzip")

        self.print_results()

    def print_results(self):
        print(self.output().read_dask().compute())


class FitModel(Task):
    __version__ = "0.0.0"
    TheData = Requirement(LoadData)
    requires = Requires()

    def run(self):

        df_max = pd.read_parquet("./data/trn_max.parquet")

        trn = self.input().read_dask().compute()

        for idx in df_max.index.values:
            trn[idx] = trn[idx] / df_max.loc[idx][0]

        feature_columns = []

        for header in num_col:
            feature_columns.append(tf.feature_column.numeric_column(header))

        for header in cat_col:
            categorical_column = (
                tf.feature_column.categorical_column_with_vocabulary_list(
                    header, trn[header].unique()
                )
            )

        indicator_column = tf.feature_column.indicator_column(categorical_column)
        feature_columns.append(indicator_column)

        feature_layer = tf.keras.layers.DenseFeatures(feature_columns)

        train, val = train_test_split(trn, test_size=0.2)

        train_ds = df_to_dataset(train)
        val_ds = df_to_dataset(val, shuffle=False)

        tf.keras.backend.set_floatx('float64')

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

        nepochs = 10

        history = model.fit(train_ds, validation_data=val_ds, epochs=nepochs, verbose=1);
