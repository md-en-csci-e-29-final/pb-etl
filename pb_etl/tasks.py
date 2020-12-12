import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # or any {'0', '1', '2'}

import tensorflow as tf

physical_devices = tf.config.list_physical_devices('GPU')
tf.config.experimental.set_memory_growth(physical_devices[0], True)

from tensorflow.keras import layers
from luigi import ExternalTask, LocalTarget, Task, Parameter
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

class BackTestRslt(ExtData):
    data_source = "results/"


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
        trn_max.to_parquet("./data/trn_max.parquet.gzip", compression="gzip")

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


class FitNNModel(Task):
    __version__ = "0.0.0"
    TheData = Requirement(LoadData)
    requires = Requires()

    def output(self):
        return LocalTarget(path="./data/repository/nn01")

    def run(self):

        trn = self.input().read_dask().compute()

        df_max = pd.read_parquet("./data/trn_max.parquet.gzip")

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
            layers.Dense(32, activation='relu'),
            layers.Dropout(.1),
            layers.Dense(1, activation='sigmoid')
        ])

        model.compile(optimizer='adam',
                      loss=tf.keras.losses.BinaryCrossentropy(from_logits=False),
                      metrics=['accuracy'])

        nepochs = 10

        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

        history = model.fit(train_ds, validation_data=val_ds, epochs=nepochs, verbose=0)

        model.save(self.output().path)

class NNPredict(Task):
    __version__ = "0.0.0"
    Data = Requirement(LoadTest)
    Model = Requirement(FitNNModel)

    requires = Requires()

    # def output(self):
    #     return LocalTarget(path="./data/result.parquet.gzip")

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/result")

    def run(self):

        tst = self.input()["Data"].read_dask().compute()

        df_max = pd.read_parquet("./data/trn_max.parquet.gzip")

        for idx in df_max.index.values:
            tst[idx] = tst[idx] / df_max.loc[idx][0]

        tst_dd = tf.data.Dataset.from_tensor_slices(dict(tst))
        tst_dd = tst_dd.batch(32)

        model = tf.keras.models.load_model(self.input()["Model"].path)
        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)
        y_test_hat = model.predict(tst_dd, verbose=0)

        tst["Y_hat"] = y_test_hat

        from dask import dataframe as dd

        rslt = pd.DataFrame(y_test_hat, index=tst.index.values)
        rslt.columns = ["Y_hat"]
        rslt.index.name = "TRANSACTION_ID"

        dask_rslt = dd.from_pandas(rslt,npartitions=1)

        self.output().write_dask(dask_rslt, compression='gzip')


class BackTest(Task):
    __version__ = "0.0.0"
    actl = Requirement(BackTestRslt)
    frcst = Requirement(NNPredict)
    requires = Requires()

    def output(self):
        return LocalTarget(path="./data/backtest.parquet.gzip")

    def run(self):
        df_actl = (
            self.input()["actl"]
                .read_dask(dtype={"TRANSACTION_ID": "int64","TARGET": "int64"})
                .set_index("TRANSACTION_ID")
        )
        df_frcst = (
            self.input()["frcst"]
                .read_dask(dtype={"TRANSACTION_ID": "int64","Y_hat": "float64"})
        )

        bcktst = df_actl.join(df_frcst)

        print(bcktst.sum().compute())

        #
        # self.output().write_dask(bcktst, compression="gzip")