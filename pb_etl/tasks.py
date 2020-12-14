import os

os.environ["TF_CPP_MIN_LOG_LEVEL"] = "3"  # or any {'0', '1', '2'}

import tensorflow as tf

physical_devices = tf.config.list_physical_devices("GPU")
if len(physical_devices) > 0:
    tf.config.experimental.set_memory_growth(physical_devices[0], True)

from tensorflow.keras import layers
from luigi import ExternalTask, LocalTarget, Task, Parameter
import pb_etl.luigi.dask.target as lt
from pb_etl.luigi.task import Requirement, Requires, TargetOutput, SaltedOutput

import pandas as pd
from sklearn.model_selection import train_test_split
from dask import dataframe as dd

# Domain attributes
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
# Traffic score file format
ts_type = {"TRANSACTION_ID": "int64", "TRAFFIC_SCORE": "float64"}

# Fields to be normalized
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
# All numeric Attributes
num_col = attr_norm.copy()
num_col.extend(["NS_V0", "NS_V1", "NS_V2"])


def df_to_dataset(dataframe, shuffle=True, batch_size=32):

    """
    A utility function to create a tf.data dataset from a Pandas Dataframe

    :param dataframe: iput data
    :param shuffle: shuffle flag (True/False)
    :param batch_size: Batch Size
    :return: ts. data.Dataset
    """

    dataframe = dataframe.copy()

    labels = dataframe.pop("TARGET")

    ds = tf.data.Dataset.from_tensor_slices((dict(dataframe), labels))

    if shuffle:
        ds = ds.shuffle(buffer_size=len(dataframe))

    ds = ds.batch(batch_size)

    return ds


class ExtData(ExternalTask):

    """
    base class to work with aws s3
    """

    __version__ = "0.0.0"

    # must be assigned in the child class
    data_source = ""

    def output(self):
        src_data_path_default = "s3://md-en-csci-e-29-final/"

        # Using .env variable to do tests from local file system

        src_data_path = os.getenv("FINAL_PROJ_BUCKET", default=src_data_path_default)

        return lt.CSVTarget(
            src_data_path + self.data_source,
            storage_options=dict(requester_pays=True),
            flag=None,
        )


class TrnAttr(ExtData):
    """
    Getting domain attributes training data set
    """

    data_source = "train/attr/"


class TrnTscore(ExtData):
    """
    Getting domain traffic score for training dataset
    """

    data_source = "train/tscore/"


class TstAttr(ExtData):
    """
    Getting data to create dataset to forecast
    """

    data_source = "test/attr/"


class TstTscore(ExtData):
    """
    Getting traffic score to add to the forecast dataset
    """

    data_source = "test/tscore/"


class BackTestRslt(ExtData):
    """Getting backtest result"""

    data_source = "results/"


class LoadData(Task):
    """
    Initial stage, loading and transformation of training data
    """

    __version__ = "0.0.0"

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

        # Joining domain attributes and traffic score
        trn = trn_atr.join(trn_ts)

        self.output().write_dask(trn, compression="gzip")


class NormalizationDenominators(Task):
    """
    Generating Data to Normalize Numeric Attributes. The data will be required for creating model and for the prediction process
    """

    __version__ = "0.0.0"

    train_data = Requirement(LoadData)
    requires = Requires()

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/trn_max")

    def run(self):
        trn = self.input().read_dask()
        trn_max = pd.DataFrame(trn[attr_norm].max().compute())
        trn_max.columns = ["max_val"]
        trn_max_dask = dd.from_pandas(trn_max, npartitions=1)
        self.output().write_dask(trn_max_dask, compression="gzip")


class LoadTest(Task):
    """
    Designing a forecast dataset
    """

    __version__ = "0.0.0"

    S3TstTscore = Requirement(TstTscore)
    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/testdata")
    S3TstAttr = Requirement(TstAttr)
    requires = Requires()

    def run(self):
        tst_ts = (
            self.input()["S3TstTscore"]
            .read_dask(dtype=ts_type)
            .set_index("TRANSACTION_ID")
        )
        tst_atr = (
            self.input()["S3TstAttr"]
            .read_dask(dtype=attr_type)
            .set_index("TRANSACTION_ID")
        )

        tst = tst_atr.join(tst_ts)

        self.output().write_dask(tst, compression="gzip")


def the_norm(df, max_val_df):
    """
    Utility function for normalization
    :param df: input dataset
    :param max_val_df: normalization values
    :return: normalized dataset
    """
    for idx in max_val_df.index.values:
        df[idx] = df[idx] / max_val_df.loc[idx][0]
    return df


class FitNNModel(Task):

    """
    Creating and fitting up a predictive model
    """

    __version__ = "0.0.0"
    TheData = Requirement(LoadData)
    MaxDenoms = Requirement(NormalizationDenominators)
    requires = Requires()

    output = SaltedOutput(target_class=LocalTarget, target_path="./data/repository/nn")

    def run(self):

        trn = self.input()["TheData"].read_dask().compute()

        df_max = self.input()["MaxDenoms"].read_dask().compute()

        # normalization of the input dataset. NN does not work well with attributes with different scales
        trn = the_norm(trn, df_max)

        # creating feature_layer
        # for more information: https://www.tensorflow.org/tutorials/structured_data/feature_columns
        # begin

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

        tf.keras.backend.set_floatx("float64")

        #  feature_layer: end

        # creating model
        model = tf.keras.Sequential(
            [
                feature_layer,
                layers.Dense(1024, activation="relu"),
                layers.Dropout(0.2),
                layers.Dense(512, activation="relu"),
                layers.Dropout(0.2),
                layers.Dense(256, activation="relu"),
                layers.Dropout(0.2),
                layers.Dense(128, activation="relu"),
                layers.Dropout(0.2),
                layers.Dense(64, activation="relu"),
                layers.Dropout(0.1),
                layers.Dense(32, activation="relu"),
                layers.Dropout(0.1),
                layers.Dense(
                    1, activation="sigmoid"
                ),  # classification task (0,1) - so using sigmoid
            ]
        )

        model.compile(
            optimizer="adam",
            loss=tf.keras.losses.BinaryCrossentropy(from_logits=False),
            metrics=["accuracy"],
        )

        # fitting the model
        nepochs = 2  # 10

        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

        history = model.fit(train_ds, validation_data=val_ds, epochs=nepochs, verbose=0)

        import json

        # Get the dictionary containing each metric and the loss for each epoch
        history_dict = history.history

        # Save it under the form of a json file
        os.makedirs("./data/repository")
        with open("./data/repository/model_hist_params", "w") as opened_file:
            json.dump(history_dict, opened_file)

        # Saving the model in the local repository
        model.save(self.output().path)


class NNPredict(Task):

    """
    Predicting stage. Getting model, dataset to forecast and predict
    """

    __version__ = "0.0.0"
    Data = Requirement(LoadTest)
    MaxDenoms = Requirement(NormalizationDenominators)
    Model = Requirement(FitNNModel)

    requires = Requires()

    output = SaltedOutput(target_class=lt.ParquetTarget, target_path="./data/result")

    def run(self):

        # getting data

        tst = self.input()["Data"].read_dask().compute()

        df_max = self.input()["MaxDenoms"].read_dask().compute()

        # normalization of the input dataset.
        tst = the_norm(tst, df_max)

        tst_dd = tf.data.Dataset.from_tensor_slices(dict(tst))
        tst_dd = tst_dd.batch(32)

        # getting Model
        model = tf.keras.models.load_model(self.input()["Model"].path)
        tf.compat.v1.logging.set_verbosity(tf.compat.v1.logging.ERROR)

        # predict deletion probability
        y_test_hat = model.predict(tst_dd, verbose=0)

        # creating resulting dataset
        tst["Y_hat"] = y_test_hat
        rslt = pd.DataFrame(y_test_hat, index=tst.index.values)
        rslt.columns = ["Y_hat"]
        rslt.index.name = "TRANSACTION_ID"

        # saving data into local repository
        dask_rslt = dd.from_pandas(rslt, npartitions=1)
        self.output().write_dask(dask_rslt, compression="gzip")


class BackTest(Task):
    """
    Backtesting. Creating backtesting dataset (transaction_id, forecast probability, actual)
    """

    __version__ = "0.0.0"
    actl = Requirement(BackTestRslt)
    frcst = Requirement(NNPredict)
    requires = Requires()

    output = SaltedOutput(
        target_class=lt.ParquetTarget, target_path="./data/repository/backtest"
    )

    def run(self):
        """
        Retrieving a dataset with actual results and joining to a predicted dataset
        :return:
        """
        df_actl = (
            self.input()["actl"]
            .read_dask(dtype={"TRANSACTION_ID": "int64", "TARGET": "int64"})
            .set_index("TRANSACTION_ID")
        )
        df_frcst = self.input()["frcst"].read_dask(
            dtype={"TRANSACTION_ID": "int64", "Y_hat": "float64"}
        )

        bcktst = df_actl.join(df_frcst)

        self.output().write_dask(bcktst, compression="gzip")


class FinalResults(Task):
    """
    Printing the deletion rates (sum(actual)/count() vs. sum(forecast)/count()): actual vs forecast
    """

    back_tests = Requirement(BackTest)
    requires = Requires()

    def run(self):
        bcktst = self.input().read_dask()
        ln = bcktst.count().compute()

        print("=== FINAL RESULTS ===")
        print("=== Real Deletion Rate (TARGET) ===")
        print("=== Forecasted deletion rate (Y_hat) ===")
        print(bcktst.sum().compute() / ln)
        print("==============================")
