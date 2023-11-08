import configparser
from pyspark import SparkConf


def get_spark_config():
    # define spark config object
    spark_conf = SparkConf()

    # read the config file
    config = configparser.ConfigParser()
    config.read("spark.conf")

    # read the configs using key, value paris
    for key, value in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, value)

    return spark_conf


def read_data_config():
    data_config = {}
    parser = configparser.ConfigParser()
    parser.read("data.conf")

    for key, val in parser.items("DATASET_CONFIGS"):
        data_config[key] = val

    return data_config


def load_df(spark_session, data_conf):
    # read a dataset
    """
    csv data read without inferschema results single job
    inferschema/header options read portion of data which is seperate action and resulting extra job
    """
    spark_df = (
        spark_session.read.option("delimiter", ",")
        .option("header", True)
        .option("inferSchema", True)
        .csv(data_conf["path"])
    )

    return spark_df


def df_aggregation(df):
    return (
        df.where("Age < 40")
        .select("Age", "Gender", "Country", "state")
        .groupBy("Country")
        .count()
    )
