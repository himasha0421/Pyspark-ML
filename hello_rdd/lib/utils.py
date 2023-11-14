import configparser
from pyspark import SparkConf
from typing import Dict


def load_spark_config():
    # define sparkconf object
    spark_cnf = SparkConf()

    # load the config file
    conf_reader = configparser.ConfigParser()
    conf_reader.read("spark.conf")

    # add key values into spark conf object
    for key, value in conf_reader.items("SPARK_APP_CONFIGS"):
        spark_cnf.set(key=key, value=value)

    return spark_cnf


def load_data():
    # laod the data config
    conf_reader = configparser.ConfigParser()
    conf_reader.read("data.conf")

    data_cnf: Dict[str, str] = {}
    for key, value in conf_reader.items("SPARK_DATA_CONFIG"):
        data_cnf[key] = value

    return data_cnf
