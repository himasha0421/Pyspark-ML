from lib.utils import load_spark_config, load_data
from pyspark import SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4J
from typing import NamedTuple


Record_schema = NamedTuple(
    "SurveyRecords",
    [
        ("Age", int),
        ("Gender", str),
        ("Country", str),
        ("State", str),
    ],
)

if __name__ == "__main__":
    # load the spark configs
    conf = load_spark_config()

    """
    dataframe API : sparkSession 
    rdd API : sparkContext SparkContext(conf=conf)
    """

    spark_session: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()
    spark_cntx: SparkContext = spark_session.sparkContext

    logger = Log4J(spark=spark_session)

    # load the data configs
    data_cnf = load_data()

    # read the datafile using spark context
    """
    methods to read binary file , hadoop file , object file
    not avilable to read csv,json,parquet,avro
    dataframe API supports csv and other file formats
    """
    RDD_dataset = spark_cntx.textFile(data_cnf["dataset_path"])

    # repartition the rdd
    partitionedRDD = RDD_dataset.repartition(2)

    # since sparkcontext reads the data in text format need to add a format
    colsRDD = partitionedRDD.map(lambda line: line.replace('"', "").split(","))
    # apply the schema to read row
    selectRDD = colsRDD.map(
        lambda cols: Record_schema(
            int(cols[1]),
            cols[2],
            cols[3],
            cols[4],
        )
    )

    # apply the filter
    filteredRDD = selectRDD.filter(lambda row: row.Age < 40)

    """
    for group by function 
        step 1: create a map rdd 
        step 2: reduce by the key and apply summation or any function
    """
    kvvRDD = filteredRDD.map(lambda row: (row.Country, 1))
    countRDD = kvvRDD.reduceByKey(lambda v1, v2: v1 + v2)  # type: ignore

    # apply a action
    colsList = countRDD.collect()

    """
    major take away :
        spark has no idea about the data structure inside RDD
        also spark can't look into lambda function 
    """

    # add data into log
    for x in colsList:
        logger.info(x)
