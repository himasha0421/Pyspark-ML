from pyspark.sql import SparkSession
from lib.logger import Log4J
from pyspark import SparkConf
from lib.utils import get_spark_config, read_data_config, load_df, df_aggregation

if __name__ == "__main__":
    # define spark configs
    conf = get_spark_config()
    data_conf = read_data_config()

    # create spark session
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()

    # initiate the logger
    logger = Log4J(spark=spark)

    # push a log message
    logger.info("Start Hello Spark")
    logger.info("Stop Spark Session")

    # get configs to logging
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    """
    spark execution plan
    1. devide the code into seperate sections based on action
        dataset read / transformation collect
    2. each section becomes at least one spark job


    job breakdown

    data reading :
        job 0 : read file
        job 1 : infer header/schema
    
    transform & collect :
        job 2 : 
            stage 1: repartition the csv ( 1 task )
            stage 2: compute where,select,groupby (2 Tasks parallel execution for each partition)
                     final group by add shuffle/sort exchange operation
            stage 3: do count and collect ( 2 tasks for each parition)
    """

    spark_df = load_df(spark_session=spark, data_conf=data_conf)
    # apply repartition trasformation
    paritioned_df = spark_df.repartition(2)

    # apply transformation
    count_df = df_aggregation(paritioned_df)

    logger.info(count_df.collect())

    # hold the execution
    input("Enter value")

    # stop spark session
    spark.stop()
