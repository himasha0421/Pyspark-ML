from pyspark.sql import SparkSession
from lib.logger import Log4J
from lib.utils import get_spark_config, load_df, read_data_config

if __name__ == "__main__":
    # load the spark conf
    conf = get_spark_config()
    data_conf = read_data_config()
    # initialize the spark session
    spark: SparkSession = SparkSession.builder.config(conf=conf).getOrCreate()  # type: ignore

    # define the logger
    logger = Log4J(spark=spark)

    # load the dataset
    spark_df = load_df(spark_session=spark, data_conf=data_conf)

    """
    spark sql only allows to run on table or a view
    first register dataframe as a view
    """

    spark_df.createOrReplaceTempView("survey_view")

    # apply sql expression
    count_df = spark.sql(
        " select Country , count(*) as count from survey_view \
            where Age < 40 group by Country"
    )

    count_df.show()
