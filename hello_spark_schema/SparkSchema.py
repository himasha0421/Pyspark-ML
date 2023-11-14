from pyspark.sql import SparkSession
from lib.utils import get_spark_config, load_df
from lib.logger import Log4J
from pyspark.sql.types import (
    StructType,
    StructField,
    DateType,
    IntegerType,
    VarcharType,
    StringType,
)


if __name__ == "__main__":
    # load the spark configs
    spark_conf = get_spark_config()

    # define spark session
    spark: SparkSession = SparkSession.builder.config(conf=spark_conf).getOrCreate()

    # initialize the logger
    logger = Log4J(spark=spark)

    """
    relay on inferschema method is not ideal better to use
        explicit
        implicit : use data types which comes with schema
    """
    ###################### CSV #########################
    # define the schema def
    schema = StructType(
        [
            StructField("FL_DATE", DateType()),
            StructField("OP_CARRIER", StringType()),
            StructField("OP_CARRIER_FL_NUM", IntegerType()),
            StructField("ORIGIN", StringType()),
            StructField("ORIGIN_CITY_NAME", StringType()),
            StructField("DEST", StringType()),
            StructField("DEST_CITY_NAME", StringType()),
            StructField("CRS_DEP_TIME", IntegerType()),
            StructField("DEP_TIME", IntegerType()),
            StructField("WHEELS_ON", IntegerType()),
            StructField("TAXI_IN", IntegerType()),
            StructField("CRS_ARR_TIME", IntegerType()),
            StructField("ARR_TIME", IntegerType()),
            StructField("CANCELLED", IntegerType()),
            StructField("DISTANCE", IntegerType()),
        ]
    )

    # load data as csv
    flight_df = (
        spark.read.format("csv")
        .option("header", True)
        # .option("inferSchema", True)
        .schema(schema=schema)
        .option("mode", "FAILFAST")
        .option("dateformat", "M/d/y")
        .load("data/flight-time.csv")
    )
    flight_df.show(5)
    logger.info("csv schema : " + flight_df.schema.simpleString())

    ###################### JSON #########################

    # DDL string to define the schema
    ddl_schema = """FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, 
          ORIGIN_CITY_NAME STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, 
          WHEELS_ON INT, TAXI_IN INT, CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    # load data as json
    flight_json = (
        spark.read.format("json")
        .schema(ddl_schema)
        .option("dateformat", "M/d/y")
        .load("data/flight-time.json")
    )

    flight_json.show(5)
    logger.info("JSON schema : " + flight_json.schema.simpleString())

    ##################### Parquet #######################3
    # load data as csv
    flight_parq = spark.read.format("parquet").load("data/flight-time.parquet")
    flight_parq.show(5)
    logger.info("csv schema : " + flight_parq.schema.simpleString())
