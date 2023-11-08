from pyspark.sql import SparkSession


class Log4J:
    def __init__(self, spark: SparkSession) -> None:
        # get jvm log4j object
        log4j = spark._jvm.org.apache.log4j  # type: ignore

        # defie logger
        root_class = "guru.learningjournal.spark.examples"
        app_conf = spark.sparkContext.getConf()
        app_name = app_conf.get("spark.app.name")

        self.logger = log4j.LogManager.getLogger(root_class + "." + app_name)  # type: ignore

    def warn(self, message):
        self.logger.warn(message)

    def info(self, message):
        self.logger.info(message)

    def error(self, message):
        self.logger.error(message)

    def debug(self, message):
        self.logger.debug(message)
