from unittest import TestCase
from pyspark.sql import SparkSession
from lib.utils import load_df, df_aggregation


# define a class per each unit test
class UtilsTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.spark: SparkSession = (
            SparkSession.builder.master("local[3]").appName("SparkTest").getOrCreate()
        )

        return super().setUpClass()

    @classmethod
    def tearDownClass(cls) -> None:
        cls.spark.stop()

    def test_datafile_loading(self) -> None:
        sample_df = load_df(self.spark, data_conf={"path": "data/sample.csv"})
        result_count = sample_df.count()
        self.assertEqual(result_count, 9, "Record count should be 9")

    def test_country_count(self):
        sample_df = load_df(self.spark, data_conf={"path": "data/sample.csv"})
        country_list = df_aggregation(sample_df).collect()
        country_dict = {}

        for row in country_list:
            country_dict[row["Country"]] = row["count"]

        self.assertEqual(country_dict["Canada"], 2, "Count for canada should be 2")
        self.assertEqual(
            country_dict["United States"], 4, "Count for united state should be 4"
        )
        self.assertEqual(
            country_dict["United Kingdom"], 1, "Count for united kingdom should be 1"
        )
