# %%
import pytest
import pathlib
import sys
import pyspark.sql
import pytz
import datetime
import logging

# %%
logger = logging.getLogger(__name__)

# %%
cwd = pathlib.Path.cwd()
code_root = cwd.parent
sys.path.append(str(code_root))

from src import cust_seg

# %%
class TestJsonToLDJson (object):
    from_date = datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
    to_date = datetime.datetime.strptime("2023-04-01", "%Y-%m-%d")
    input_file_path = "./data/CustData.json"
    output_file_path = "./CustSeg.csv"

    @pytest.fixture(scope="session")
    def spark_session(self):
        """Fixture for creating a spark context."""

        spark = pyspark.sql.SparkSession.builder\
            .master('local[2]')\
            .appName("customer_segment")\
            .config("spark.scheduler.mode", "FAIR")\
            .getOrCreate()
        yield spark
        spark.stop()

    @pytest.fixture(scope="session")
    def expected_df(self, spark_session):
        yield spark_session.read.format("csv").options(header=True).load("./data/CustSeg.csv")

    def test_process_cust_data (self, spark_session, expected_df):
        logger.info("TestJsonToLDJson.test_process_cust_data")
        actual_df = cust_seg.process_cust_data(spark_session, self.input_file_path, self.from_date, self.to_date)

        diff_df1 = actual_df.subtract(expected_df)
        diff_df1.show()
        logger.info(diff_df1.collect())
        assert(diff_df1.count() == 0)

        diff_df2 = actual_df.subtract(expected_df)
        diff_df2.show()
        logger.info(diff_df2.collect())
        assert(diff_df2.count() == 0)