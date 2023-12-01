# %%
import pyspark
import datetime
import pytz
import logging
from pyspark.sql.types import TimestampType
from pyspark.sql import functions as sf

# %%
# from and to date is hard coded here, assume these will be replace by cmd line arguments
from_date = datetime.datetime.strptime("2023-01-01", "%Y-%m-%d")
to_date = datetime.datetime.strptime("2023-04-01", "%Y-%m-%d")
input_file_path = "../tests/data/CustData.json"
output_file_path = "./CustSeg.csv"

# %%
logger = logging.getLogger(__name__)

# %%
def load_df_from_file (spark: pyspark.sql.SparkSession, path_prefix: str, format: str, read_options: dict={}) -> pyspark.sql.DataFrame:
    logger.info ("Loading %s files in: %s\nWith options: %s", format, path_prefix, read_options)
    try:
        return_df = spark.read\
            .format(format)\
            .options(**read_options)\
            .load(path_prefix)
    except:
        logger.error("Error loading files from: %s", path_prefix)
        raise 
    else:
        logger.info("Successfully loaded file into data frame.")
        return return_df

# %%
def write_df_to_file (spark: pyspark.sql.SparkSession, 
    df: pyspark.sql.DataFrame, 
    format: str, 
    path_prefix: str, 
    write_options: dict={},
    single_file: bool=False):
    logger.info ("Writing %s files to: %s\nWith options: %s", format, path_prefix, write_options)
    try:
        if single_file:
            df = df.coalesce(1)
        df.write\
            .format(format)\
            .mode("overwrite")\
            .options(**write_options)\
            .save(path_prefix)
    except:
        logger.error("Error writing files to: %s", path_prefix)
        raise 
    else:
        logger.info("Successfully written Data Frame to %s file.", format)

# %%
def transform_df (df:pyspark.sql.DataFrame) -> pyspark.sql.DataFrame:
    df = df.withColumn("purchase_date", sf.from_utc_timestamp(sf.col("purchase_date"), "UTC"))
    return df

# %%
def calculate_segment (df:pyspark.sql.DataFrame, from_date:datetime.datetime, to_date:datetime.datetime) -> pyspark.sql.DataFrame:
    sum_df = df\
        .where((sf.col("purchase_date") >= from_date) & (sf.col("purchase_date") < to_date))\
        .groupby(sf.col("customer_id"), sf.col("name"), sf.col("email"))\
        .agg(sf.sum(sf.col("purchase_amount")).alias("total_purchase_amount"))

    sum_df = sum_df.withColumn("segment", 
        sf.when(sf.col("total_purchase_amount") < 100, "Low")\
        .when(sf.col("total_purchase_amount") > 500, "High")\
        .otherwise("Medium")
    ).orderBy(sf.col("customer_id"))
    return sum_df

# %%
def process_cust_data (
    spark: pyspark.sql.SparkSession,
    input_file_path: str,
    from_date:datetime.datetime, 
    to_date:datetime.datetime,
    input_format="json", 
    read_options={"multiLine": True}
    ) -> pyspark.sql.DataFrame:

    df = load_df_from_file (spark, input_file_path, input_format, read_options=read_options)
    df_t = transform_df (df)
    seg_df = calculate_segment (df_t, from_date, to_date)
    return seg_df

# %%
def main (args):
    spark = pyspark.sql.SparkSession.builder\
        .appName("customer_segment")\
        .config("spark.scheduler.mode", "FAIR")\
        .getOrCreate()
    seg_df = process_cust_data (spark, input_file_path, from_date, to_date)
    write_df_to_file (spark, seg_df, format="csv", path_prefix=output_file_path, write_options={"header": True}, single_file=True)

# %%
if __name__ in {'__main__'}:
    main (None)
