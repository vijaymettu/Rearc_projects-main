from pyspark.sql import SparkSession
from pyspark.sql.functions import col, mean, stddev,row_number, sum, trim
from pyspark.sql.window import Window
from pyspark.sql.types import *

spark = SparkSession.builder.appName("rearc_code").getOrCreate()

df_pop_raw = spark.read.json(r"src\rearc\data\population_data.jsonl")

df_pop_raw = df_pop_raw.toDF(*[col.strip().replace(" ", "") for col in df_pop_raw.columns])

df_pop_raw = df_pop_raw.select([trim(col(c)).alias(c) if dict(df_pop_raw.dtypes)[c] == 'string' else col(c) for c in df_pop_raw.columns])

# Part 3.1: Data Analytics
df_report_1 = df_pop_raw.filter((col("Year") >= 2013) & (col("Year") <= 2018)).select(mean("Population"), stddev("Population"))

df_report_1.show()

# time series data
schema = StructType([
    StructField("series_id", StringType(), True),
    StructField("year", StringType(), True),
    StructField("period", StringType(), True),
    StructField("value", StringType(), True),
    StructField("footnote_codes", StringType(), True)
])

filepath = r"src\rearc\data\pr.data.0.Current.csv"

df_ts_raw = spark.read.csv(
    filepath,
    schema=schema,
    header=True
)

df_ts_raw = df_ts_raw.select([trim(col(c)).alias(c) if dict(df_ts_raw.dtypes)[c] == 'string' else col(c) for c in df_ts_raw.columns])

df_ts_raw = df_ts_raw.withColumn("year", col("year").cast("int")) \
                     .withColumn("value", col("value").cast("double"))

df_ts_raw.show()

# Part 3.2: Data Analytics
df_ts_sum = df_ts_raw.groupBy("series_id", "year") \
                     .agg(sum("value").alias("total_value"))

window_spec = Window.partitionBy("series_id").orderBy(col("total_value").desc())

df_report_2 = df_ts_sum.withColumn("rank", row_number().over(window_spec)) \
                       .filter(col("rank") == 1) \
                       .select("series_id", "year", col("total_value").alias("value"))

df_report_2.show()

# # Part 3.3: Data Analytics

df_report_3 = df_ts_raw.join(df_pop_raw, df_ts_raw.year == df_pop_raw.Year, "left"). \
select(df_ts_raw.series_id, df_ts_raw.year, df_ts_raw.period, df_ts_raw.value, df_pop_raw.Population).filter((col("series_id") == 'PRS30006032') & (col("year") == 2018) & (col("period") == 'Q01'))


df_report_3.show()


