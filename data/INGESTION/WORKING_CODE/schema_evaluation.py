from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)

spark = SparkSession.builder.appName("schema-eval").getOrCreate()

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# CSV
df_csv = (
    spark.read
    .schema(schema)
    .option("header", "true")
    .option("mode", "FAILFAST")
    .csv("gs://bucket/path/data.csv")
)

# ORC
df_orc = (
    spark.read
    .schema(schema)
    .orc("gs://bucket/path/data.orc")
)

# PARQUET
df_parquet = (
    spark.read
    .schema(schema)
    .parquet("gs://bucket/path/data.parquet")
)

# JSON
df_json = (
    spark.read
    .schema(schema)
    .option("mode", "FAILFAST")
    .json("gs://bucket/path/data.json")
)

# Schema validation
for df in [df_csv, df_orc, df_parquet, df_json]:
    df.printSchema()
    df.show(5, truncate=False)

# -----------------------------------------------------------

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, TimestampType
)

spark = (
    SparkSession.builder
    .appName("bq-schema")
    .getOrCreate()
)

schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("source", StringType(), True),
    StructField("created_at", TimestampType(), True)
])

# BigQuery READ
df_bq_read = (
    spark.read
    .format("bigquery")
    .schema(schema)
    .option("table", "quantum-episode-345713.dataset_name.source_table")
    .load()
)

# BigQuery WRITE
(
    df_bq_read
    .write
    .format("bigquery")
    .option("table", "quantum-episode-345713.dataset_name.target_table")
    .option("writeMethod", "direct")
    .mode("append")
    .save()
)
