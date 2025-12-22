from pyspark.sql import SparkSession

def get_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

def stop_spark(spark):
    if spark:
        spark.stop()

def read_gcs(
    spark,
    path: str,
    fmt: str,
    **options
):
    return spark.read.format(fmt).options(**options).load(path)

def read_csv(spark, path: str, header=True, infer_schema=True):
    return spark.read.csv(
        path,
        header=header,
        inferSchema=infer_schema
    )

def read_json(spark, path: str, multiline=False):
    return spark.read.option("multiline", multiline).json(path)

def read_parquet(spark, path: str):
    return spark.read.parquet(path)

def write_df(
    df,
    path: str,
    fmt: str,
    mode="overwrite",
    partition_cols: list = None,
    **options
):
    writer = df.write.format(fmt).mode(mode)

    if partition_cols:
        writer = writer.partitionBy(partition_cols)

    if options:
        writer = writer.options(**options)

    writer.save(path)

def write_parquet(df, path: str, mode="overwrite", partition_cols=None):
    write_df(df, path, "parquet", mode, partition_cols)

def write_json(df, path: str, mode="overwrite"):
    df.write.mode(mode).json(path)

def write_csv(df, path: str, mode="overwrite", header=True):
    df.write.mode(mode).option("header", header).csv(path)

def read_bq(
    spark,
    table: str,
    project: str = None,
    dataset: str = None,
    query: str = None
):
    reader = spark.read.format("bigquery")

    if table:
        reader = reader.option("table", table)

    if query:
        reader = reader.option("query", query)

    if project:
        reader = reader.option("project", project)

    if dataset:
        reader = reader.option("dataset", dataset)

    return reader.load()

def write_bq(
    df,
    table: str,
    write_mode="append",
    partition_field: str = None,
    cluster_fields: list = None
):
    writer = df.write.format("bigquery").mode(write_mode)

    if partition_field:
        writer = writer.option("partitionField", partition_field)

    if cluster_fields:
        writer = writer.option("clusteredFields", ",".join(cluster_fields))

    writer.save(table)

def cache_df(df):
    df.cache()
    df.count()  # materialize cache
    return df

def uncache_df(df):
    df.unpersist()

def df_info(df, rows=5):
    df.printSchema()
    df.show(rows, truncate=False)

from pyspark.sql.functions import current_timestamp, lit

def add_audit_columns(df, source_system: str):
    return (
        df
        .withColumn("ingestion_ts", current_timestamp())
        .withColumn("source_system", lit(source_system))
    )

def validate_not_empty(df, name="DataFrame"):
    if df.rdd.isEmpty():
        raise ValueError(f"{name} is empty")
