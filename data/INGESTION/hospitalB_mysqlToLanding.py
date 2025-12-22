# =============================================================================
# Imports
# =============================================================================
from google.cloud import storage
import datetime
import argparse
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType

# Common utilities
from common_lib.spark_utils import get_spark, read_csv
from common_lib.bq_utils import get_bq_client, run_query
from common_lib.config_utils import (
    log_event,
    save_logs_to_gcs,
    save_logs_to_bigquery
)
from common.constants import Constants


# =============================================================================
# Argument Parsing
# =============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")
parser.add_argument("--project_id", required=True, help="GCP project ID")
parser.add_argument("--hospital_name", required=True, help="Hospital name")
parser.add_argument("--hospital_db", required=True, help="Hospital database name")
parser.add_argument("--mysql_host", required=True, help="MySQL host address")
parser.add_argument("--mysql_port", required=True, help="MySQL port number")
args = parser.parse_args()


# =============================================================================
# GCS Configuration
# =============================================================================
GCS_BUCKET = args.gcs_bucket
HOSPITAL_NAME = args.hospital_name

LANDING_PATH = Constants.GCP.GCS_LANDING_PATH.format(
    gcs_bucket=GCS_BUCKET,
    hospital_name=HOSPITAL_NAME
)
ARCHIVE_PATH = Constants.GCP.GCS_ARCHIVE_PATH.format(
    gcs_bucket=GCS_BUCKET,
    hospital_name=HOSPITAL_NAME
)
CONFIG_FILE_PATH = Constants.GCP.GCS_CONFIG_PATH.format(
    gcs_bucket=GCS_BUCKET
)


# =============================================================================
# BigQuery Configuration
# =============================================================================
BQ_PROJECT = args.project_id

BQ_AUDIT_TABLE = Constants.BQ.AUDIT_LOG_TABLE.format(
    bq_project=BQ_PROJECT
)
BQ_LOG_TABLE = Constants.BQ.PIPELINE_LOGS_TABLE.format(
    bq_project=BQ_PROJECT
)
BQ_TEMP_PATH = Constants.BQ.TEMP_PATH.format(
    gcs_bucket=GCS_BUCKET
)


# =============================================================================
# Client Initialization
# =============================================================================
storage_client = storage.Client()
bq_client = get_bq_client(BQ_PROJECT)
bucket = storage_client.bucket(GCS_BUCKET)


# =============================================================================
# Spark Initialization
# =============================================================================
spark = get_spark(
    Constants.Common.AppName.format(hospital_name=HOSPITAL_NAME)
)


# =============================================================================
# MySQL Configuration
# =============================================================================
MYSQL_HOST = args.mysql_host
MYSQL_PORT = args.mysql_port
HOSPITAL_NAME_DB = args.hospital_db

MYSQL_CONFIG = {
    "url": Constants.MySQL.JDBC_URL.format(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        database=HOSPITAL_NAME_DB
    ),
    "driver": Constants.MySQL.DRIVER,
    "user": "myuser",
    "password": "asdf@ATH10"
}


# =============================================================================
# Read Configuration File
# =============================================================================
def read_config_file():
    df = read_csv(spark, CONFIG_FILE_PATH, header=True)
    log_event(
        Constants.Logger.INFO,
        Constants.SuccessMessage.CONFIG_FILE_READ_MESSAGE
    )
    return df


config_df = read_config_file()
config_df.show()


# =============================================================================
# Move Existing Files to Archive
# =============================================================================
def move_existing_files_to_archive(table):
    blobs = list(
        storage_client
        .bucket(GCS_BUCKET)
        .list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/")
    )

    existing_files = [
        blob.name for blob in blobs if blob.name.endswith(".json")
    ]

    if not existing_files:
        log_event(
            Constants.Logger.INFO,
            Constants.InfoMessage.NO_EXISTING_FILES_MESSAGE.format(
                table=table
            )
        )
        return

    for file in existing_files:
        source_blob = bucket.blob(file)

        # Extract date from file name
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = (
            date_part[-4:],
            date_part[2:4],
            date_part[:2]
        )

        archive_path = (
            f"landing/{HOSPITAL_NAME}/archive/{table}/"
            f"{year}/{month}/{day}/{file.split('/')[-1]}"
        )

        destination_blob = bucket.blob(archive_path)

        bucket.copy_blob(
            source_blob,
            bucket,
            destination_blob.name
        )

        source_blob.delete()

        log_event(
            Constants.Logger.INFO,
            Constants.SuccessMessage.FILE_MOVED_TO_ARCHIVE_MESSAGE.format(
                file=file,
                archive_path=archive_path
            ),
            table=table
        )


# =============================================================================
# Get Latest Watermark
# =============================================================================
def get_latest_watermark(table_name):
    query = Constants.Query.WATERMARK_QUERY.format(
        BQ_AUDIT_TABLE=BQ_AUDIT_TABLE,
        table_name=table_name,
        hospital_db=HOSPITAL_NAME_DB
    )

    result = run_query(bq_client, query)

    for row in result:
        return (
            row.latest_timestamp
            if row.latest_timestamp
            else Constants.Common.DEFAULT_WATERMARK
        )

    return Constants.Common.DEFAULT_WATERMARK


# =============================================================================
# Extract Data and Save to Landing Zone
# =============================================================================
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = (
            get_latest_watermark(table)
            if load_type.lower() == Constants.Common.DEFAULT_LOAD_TYPE
            else None
        )

        log_event(
            Constants.Logger.INFO,
            Constants.SuccessMessage.LATEST_WATERMARK_MESSAGE.format(
                table=table,
                last_watermark=last_watermark
            ),
            table=table
        )

        query = (
            Constants.Query.FULL_TABLE_QUERY.format(table=table)
            if load_type.lower() == Constants.Common.FULL_LOAD_TYPE
            else Constants.Query.INCREMENTAL_TABLE_QUERY.format(
                table=table,
                watermark_col=watermark_col,
                last_watermark=last_watermark
            )
        )

        df = (
            spark.read.format("jdbc")
            .option("url", MYSQL_CONFIG["url"])
            .option("user", MYSQL_CONFIG["user"])
            .option("password", MYSQL_CONFIG["password"])
            .option("driver", MYSQL_CONFIG["driver"])
            .option("dbtable", query)
            .load()
        )

        log_event(
            Constants.Logger.SUCCESS,
            Constants.SuccessMessage.DATA_EXTRACTED_MESSAGE.format(
                table=table
            ),
            table=table
        )

        today = datetime.datetime.today().strftime("%d%m%Y")
        JSON_FILE_PATH = (
            f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"
        )

        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(
            df.toPandas().to_json(orient="records", lines=True),
            content_type="application/json"
        )

        log_event(
            Constants.Logger.SUCCESS,
            Constants.SuccessMessage.JSON_FILE_WRITTEN_MESSAGE.format(
                gcs_bucket=GCS_BUCKET,
                json_file_path=JSON_FILE_PATH
            ),
            table=table
        )
        schema_audit = StructType([
            StructField("data_source", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("load_timestamp", TimestampType(), True),
            StructField("status", StringType(), True)
        ])

        audit_df = spark.createDataFrame(
            [
                (
                    HOSPITAL_NAME_DB,
                    table,
                    load_type,
                    df.count(),
                    datetime.datetime.now(),
                    Constants.Logger.SUCCESS
                )
            ],
            schema=schema_audit
        )

        (
            audit_df.write.format("bigquery")
            .option("table", BQ_AUDIT_TABLE)
            .option("temporaryGcsBucket", GCS_BUCKET)
            .mode("append")
            .save()
        )

        log_event(
            Constants.Logger.SUCCESS,
            Constants.SuccessMessage.AUDIT_LOG_UPDATED_MESSAGE.format(
                table=table
            ),
            table=table
        )

    except Exception as e:
        log_event(
            Constants.Logger.ERROR,
            Constants.ErrorMessage.ERROR_PROCESSING_TABLE_MESSAGE.format(
                table=table,
                error_msg=str(e)
            ),
            table=table
        )


# =============================================================================
# Driver Code
# =============================================================================
for row in config_df.collect():
    if (
        row[Constants.Common.IS_ACTIVE_COLUMN] == "1"
        and row[Constants.Common.DATA_SOURCE_COLUMN] == HOSPITAL_NAME_DB
    ):
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)


# =============================================================================
# Persist Pipeline Logs
# =============================================================================
save_logs_to_gcs(storage_client, GCS_BUCKET)
save_logs_to_bigquery(spark, BQ_LOG_TABLE, BQ_TEMP_PATH)


# =============================================================================
# Stop Spark Session
# =============================================================================
spark.stop()
