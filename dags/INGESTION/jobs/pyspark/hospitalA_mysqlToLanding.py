# -------- Imports ----------
from google.cloud import storage, bigquery
from pyspark.sql import SparkSession
import datetime
from .constants import Constants
from .config_reader import ConfigReader
from .event_logger import EventLogger
from spark_builder import SparkBuilder

# If using local notebook:
# export GOOGLE_APPLICATION_CREDENTIALS="key.json"

# -------- Configurations ----------
GCS_BUCKET = Constants.GCP.GCS_BUCKET_KEY
HOSPITAL_NAME = Constants.Common.HOSPITAL_NAME_A
HOSPITAL_NAME_A_DB = Constants.Common.HOSPITAL_NAME_A_DB

CONFIG_FILE_PATH = Constants.Common.CONFIG_FILE_PATH.format(
    gcs_bucket=GCS_BUCKET
)

BQ_PROJECT = Constants.GCP.BQ_PROJECT_ID
BQ_AUDIT_TABLE = Constants.Common.BQ_AUDIT_TABLE.format(bq_project=BQ_PROJECT)
BQ_LOG_TABLE = Constants.Common.BQ_LOG_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.Common.BQ_TEMP_PATH.format(gcs_bucket=GCS_BUCKET)

MYSQL_CONFIG = {
    "url": Constants.MySQL.URL_A.format(hospital_db=HOSPITAL_NAME_A_DB),
    "driver": Constants.MySQL.DRIVER,
    "user": Constants.MySQL.USER_A,
    "password": Constants.MySQL.PASSWORD_A,
}

# -------- Notebook-Safe Spark Session ----------
spark_builder = SparkBuilder()
spark = spark_builder.build_spark_session(
    Constants.SPARK.APP_NAME_HOSPITAL_A,
    BQ_PROJECT,
    BQ_TEMP_PATH
)

class Hospital_A_Job:
    def __init__(self, spark):
        self.spark = spark
        self.storage_client = storage.Client()
        self.bq_client = bigquery.Client()

        self.config_reader = ConfigReader()
        self.event_logger = EventLogger()
        self.bucket = self.storage_client.bucket(GCS_BUCKET)

        self.log_entries = []

        # Read Config
        self.config_df = self.config_reader.read_config_file(
            self.spark, CONFIG_FILE_PATH
        )
        self.log_event(Constants.Logger.INFO, Constants.Message.CONFIG_FILE_READ_MESSAGE)

    # ---------- Logging ----------
    def log_event(self, event_type, message, table=None):
        log_entry = {
            "timestamp": datetime.datetime.now().isoformat(),
            "event_type": event_type,
            "message": message,
            "table": table
        }
        self.log_entries.append(log_entry)

        print(Constants.Common.PRINT_STATEMENT.format(
            timestamp=log_entry['timestamp'], 
            event_type=event_type,
            message=message
        ))

    # ---------- Get Latest Watermark ----------
    def get_latest_watermark(self, table_name):
        query = Constants.Query.AUDIT_QUERY.format(
            bq_audit_table=BQ_AUDIT_TABLE,
            table_name=table_name,
            hospital_db=HOSPITAL_NAME_A_DB
        )
        result = self.bq_client.query(query).result()

        for row in result:
            return row.latest_timestamp or Constants.Common.DEFAULT_DATE

        return Constants.Common.DEFAULT_DATE

    # ---------- Start Job ----------
    def start_job(self):
        for row in self.config_df.collect():

            if row[Constants.Common.IS_ACTIVE] != "1":
                continue

            if row[Constants.Common.DATASOURCE] != HOSPITAL_NAME_A_DB:
                continue

            table = row[Constants.Common.TABLE_NAME]
            load_type = row[Constants.Common.LOAD_TYPE]
            watermark = row[Constants.Common.WATERMARK]

            self.move_existing_files_to_archive(table)
            self.extract_and_save_to_landing(table, load_type, watermark)

    # ---------- Move Existing Files ----------
    def move_existing_files_to_archive(self, table):
        prefix = Constants.Common.LANDING_TABLE_PATH.format(
            hospital_name=HOSPITAL_NAME, table=table
        )

        blobs = list(self.bucket.list_blobs(prefix=prefix))
        json_files = [b for b in blobs if b.name.endswith(".json")]

        if not json_files:
            self.log_event(Constants.Logger.INFO, Constants.Message.NO_EXISTING_FILES.format(table=table))
            return

        for blob in json_files:
            filename = blob.name.split("/")[-1]
            date_part = filename.split("_")[-1].replace(".json", "")

            year, month, day = date_part[-4:], date_part[2:4], date_part[0:2]

            archive_path = Constants.Common.ARCHIVE_FILE_PATH.format(
                hospital_name=HOSPITAL_NAME,
                table=table,
                year=year,
                month=month,
                day=day,
                filename=filename
            )

            dest_blob = self.bucket.blob(archive_path)
            self.bucket.copy_blob(blob, self.bucket, dest_blob.name)
            blob.delete()

            self.log_event(Constants.Logger.INFO, Constants.Message.FILES_MOVED.format(
                file=blob.name,
                archive_path=archive_path
            ), table=table)

    # ---------- Extract from MySQL & Save ----------
    def extract_and_save_to_landing(self, table, load_type, watermark_col):
        try:
            last_watermark = None
            if load_type.lower() == Constants.Common.INCREMENTAL:
                last_watermark = self.get_latest_watermark(table)

            # Build query
            if load_type.lower() == Constants.Common.FULL:
                query = Constants.Query.FILL_LOAD_QUERY.format(table=table)
            else:
                query = Constants.Query.INCREMENTAL_LOAD_QUERY.format(
                    table=table,
                    watermark_col=watermark_col,
                    last_watermark=last_watermark
                )

            # JDBC read (correct SQL wrapping)
            df = (
                self.spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", f"({query}) as t")
                .load()
            )

            self.log_event(Constants.Logger.SUCCES, Constants.Message.DATA_EXTRACTED.format(table=table))

            # Save to GCS without Pandas
            today = datetime.datetime.today().strftime("%d%m%Y")
            json_path = Constants.Common.LANDING_JSON_FILE_PATH.format(
                hospital_name=HOSPITAL_NAME,
                table=table,
                today=today
            )

            blob = self.bucket.blob(json_path)

            # Convert Spark DF → JSON string safely
            json_str = "\n".join(
                [row.json for row in df.selectExpr("to_json(struct(*)) as json").collect()]
            )

            blob.upload_from_string(json_str, content_type="application/json")

            self.log_event(
                Constants.Logger.SUCCES,
                Constants.Message.JSON_FILE_LOADED_TO_GCS.format(
                    gcs_bucket=GCS_BUCKET, json_file_path=json_path
                ),
                table=table
            )

            # Audit logging
            audit_df = self.spark.createDataFrame([
                (
                    HOSPITAL_NAME_A_DB,
                    table,
                    load_type,
                    df.count(),
                    datetime.datetime.now(),
                    Constants.Logger.SUCCES
                )
            ], ["data_source", "table_name", "load_type", "record_count", "load_timestamp", "status"])

            (
                audit_df.write.format(Constants.Common.BIGQUERY)
                .option(Constants.Common.TABLE, BQ_AUDIT_TABLE)
                .option(Constants.Common.TEMPORARYGCSBUCKET, GCS_BUCKET)
                .mode(Constants.Common.APPEND)
                .save()
            )

            self.log_event(Constants.Logger.SUCCES, Constants.Message.AUDIT_LOG_UPDATED.format(table=table))

        except Exception as e:
            self.log_event(Constants.Logger.ERROR, str(e), table=table)

job = Hospital_A_Job(spark)
job.start_job()

job.event_logger.save_logs_to_gcs(job.storage_client, GCS_BUCKET)
job.event_logger.save_logs_to_bigquery(
    spark, job.log_entries, BQ_LOG_TABLE, BQ_TEMP_PATH
)