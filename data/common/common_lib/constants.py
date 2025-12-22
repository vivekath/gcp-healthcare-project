from enum import Enum
import os
from dotenv import load_dotenv

class Constants:

    class Logger:
        SUCCESS = "SUCCESS"
        INFO = "INFO"
        WARNING = "WARNING"
        ERROR = "ERROR"
        CRITICAL = "CRITICAL"
        DEBUG = "DEBUG"
        COMPONENT = "component"
        MESSAGE = "message"

    class SuccessMessage:
        DATA_EXTRACTED_MESSAGE = "✅ Successfully extracted data from {table}"
        JSON_FILE_WRITTEN_MESSAGE = (
            "✅ JSON file successfully written to gs://{gcs_bucket}/{json_file_path}"
        )
        AUDIT_LOG_UPDATED_MESSAGE = "✅ Audit log updated for {table}"
        CONFIG_FILE_READ_MESSAGE = "✅ Successfully read the config file"
        NO_EXISTING_FILES_MESSAGE = "No existing files for table {table}"
        FILE_MOVED_TO_ARCHIVE_MESSAGE = "Moved {file} to {archive_path}"
        LATEST_WATERMARK_MESSAGE = "Latest watermark for {table}: {last_watermark}"
        

    class ErrorMessage:
        ERROR_PROCESSING_TABLE_MESSAGE = "Error processing {table}: {error_msg}"

    class GCP:
        GCS_LANDING_PATH = "gs://{gcs_bucket}/landing/{hospital_name}/"
        GCS_ARCHIVE_PATH = "gs://{gcs_bucket}/landing/{hospital_name}/archive/"
        GCS_CONFIG_PATH = "gs://{gcs_bucket}/configs/load_config.csv"
        GCS_CLAIMS_PATH = "gs://{gcs_bucket}/landing/claims/*.csv"
        GCS_CPT_CODES_PATH = "gs://{gcs_bucket}/landing/cptcodes/*.csv"
    
    class BQ:
        AUDIT_LOG_TABLE = "{bq_project}.temp_dataset.audit_log"
        PIPELINE_LOGS_TABLE = "{bq_project}.temp_dataset.pipeline_logs"
        TEMP_PATH = "{gcs_bucket}/temp/"
        CLAIMS_TABLE = "{bq_project}.bronze_dataset.claims"
        CPT_CODES_TABLE = "{bq_project}.bronze_dataset.cpt_codes"
    
    class MySQL:
        JDBC_URL = "jdbc:mysql://{host}:{port}/{database}"
        DRIVER = "com.mysql.cj.jdbc.Driver"

    class SPARK:
        SPARK_JARS_PACKAGES_KEY = "spark.jars.packages"
        SPARK_JARS_KEY = "spark.jars"
        BIGQUERY_PACKAGE = (
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"
        )
        APP_NAME = "Data_Analytics"
        SPARK_BQ_PROJECT_KEY = "spark.bigquery.project"
        SPARK_BQ_TMPGCS_KEY = "spark.bigquery.temporaryGcsBucket"
        MAX_TO_STRING_FIELDS_KEY = "spark.sql.debug.maxToStringFields"
        MAX_TO_STRING_FIELDS = 2000
        FS_GS_IMPL_KEY = "fs.gs.impl"
        FS_GS_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        FS_ABSTRACT_GS_IMPL_KEY = "fs.AbstractFileSystem.gs.impl"
        FS_ABSTRACT_GS_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        MEMORY_OVERHEAD_KEY ="spark.executor.memoryOverhead"
        EXECUTOR_MEMORY_OVERHEAD_SIZE = "4g"
        DRIVER_MEMORY_SIZE = "32g"
        DRIVER_MEMORY_KEY ="spark.driver.memory"

    class Common:
        APP_NAME = "Healthcare_ETL_{hospital_name}_Job"
        DEFAULT_WATERMARK = "1900-01-01 00:00:00"
        DEFAULT_LOAD_TYPE = "incremental"
        FULL_LOAD_TYPE = "full"
        IS_ACTIVE_COLUMN = "is_active"
        DATA_SOURCE_COLUMN = "datasource"


    class Query:
        INCREMENTAL_TABLE_QUERY = "(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
        FULL_TABLE_QUERY = "(SELECT * FROM {table}) AS t"
        WATERMARK_QUERY = """
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{BQ_AUDIT_TABLE}`
        WHERE table_name = '{table_name}' and data_source = "{hospital_db}"
        """
        AUDIT_QUERY = "SELECT COUNT(*) as total_records FROM {table_name}"
        AUDIT_LOAD_QUERY = "select * from {audit_table_ref} where target_table = '{target_table}' and load_date >= '{load_date}'"
        COUNT_QUERY = "SELECT COUNT(*) FROM ({query})"

    class Schema:
        STRING = "string"
        INTEGER = "integer"
        DOUBLE = "double"
        DATE = "date"
        TIMESTAMP = "timestamp"
        DATETIME = "datetime"
        BOOLEAN = "boolean"
        FLOAT = "float"
        JSON = "json"
        NUMERIC = "numeric"
        REQUIRED_KEY = "REQUIRED"
        NULLABLE_KEY = "NULLABLE"

    class ResponseStatusCode(Enum):
        OK = 200
        CREATED = 201
        ACCEPTED = 202
        NO_CONTENT = 204
        BAD_REQUEST = 400
        UNAUTHORIZED = 401
        FORBIDDEN = 403
        NOT_FOUND = 404
        METHOD_NOT_ALLOWED = 405
        CONFLICT = 409
        INTERNAL_SERVER_ERROR = 500
        NOT_IMPLEMENTED = 501
        BAD_GATEWAY = 502
        SERVICE_UNAVAILABLE = 503
        GATEWAY_TIMEOUT = 504
        HTTP_VERSION_NOT_SUPPORTED = 505

    class TransformationType(Enum):
        UPPERCASE = "uppercase"
        LOWERCASE = "lowercase"
        TRIM = "trim"
        DATE_FORMAT = "date_format"
        REPLACE_PATTERN = "replace_pattern"
        DEFAULT_VALUE = "default_value"

    class Extensions:
        CSV = "csv"
        JSON = "json"
        YAML = "yaml"
        TEXT = "txt"
        SQL_EXTENSION = "sql"
