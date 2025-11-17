from enum import Enum
import os
from dotenv import load_dotenv
from datetime import timedelta
from airflow.utils.dates import days_ago

class Constants:
    class DAG_ARGS:
        OWNER = "VIVEK ATHILKAR"
        START_DATE = None
        DEPENDS_ON_PAST = False
        EMAIL_ON_FAILURE = False
        EMAIL_ON_RETRY = False
        EMAIL = ["vivekneosoft@gmail.com"]
        EMAIL_ON_SUCCESS = False
        RETRIES = 1
        RETRY_DELAY = timedelta(minutes=5)
        START_DATE_DAYS_AGO  = days_ago(1)
    
    class Parent_DAG:
        DAG_ID="parent_dag"
        SCHEDULE_INTERVAL="0 5 * * *"
        DESCRIPTION="Parent DAG to trigger PySpark and BigQuery DAGs"
        TAGS=["parent", "orchestration", "etl"]
        TRIGGER_PYSPARK_DAG = "trigger_pyspark_dag"
        TRIGGER_BIGQUERY_DAG = "trigger_bigquery_dag"
    
    class Pyspark_DAG:
        DAG_ID = "pyspark_dag"
        SCHEDULE_INTERVAL=None
        DESCRIPTION="DAG to start a Dataproc cluster, run PySpark jobs, and stop the cluster"
        TAGS=["pyspark", "dataproc", "etl", "marvel"]
        START_CLUSTER = "start_cluster"
        PYSPARK_TASK_1 = "pyspark_task_1"
        PYSPARK_TASK_2 = "pyspark_task_2"
        PYSPARK_TASK_3 = "pyspark_task_3"
        PYSPARK_TASK_4 = "pyspark_task_4"
        STOP_CLUSTER = "stop_cluster"
        PY_SPARK_AIRFLOW_COMPOSER_BUCKET_FILE_PATH = "gs://{composer_bucket}/data/INGESTION/jobs/pyspark/{job_name}.py"

    
    class BQ_DAG:
        DAG_ID = "bigquery_dag"
        SCHEDULE_INTERVAL=None
        DESCRIPTION="DAG to run the bigquery jobs"
        TAGS=["gcs", "bq", "etl", "marvel"]
        BQ_SQL_AIRFLOW_COMPOSER_FILE_PATH = "/home/airflow/gcs/data/INGESTION/jobs/bq/{job_name}.sql"
        BRONZE_TABLES = "bronze_tables"
        SILVER_TABLES = "silver_tables"
        GOLD_TABLES = "gold_tables"
        PRIORITY = "BATCH"
        USELEGACYSQL = False


    class Jobs:
        BRONZE = "bronze"
        SILVER = "silver"
        GOLD = "gold"
        HOSPITALA_MYSQLTOLANDING = "hospitalA_mysqlToLanding"
        HOSPITALB_MYSQLTOLANDING = "hospitalB_mysqlToLanding"
        CLAIMS = "claims"
        CPT_CODES = "cpt_codes"

    class Query:
        AUDIT_QUERY = """
        SELECT MAX(load_timestamp) AS latest_timestamp
        FROM `{bq_audit_table}`
        WHERE table_name = '{table_name}' and data_source = '{hospital_db}'
        """
        FILL_LOAD_QUERY = "(SELECT * FROM {table}) AS t"
        INCREMENTAL_LOAD_QUERY = "(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"

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

    class Logger:
        INFO = "INFO"
        WARNING = "WARNING"
        ERROR = "ERROR"
        CRITICAL = "CRITICAL"
        DEBUG = "DEBUG"
        COMPONENT = "component"
        MESSAGE = "message"
        SUCCES = "SUCCESS"

    
    class TransformationType(Enum):
        UPPERCASE = "uppercase"
        LOWERCASE = "lowercase"
        TRIM = "trim"
        DATE_FORMAT = "date_format"
        REPLACE_PATTERN = "replace_pattern"
        DEFAULT_VALUE = "default_value"
    
    class Message:
        CONFIG_FILE_READ_MESSAGE = "✅ Successfully read the config file"
        LOGS_SAVED_TO_GCS_MESSAGE = "✅ Logs successfully saved to GCS at gs://{gcs_bucket}/{log_filepath}"
        LOGS_STORED_IN_BQ = "✅ Logs stored in BigQuery for future analysis"
        NO_EXISTING_FILES = "No existing files for table {table}"
        FILES_MOVED = "Moved {file} to {archive_path}"
        LATEST_WATERMARK = "Latest watermark for {table}: {last_watermark}"
        DATA_EXTRACTED = "✅ Successfully extracted data from {table}"
        JSON_FILE_LOADED_TO_GCS = "✅ JSON file successfully written to gs://{gcs_bucket}/{json_file_path}"
        AUDIT_LOG_UPDATED = "✅ Audit log updated for {table}"
        ERROR_MESSAGE = "Error processing {table}: {str}"
    
    class GCP:
        load_dotenv("/tmp/.env")
        GCS_CONNECTOR_JAR_PATH = os.getenv("GCS_CONNECTOR_JAR_PATH")
        GCS_BUCKET_KEY = "heathcare-bucket-12112025"
        BQ_PROJECT_ID = "quantum-episode-345713"
        REGION = "us-east1"
        CLUSTER_NAME = "my-demo-cluster2"
        COMPOSER_BUCKET = "us-central1-demo-instance-1bf3538f-bucket"
        LOCATION = "US"


    class SPARK:
        SPARK_JARS_PACKAGES_KEY = "spark.jars.packages"
        SPARK_JARS_KEY = "spark.jars"
        BIGQUERY_PACKAGE = (
            "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.34.0"
        )
        APP_NAME_HOSPITAL_A = "HospitalAMySQLToLanding"
        APP_NAME_HOSPITAL_B = "HospitalBMySQLToLanding"
        APP_CLAIMS_TO_BRONZE = "ClaimsToBronze"
        APP_CPT_CODES_TO_BRONZE = "CptCodesToBronze"
        SPARK_BQ_PROJECT_KEY = "spark.bigquery.project"
        SPARK_BQ_TMPGCS_KEY = "spark.bigquery.temporaryGcsBucket"
        MAX_TO_STRING_FIELDS_KEY = "spark.sql.debug.maxToStringFields"
        MAX_TO_STRING_FIELDS = 2000
        FS_GS_IMPL_KEY = "fs.gs.impl"
        FS_GS_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem"
        FS_ABSTRACT_GS_IMPL_KEY = "fs.AbstractFileSystem.gs.impl"
        FS_ABSTRACT_GS_IMPL = "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS"
        MEMORY_OVERHEAD_KEY ="spark.executor.memoryOverhead"
        EXECUTOR_MEMORY_OVERHEAD_SIZE = "2g"
        DRIVER_MEMORY_SIZE = "16g"
        DRIVER_MEMORY_KEY ="spark.driver.memory"
        SPARK_BQ_PROJECT_KEY = "spark.bigquery.project"
        SPARK_BQ_TMPGCS_KEY = "spark.bigquery.temporaryGcsBucket"
        EXECUTOR_MEMORY_OVERHEAD_SIZE = "2g"
        DRIVER_MEMORY_SIZE = "16g"
        MEMORY_EXECUTOR_SIZE = "4g"

        DRIVER_MEMORY_KEY ="spark.driver.memory"
        MEMORY_EXECUTOR_KEY = "spark.executor.memory"
    
    class MySQL:
        DRIVER = "com.mysql.cj.jdbc.Driver"

        URL_A = "jdbc:mysql://34.63.194.202:3306/{hospital_db}"
        USER_A = "myuser"
        PASSWORD_A = "asdf@ATH10"

        URL_B = "jdbc:mysql://34.63.194.202:3306/{hospital_db}"
        USER_B = "myuser"
        PASSWORD_B = "asdf@ATH10"

    class Common:
        PROJECT_ID_KEY = "project_id"
        DATASET_ID_KEY = "dataset_id"
        HOSPITAL_NAME_A = "hospital-a"
        HOSPITAL_NAME_B = "hospital-b"
        HOSPITAL_NAME_A_DB = "hospital_a_db"
        HOSPITAL_NAME_B_DB = "hospital_b_db"
        LANDING_PATH = "gs://{gcs_bucket}/landing/{hospital_name}/"
        ARCHIVE_PATH = "gs://{gcs_bucket}/landing/{hospital_name}/archive/"
        CONFIG_FILE_PATH = "gs://{gcs_bucket}/configs/load_config.csv"
        BQ_AUDIT_TABLE = "{bq_project}.temp_dataset.audit_log"
        BQ_LOG_TABLE = "{bq_project}.temp_dataset.pipeline_logs"
        BQ_TEMP_PATH = "{gcs_bucket}/temp/"
        PRINT_STATEMENT = "[{timestamp}] {event_type} - {message}"
        LOG_FILENAME = "pipeline_log_{timestamp}.json"
        LOG_FILEPATH = "temp/pipeline_logs/{log_filename}"
        BIGQUERY = "bigquery"
        TABLE = "table"
        TEMPORARYGCSBUCKET = "temporaryGcsBucket"
        APPEND = "append"
        OVERWRITE = "overwrite"
        LANDING_TABLE_PATH = "landing/{hospital_name}/{table}/"
        DEFAULT_DATE = "1900-01-01 00:00:00"
        ARCHIVE_FILE_PATH =  "landing/{hospital_name}/archive/{table}/{year}/{month}/{day}/{filename}"
        INCREMENTAL = "incremental"
        FULL = "full"
        LANDING_JSON_FILE_PATH = "landing/{hospital_name}/{table}/{table}_{today}.json"
        IS_ACTIVE = "is_active"
        DATASOURCE = "datasource"
        BQ_CPT_CODE_TABLE = "{bq_project}.bronze_dataset.cpt_codes"
        GCS_LANDING_CPT_CODE = "gs://{gcs_bucket}/landing/cptcodes/*.csv"

        BQ_CLAIM_TABLE = "{bq_project}.bronze_dataset.claims"
        GCS_LANDING_CLAIM = "gs://{gcs_bucket}/landing/claims/*.csv"
        HOSPITAL_2 = "hospital2"
        HOSPITAL_1 = "hospital1"
        HOSB = "hosb"
        HOSA = "hosa"
        
    class Extensions:
        CSV = "csv"
        JSON = "json"
        YAML = "yaml"
        TEXT = "txt"
        SQL_EXTENSION = "sql"

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
