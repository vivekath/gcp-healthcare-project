from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
import google.cloud.logging
import logging

storage_client = storage.Client()
bq_client = bigquery.Client()

spark = SparkSession.builder.appName("HospitalBMySQLToLanding").getOrCreate()

# Initialize Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('hospital-a-data-pipeline')

GCS_BUCKET = "healthcare-bucket-20122025"
HOSPITAL_NAME = "hospital-b"
LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"
CONFIG_FILE_PATH = f"gs://{GCS_BUCKET}/configs/load_config.csv"

BQ_PROJECT = "quantum-episode-345713"
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"
BQ_LOG_TABLE = f"{BQ_PROJECT}.temp_dataset.pipeline_logs"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp"

MYSQL_CONFIG = {
    "url": "jdbc:mysql://34.14.185.46:3306/hospital_b_db",
    "driver": "com.mysql.cj.jdbc.Driver",
    "user": "myuser",
    "password": "asdf@ATH10"
}

bucket = storage_client.bucket(GCS_BUCKET)
    
# Logging helper function
def log_pipeline_step(step, message, level='INFO'):
    if level == 'INFO' or level == 'SUCCESS':
        logger.info(f"Step: {step}, Message: {message}")
    elif level == 'ERROR':
        logger.error(f"Step: {step}, Error: {message}")
    elif level == 'WARNING':
        logger.warning(f"Step: {step}, Warning: {message}")

log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    log_pipeline_step("Test Event Type", message, level=event_type)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")
    
def read_config_file():
    df = spark.read.format('csv').option('header', 'true').load(CONFIG_FILE_PATH)
    log_event("INFO", "✅ Successfully read the config file")
    return df

def save_logs_to_gcs():
    log_filename = f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json"
    log_filepath = f"temp/pipeline_logs/{log_filename}"
    
    json_data = json.dumps(log_entries, indent=4)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    
    print(f"✅ Logs successfully saved to GCS at gs://{GCS_BUCKET}/{log_filepath}")

def save_logs_to_bigquery():
    if log_entries:       
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message", StringType(), True),
            StructField("table", StringType(), True)
        ])
        log_df = spark.createDataFrame(log_entries, schema=schema)
        log_df.write.format("bigquery")\
            .option("table", BQ_LOG_TABLE) \
            .option("temporaryGcsBucket", BQ_TEMP_PATH) \
            .mode("append") \
            .save()
        print("✅ Logs stored in BigQuery for future analysis")

        
def move_existing_files_to_archive(table):
    blobs = list(bucket.list_blobs(prefix=f"landing/{HOSPITAL_NAME}/{table}/"))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(".json")]    
    
    if not existing_files:
        log_event("INFO", f"No existing files for table {table}")
        return
    
    for file in existing_files:
        source_blob = bucket.blob(file)
        
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        archive_path = f"landing/{HOSPITAL_NAME}/archive/{table}/{year}/{month}/{day}/{file.split('/')[-1]}"
        destination_blob = bucket.blob(archive_path)                
        bucket.copy_blob(source_blob, bucket, destination_blob.name)
        source_blob.delete()
        
        log_event("INFO", f"Moved {file} to {archive_path}", table=table)

def get_latest_watermark(table_name):
    query = f"""
    SELECT MAX(load_timestamp) AS latest_timestamp
    FROM `{BQ_AUDIT_TABLE}`
    WHERE table_name = "{table_name}" AND data_source = "hospital_b_db"
    """
    
    query_job = bq_client.query(query)
    result = query_job.result()
    
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else "1900-01-01 00:00:00"
    return "1900-01-01 00:00:00"
    
    
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == "incremental" else None
        log_event("INFO", f"Latest watermark for {table}: {last_watermark}", table=table)    
        
        query = f"(SELECT * FROM {table}) AS t" if load_type.lower() == "full" else \
                f"(SELECT * FROM {table} WHERE {watermark_col} > '{last_watermark}') AS t"
                
        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())
        log_event("SUCCESS", f"✅ Successfully extracted data from {table}", table=table)
        
        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = f"landing/{HOSPITAL_NAME}/{table}/{table}_{today}.json"
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")

        log_event("SUCCESS", f"✅ JSON file successfully written to gs://{GCS_BUCKET}/{JSON_FILE_PATH}", table=table)
        
        schema_audit = StructType([
            StructField("data_source", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("load_timestamp", TimestampType(), True),
            StructField("status", StringType(), True)
        ])
        
        audit_df = spark.createDataFrame(
            [("hospital_b_db", table, load_type, df.count(), datetime.datetime.now(), "SUCCESS")], schema=schema_audit)
        
        audit_df.write.format("bigquery")\
                    .option("table", BQ_AUDIT_TABLE)\
                    .option("temporaryGcsBucket", GCS_BUCKET)\
                    .mode("append")\
                    .save()

        log_event("SUCCESS", f"✅ Audit log updated for {table}", table=table)
        
    except Exception as e:
        log_event("ERROR", f"Error processing {table}: {str(e)}", table=table)

config_df = read_config_file()
config_df.show()

# config_df.createOrReplaceTempView("config_table")
# spark.sql("SELECT * FROM config_table").show()

for row in config_df.collect():
    if row["is_active"] == "1" and row.asDict().get("datasource", "") == "hospital_b_db":
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()