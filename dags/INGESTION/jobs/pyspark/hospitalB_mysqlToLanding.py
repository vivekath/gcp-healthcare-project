from google.cloud import storage, bigquery
import pandas as pd
from pyspark.sql import SparkSession
import datetime
import json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType,TimestampType
from .constants import Constants

storage_client = storage.Client()
bq_client = bigquery.Client()

spark = SparkSession.builder.appName(Constants.SPARK.APP_NAME_HOSPITAL_B).getOrCreate()

GCS_BUCKET = Constants.GCP.GCS_BUCKET_KEY
HOSPITAL_NAME = Constants.Common.HOSPITAL_NAME_B
HOSPITAL_NAME_B_DB = Constants.Common.HOSPITAL_NAME_B_DB
LANDING_PATH = Constants.Common.LANDING_PATH.format(gcs_bucket=GCS_BUCKET, hospital_name=HOSPITAL_NAME)
ARCHIVE_PATH = Constants.Common.ARCHIVE_PATH.format(gcs_bucket=GCS_BUCKET, hospital_name=HOSPITAL_NAME)
CONFIG_FILE_PATH = Constants.Common.CONFIG_FILE_PATH.format(gcs_bucket=GCS_BUCKET)

BQ_PROJECT = Constants.GCP.BQ_PROJECT_ID
BQ_AUDIT_TABLE = Constants.Common.BQ_AUDIT_TABLE.format(bq_project=BQ_PROJECT) 
BQ_LOG_TABLE = Constants.Common.BQ_LOG_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.Common.BQ_TEMP_PATH.format(gcs_bucket=GCS_BUCKET)

MYSQL_CONFIG = {
    "url": Constants.MySQL.URL_B.format(hospital_db=HOSPITAL_NAME_B_DB),
    "driver": Constants.MySQL.DRIVER,
    "user": Constants.MySQL.USER_B,
    "password": Constants.MySQL.PASSWORD_B
}

bucket = storage_client.bucket(GCS_BUCKET)
    
log_entries = []

def log_event(event_type, message, table=None):
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    print(Constants.Common.PRINT_STATEMENT.format(timestamp=log_entry['timestamp'], event_type=event_type, message=message))
    
def read_config_file():
    df = spark.read.format('csv').option('header', 'true').load(CONFIG_FILE_PATH)
    log_event(Constants.Logger.INFO, Constants.Message.CONFIG_FILE_READ_MESSAGE)
    return df

def save_logs_to_gcs():
    log_filename = Constants.Common.LOG_FILENAME.format(timestamp=datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
    log_filepath = Constants.Common.LOG_FILEPATH.format(log_filename=log_filename)
    
    json_data = json.dumps(log_entries, indent=4)
    blob = bucket.blob(log_filepath)
    blob.upload_from_string(json_data, content_type="application/json")
    
    print(Constants.Message.LOGS_SAVED_TO_GCS_MESSAGE.format(gcs_bucket=GCS_BUCKET, log_filepath=log_filepath))


def save_logs_to_bigquery():
    if log_entries:       
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message", StringType(), True),
            StructField("table", StringType(), True)
        ])
        log_df = spark.createDataFrame(log_entries, schema=schema)
        log_df.write.format(Constants.Common.BIGQUERY)\
            .option(Constants.Common.TABLE, BQ_LOG_TABLE, BQ_LOG_TABLE) \
            .option(Constants.Common.TEMPORARYGCSBUCKET, BQ_TEMP_PATH) \
            .mode(Constants.Common.APPEND) \
            .save()
        print(Constants.Message.LOGS_STORED_IN_BQ)

        
def move_existing_files_to_archive(table):
    blobs = list(bucket.list_blobs(prefix=Constants.Common.LANDING_TABLE_PATH.format(hospital_name=HOSPITAL_NAME,table=table)))
    existing_files = [blob.name for blob in blobs if blob.name.endswith(f".{Constants.Extensions.JSON}")]    
    
    if not existing_files:
        log_event(Constants.Logger.INFO, Constants.Message.NO_EXISTING_FILES.format(table=table))
        return
    
    for file in existing_files:
        source_blob = bucket.blob(file)
        
        date_part = file.split("_")[-1].split(".")[0]
        year, month, day = date_part[-4:], date_part[2:4], date_part[:2]
        archive_path = Constants.Common.ARCHIVE_FILE_PATH.format(hospital_name=HOSPITAL_NAME,table=table,year=year,month=month,day=day,filename=file.split('/')[-1]) 
        destination_blob = bucket.blob(archive_path)                
        bucket.copy_blob(source_blob, bucket, destination_blob.name)
        source_blob.delete()

        log_event(Constants.Logger.INFO, Constants.Message.FILES_MOVED.format(file=file,archive_path=archive_path), table=table)

def get_latest_watermark(table_name):
    query = Constants.Query.AUDIT_QUERY.format(bq_audit_table=BQ_AUDIT_TABLE, table_name=table_name,hospital_db=HOSPITAL_NAME_B_DB)

    query_job = bq_client.query(query)
    result = query_job.result()
    
    for row in result:
        return row.latest_timestamp if row.latest_timestamp else Constants.Common.DEFAULT_DATE
    return Constants.Common.DEFAULT_DATE
    
    
def extract_and_save_to_landing(table, load_type, watermark_col):
    try:
        last_watermark = get_latest_watermark(table) if load_type.lower() == Constants.Common.INCREMENTAL else None
        log_event(Constants.Logger.INFO, Constants.Message.LATEST_WATERMARK.format(table=table,last_watermark=last_watermark), table=table)

        query = Constants.Query.FILL_LOAD_QUERY.format(table=table) if load_type.lower() == Constants.Common.FULL else \
                Constants.Query.INCREMENTAL_LOAD_QUERY.format(table=table,watermark_col=watermark_col,last_watermark=last_watermark)
                
        df = (spark.read.format("jdbc")
                .option("url", MYSQL_CONFIG["url"])
                .option("user", MYSQL_CONFIG["user"])
                .option("password", MYSQL_CONFIG["password"])
                .option("driver", MYSQL_CONFIG["driver"])
                .option("dbtable", query)
                .load())
        
        log_event(Constants.Logger.SUCCES, Constants.Message.DATA_EXTRACTED.format(table=table), table=table)     

        today = datetime.datetime.today().strftime('%d%m%Y')
        JSON_FILE_PATH = Constants.Common.LANDING_JSON_FILE_PATH.format(hospital_name=HOSPITAL_NAME,table=table,table=table,today=today)
        blob = bucket.blob(JSON_FILE_PATH)
        blob.upload_from_string(df.toPandas().to_json(orient="records", lines=True), content_type="application/json")

        log_event(Constants.Logger.SUCCES, Constants.Message.JSON_FILE_LOADED_TO_GCS.format(gcs_bucket=GCS_BUCKET, json_file_path=JSON_FILE_PATH), table=table)
        
        schema_audit = StructType([
            StructField("data_source", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("load_type", StringType(), True),
            StructField("record_count", IntegerType(), True),
            StructField("load_timestamp", TimestampType(), True),
            StructField("status", StringType(), True)
        ])
        
        audit_df = spark.createDataFrame(
            [(HOSPITAL_NAME_B_DB, table, load_type, df.count(), datetime.datetime.now(), Constants.Logger.SUCCES)], schema=schema_audit)
        
        audit_df.write.format(Constants.Common.BIGQUERY)\
                    .option(Constants.Common.TABLE, BQ_AUDIT_TABLE)\
                    .option(Constants.Common.TEMPORARYGCSBUCKET, GCS_BUCKET)\
                    .mode(Constants.Common.APPEND)\
                    .save()

        log_event(Constants.Logger.SUCCES, Constants.Message.AUDIT_LOG_UPDATED.format(table=table), table=table)

    except Exception as e:
        log_event(Constants.Logger.ERROR, Constants.Message.ERROR_MESSAGE.format(table=table,str=str(e)), table=table)

config_df = read_config_file()
config_df.show()

# config_df.createOrReplaceTempView("config_table")
# spark.sql("SELECT * FROM config_table").show()

for row in config_df.collect():
    if row[Constants.Common.IS_ACTIVE] == "1" and row.asDict().get(Constants.Common.DATASOURCE, "") == HOSPITAL_NAME_B_DB:
        db, src, table, load_type, watermark, _, targetpath = row
        move_existing_files_to_archive(table)
        extract_and_save_to_landing(table, load_type, watermark)

save_logs_to_gcs()
save_logs_to_bigquery()