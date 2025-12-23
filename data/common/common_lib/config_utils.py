import google.cloud.logging
import logging
import datetime
import json
from .gcs_utils import upload_string
from .constants import Constants
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType,LongType,DoubleType,DateType,TimestampType,
   BooleanType, BinaryType
)
import yaml

# Initialize Google Cloud Logging
logging_client = google.cloud.logging.Client()
logging_client.setup_logging()
logger = logging.getLogger('hospital-a-data-pipeline')
log_entries = []

# Logging helper function
def log_pipeline_step(step, message, level=Constants.Logger.INFO):
    if level == Constants.Logger.INFO or level == Constants.Logger.SUCCESS:
        logger.info(f"Step: {step}, Message: {message}")
    elif level == Constants.Logger.ERROR:
        logger.error(f"Step: {step}, Error: {message}")
    elif level == Constants:
        logger.warning(f"Step: {step}, Warning: {message}")

def log_event(event_type, message, table=None):
    """Log an event and store it in the log list"""
    log_entry = {
        "timestamp": datetime.datetime.now().isoformat(),
        "event_type": event_type,
        "message": message,
        "table": table
    }
    log_entries.append(log_entry)
    log_pipeline_step("Test Event Type", message, level=event_type)
    print(f"[{log_entry['timestamp']}] {event_type} - {message}")

def save_logs_to_gcs(storage_client, gcs_bucket):
    """Save logs to a JSON file and upload to GCS"""
    log_filename = (f"pipeline_log_{datetime.datetime.now().strftime('%Y%m%d%H%M%S')}.json")
    log_filepath = f"temp/pipeline_logs/{log_filename}"

    json_data = json.dumps(log_entries, indent=4)

    # Reuse common utility
    upload_string(
        client=storage_client,
        bucket_name=gcs_bucket,
        destination_blob=log_filepath,
        data=json_data,
        content_type="application/json"
    )

    print(f"✅ Logs successfully saved to gs://{gcs_bucket}/{log_filepath}")


def save_logs_to_bigquery(spark, bq_log_table, bq_temp_path):
    """Save logs to BigQuery"""
    if log_entries:
        schema = StructType([
            StructField("timestamp", StringType(), True),
            StructField("event_type", StringType(), True),
            StructField("message", StringType(), True),
            StructField("table", StringType(), True)
        ])
        log_df = spark.createDataFrame(log_entries, schema=schema)
        log_df.write.format("bigquery") \
            .option("table", bq_log_table) \
            .option("temporaryGcsBucket", bq_temp_path) \
            .mode("append") \
            .save()
        print("✅ Logs stored in BigQuery for future analysis")


def load_schema_from_yaml(yaml_filepath: str, schema_name: str) -> StructType:
    type_map = {
        "string": StringType(),
        "float": FloatType(),
        "integer": IntegerType(),
        "bigint": LongType(),
        "double": DoubleType(),
        "date": DateType(),
        "timestamp": TimestampType(),
        "boolean": BooleanType(),
        "binary": BinaryType(),
    }

    with open(yaml_filepath, "r") as file:
        config = yaml.safe_load(file)

    if schema_name not in config:
        raise ValueError(f"Schema '{schema_name}' not found in {yaml_filepath}")

    fields = []
    for field in config[schema_name]:
        fields.append(
            StructField(
                field["name"],
                type_map.get(field["type"].lower(), StringType()),
                field.get("nullable", True)
            )
        )

    return StructType(fields)
