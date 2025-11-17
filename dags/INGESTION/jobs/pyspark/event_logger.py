import json
from .constants import Constants

class EventLogger:
    def __init__(self):  
        pass

    def save_logs_to_gcs(self, storage_client,gcs_bucket):
        """Save logs to a JSON file and upload to GCS"""
        log_filename = Constants.Common.LOG_FILENAME.format(timestamp=datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
        log_filepath = Constants.Common.LOG_FILEPATH.format(log_filename=log_filename)
        
        json_data = json.dumps(self.log_entries, indent=4)

        # Get GCS bucket
        bucket = storage_client.bucket(gcs_bucket)
        blob = bucket.blob(log_filepath)
        
        # Upload JSON data as a file
        blob.upload_from_string(json_data, content_type="application/json")
        print(Constants.Message.LOGS_SAVED_TO_GCS_MESSAGE.format(gcs_bucket=GCS_BUCKET, log_filepath=log_filepath))

    def save_logs_to_bigquery(self, spark, log_entries, bq_log_table, bq_temp_path):
        """Save logs to BigQuery"""
        if log_entries:
            log_df = spark.createDataFrame(self.log_entries)
            log_df.write.format(Constants.Common.BIGQUERY) \
                .option(Constants.Common.TABLE, bq_log_table) \
                .option(Constants.Common.TEMPORARYGCSBUCKET, bq_temp_path) \
                .mode(Constants.Common.APPEND) \
                .save()
            print(Constants.Message.LOGS_STORED_IN_BQ)
