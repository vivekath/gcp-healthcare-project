# =============================================================================
# Imports
# =============================================================================
from pyspark.sql.functions import input_file_name, when
import argparse

from common_lib.spark_utils import get_spark, read_csv
from common_lib.constants import Constants


# =============================================================================
# Spark Initialization
# =============================================================================
spark = get_spark(Constants.Common.APP_NAME.format(hospital_name="claims"))


# =============================================================================
# Argument Parsing
# =============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")
parser.add_argument("--project_id", required=True, help="GCP project ID")
args = parser.parse_args()


# =============================================================================
# Configuration
# =============================================================================
GCS_BUCKET = args.gcs_bucket
BQ_PROJECT = args.project_id

CLAIMS_BUCKET_PATH = Constants.GCP.GCS_CLAIMS_PATH.format(gcs_bucket=GCS_BUCKET)
BQ_TABLE = Constants.BQ.CLAIMS_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.BQ.TEMP_PATH.format(gcs_bucket=GCS_BUCKET)


# =============================================================================
# Read Claims Data from GCS
# =============================================================================
claims_df = read_csv(spark, CLAIMS_BUCKET_PATH, header=True) 


# =============================================================================
# Add Datasource Column
# =============================================================================
claims_df = claims_df.withColumn(
    Constants.Common.DATA_SOURCE_COLUMN,
    when(input_file_name().contains("hospital2"), "hosb")
    .when(input_file_name().contains("hospital1"), "hosb")
    .otherwise("None")
)


# =============================================================================
# Remove Duplicate Records
# =============================================================================
claims_df = claims_df.drop_duplicates()


# =============================================================================
# Write to BigQuery
# =============================================================================
(
    claims_df.write.format("bigquery")
    .option("table", BQ_TABLE)
    .option("temporaryGcsBucket", BQ_TEMP_PATH)
    .mode("overwrite")
    .save()
)

# =============================================================================
# Stop Spark Session
# =============================================================================
spark.stop()