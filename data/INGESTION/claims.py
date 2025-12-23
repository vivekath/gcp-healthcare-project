# =============================================================================
# Imports
# =============================================================================
from pyspark.sql.functions import input_file_name, when
import argparse

from common_lib.spark_utils import get_spark, read_csv
from common_lib.constants import Constants
import json
from pyspark.sql.types import (
    StructType, StructField,
    StringType, FloatType
)


# =============================================================================
# Argument Parsing
# =============================================================================
parser = argparse.ArgumentParser()
parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")
parser.add_argument("--project_id", required=True, help="GCP project ID")
parser.add_argument("--spark_config", required=True, help="Spark configuration", type=str)

args = parser.parse_args()


# =============================================================================
# Spark Initialization
# =============================================================================
spark_config_dict = json.loads(args.spark_config)

spark = get_spark(Constants.Common.APP_NAME.format(hospital_name="claims"), spark_config=spark_config_dict)

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
claims_schema = StructType([
    StructField("ClaimID", StringType(), True),
    StructField("TransactionID", StringType(), True),
    StructField("PatientID", StringType(), True),
    StructField("EncounterID", StringType(), True),
    StructField("ProviderID", StringType(), True),
    StructField("DeptID", StringType(), True),
    StructField("ServiceDate", StringType(), True),
    StructField("ClaimDate", StringType(), True),
    StructField("PayorID", StringType(), True),
    StructField("ClaimAmount", FloatType(), True),
    StructField("PaidAmount", FloatType(), True),
    StructField("ClaimStatus", StringType(), True),
    StructField("PayorType", StringType(), True),
    StructField("Deductible", FloatType(), True),
    StructField("Coinsurance", FloatType(), True),
    StructField("Copay", FloatType(), True),
    StructField("InsertDate", StringType(), True),
    StructField("ModifiedDate", StringType(), True),
    StructField("datasource", StringType(), False)  # REQUIRED
])

claims_df = read_csv(spark, CLAIMS_BUCKET_PATH, header=True, schema=claims_schema)


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