# =============================================================================
# Imports
# =============================================================================
import argparse

from common_lib.spark_utils import get_spark, read_csv
from common_lib.constants import Constants
import json
from pyspark.sql.types import (
    StructType, StructField,
    StringType
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

spark = get_spark(Constants.Common.APP_NAME.format(hospital_name="cpt_codes"), spark_config=spark_config_dict)

# =============================================================================
# Configuration
# =============================================================================
GCS_BUCKET = args.gcs_bucket
BQ_PROJECT = args.project_id

CPT_BUCKET_PATH = Constants.GCP.GCS_CPT_CODES_PATH.format(gcs_bucket=GCS_BUCKET)
BQ_TABLE = Constants.BQ.CPT_CODES_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.BQ.TEMP_PATH.format(gcs_bucket=GCS_BUCKET)


# =============================================================================
# Read CPT Codes Data
# =============================================================================
cpt_codes_schema = StructType([
    StructField("procedure_code_category", StringType(), True),
    StructField("cpt_codes", StringType(), True),
    StructField("procedure_code_descriptions", StringType(), True),
    StructField("code_status", StringType(), True)
])
cpt_code_df = read_csv(spark, CPT_BUCKET_PATH, header=True, schema=cpt_codes_schema)


# =============================================================================
# Normalize Column Names
# =============================================================================
for col in cpt_code_df.columns:
    new_col = col.replace(" ", "_").lower()
    cpt_code_df = cpt_code_df.withColumnRenamed(col, new_col)


# =============================================================================
# Write to BigQuery
# =============================================================================
(
    cpt_code_df.write.format("bigquery")
    .option("table", BQ_TABLE)
    .option("temporaryGcsBucket", BQ_TEMP_PATH)
    .mode("overwrite")
    .save()
)

# =============================================================================
# Stop Spark Session
# =============================================================================
spark.stop()