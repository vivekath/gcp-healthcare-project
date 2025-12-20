from pyspark.sql import SparkSession
import argparse

from common_lib.spark_utils import get_spark
spark = get_spark("Healthcare_ETL_Job")

# -------------------------
# Argument parsing
# -------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--gcs_bucket", required=True, help="GCS bucket name")
parser.add_argument("--project_id", required=True, help="GCP project ID")
args = parser.parse_args()

GCS_BUCKET = args.gcs_bucket
BQ_PROJECT = args.project_id

CPT_BUCKET_PATH = f"gs://{GCS_BUCKET}/landing/cptcodes/*.csv"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.cpt_codes"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

cpt_code_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

for col in cpt_code_df.columns:
    new_col = col.replace(" ","_").lower()
    cpt_code_df = cpt_code_df.withColumnRenamed(col, new_col)

cpt_code_df.write.format("bigquery")\
        .option("table",BQ_TABLE)\
        .option("temporaryGcsBucket",BQ_TEMP_PATH)\
        .mode("overwrite")\
        .save()