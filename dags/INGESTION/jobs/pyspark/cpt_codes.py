from pyspark.sql import SparkSession
from .constants import Constants

spark = SparkSession.builder.appName(Constants.SPARK.APP_CPT_CODES_TO_BRONZE).getOrCreate()

GCS_BUCKET = Constants.GCP.GCS_BUCKET_KEY
CPT_BUCKET_PATH = Constants.Common.GCS_LANDING_CPT_CODE.format(gcs_bucket=GCS_BUCKET)
BQ_PROJECT = Constants.GCP.BQ_PROJECT_ID
BQ_TABLE = Constants.Common.BQ_CPT_CODE_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.Common.BQ_TEMP_PATH.format(gcs_bucket=GCS_BUCKET)

cpt_code_df = spark.read.csv(CPT_BUCKET_PATH, header=True)

for col in cpt_code_df.columns:
    new_col = col.replace(" ","_").lower()
    cpt_code_df = cpt_code_df.withColumnRenamed(col, new_col)

cpt_code_df.write.format(Constants.Common.BIGQUERY)\
        .option(Constants.Common.TABLE,BQ_TABLE)\
        .option(Constants.Common.TEMPORARYGCSBUCKET,BQ_TEMP_PATH)\
        .mode(Constants.Common.OVERWRITE)\
        .save()