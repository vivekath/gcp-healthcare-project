from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when
from constants import Constants

spark = SparkSession.builder.appName(Constants.SPARK.APP_CLAIMS_TO_BRONZE).getOrCreate()

GCS_BUCKET = Constants.GCP.GCS_BUCKET_KEY
CLAIMS_BUCKET_PATH = Constants.Common.GCS_LANDING_CLAIM.format(gcs_bucket=GCS_BUCKET)
BQ_PROJECT = Constants.GCP.BQ_PROJECT_ID
BQ_TABLE = Constants.Common.BQ_CLAIM_TABLE.format(bq_project=BQ_PROJECT)
BQ_TEMP_PATH = Constants.Common.BQ_TEMP_PATH.format(gcs_bucket=GCS_BUCKET)

claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

claims_df = claims_df.withColumn(Constants.Common.DATASOURCE, when(input_file_name().contains(Constants.Common.HOSPITAL_2),Constants.Common.HOSB)
                                .when(input_file_name().contains(Constants.Common.HOSPITAL_1), Constants.Common.HOSA).otherwise("None"))

claims_df = claims_df.drop_duplicates()

claims_df.write.format(Constants.Common.BIGQUERY)\
        .option(Constants.Common.TABLE,BQ_TABLE)\
        .option(Constants.Common.TEMPORARYGCSBUCKET,BQ_TEMP_PATH)\
        .mode(Constants.Common.OVERWRITE)\
        .save()