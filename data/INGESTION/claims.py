from pyspark.sql import SparkSession
from pyspark.sql.functions import input_file_name, when

spark = SparkSession.builder.appName("HospitalAMySQLToLanding").getOrCreate()

GCS_BUCKET = "healthcare-bucket-20122025"
CLAIMS_BUCKET_PATH = f"gs://{GCS_BUCKET}/landing/claims/*.csv"
BQ_PROJECT = "quantum-episode-345713"
BQ_TABLE = f"{BQ_PROJECT}.bronze_dataset.claims"
BQ_TEMP_PATH = f"{GCS_BUCKET}/temp/"

claims_df = spark.read.csv(CLAIMS_BUCKET_PATH, header=True)

claims_df = claims_df.withColumn("datasource", when(input_file_name().contains("hospital2"),"hosb")
                                .when(input_file_name().contains("hospital1"), "hosb").otherwise("None"))

claims_df = claims_df.drop_duplicates()

claims_df.write.format("bigquery")\
        .option("table",BQ_TABLE)\
        .option("temporaryGcsBucket",BQ_TEMP_PATH)\
        .mode("overwrite")\
        .save()