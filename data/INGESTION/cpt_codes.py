from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("HospitalAMySQLToLanding").getOrCreate()

GCS_BUCKET = "healthcare-bucket-20122025"
CPT_BUCKET_PATH = f"gs://{GCS_BUCKET}/landing/cptcodes/*.csv"
BQ_PROJECT = "quantum-episode-345713"
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