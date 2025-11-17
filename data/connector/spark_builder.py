from pyspark.sql import SparkSession
from data.configs.constants import Constants

class SparkBuilder:
    def __init__(self):
        # os.environ[Constants.GCP.GOOGLE_APPLICATION_CREDENTIALS] = "/tmp/lrdataanalyticsdw-de-us-prod.json"
        pass

    def build_spark_session(self, app_name: str, project_id: str, gcs_temp_bucket: str): 
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark


    # def get_spark_session_dataproc(app_name: str, project_id: str, gcs_temp_bucket: str):
    #         try:
    #             spark = (
    #                 SparkSession.builder
    #                     .appName(app_name)

    #                     # --- BigQuery Required Configuration ---
    #                     .config(Constants.SPARK.SPARK_BQ_PROJECT_KEY, project_id)
    #                     .config(Constants.SPARK.SPARK_BQ_TMPGCS_KEY, gcs_temp_bucket)

    #                     # --- Performance / JVM Memory Settings ---
    #                     .config(Constants.SPARK.MEMORY_EXECUTOR_KEY, Constants.SPARK.MEMORY_EXECUTOR_SIZE)
    #                     .config(Constants.SPARK.MEMORY_OVERHEAD_KEY, Constants.SPARK.EXECUTOR_MEMORY_OVERHEAD_SIZE)
    #                     .config(Constants.SPARK.DRIVER_MEMORY_KEY, Constants.SPARK.DRIVER_MEMORY_SIZE)

    #                     # --- Recommended for BigQuery ---
    #                     # .config("spark.sql.shuffle.partitions", "200")
    #                     # .config("viewsEnabled", "true")
    #                     # .config("materializationDataset", "spark_temp_ds")

    #                     .getOrCreate()
    #             )

    #             return spark
    #         except ValueError as ve:
    #             raise ve
    #         except Exception as e:
    #             error_message = f"{Constants.ErrorMessage.CREATESPARKSESSIONERROR} :{e}"
    #             raise RuntimeError(error_message) from e             


"""
from pyspark.sql import SparkSession

def get_spark_session(app_name: str,
                      project_id: str,
                      gcs_temp_bucket: str,
                      gcs_connector_jar: str,
                      bigquery_package: str):
    
    spark = (
        SparkSession.builder
            .appName(app_name)
            
            # --- BigQuery + GCS Connectors ---
            .config("spark.jars.packages", bigquery_package)   # BigQuery connector
            .config("spark.jars", gcs_connector_jar)           # GCS connector .jar

            # --- BigQuery Required Configuration ---
            .config("spark.bigquery.project", project_id)
            .config("spark.bigquery.tempGcsBucket", gcs_temp_bucket)

            # --- Performance / JVM Memory Settings ---
            .config("spark.executor.memory", "4g")
            .config("spark.executor.memoryOverhead", "1g")
            .config("spark.driver.memory", "4g")

            # --- Recommended for BigQuery ---
            .config("spark.sql.shuffle.partitions", "200")
            .config("viewsEnabled", "true")
            .config("materializationDataset", "spark_temp_ds")

            .getOrCreate()
    )

    return spark


def get_spark_session(app_name: str,
                      project_id: str,
                      gcs_temp_bucket: str,
                      gcs_connector_jar: str,
                      bigquery_package: str,
                      mysql_jdbc_package: str = None,
                      mysql_jdbc_jar: str = None):
    """
    # Create a SparkSession configured for:
    # - BigQuery
    # - Google Cloud Storage
    # - Cloud SQL MySQL (via JDBC connector)
    """

    # 1️⃣ Build spark.jars.packages list (BigQuery + optionally MySQL JDBC)
    packages = [bigquery_package]
    if mysql_jdbc_package:   # Maven package format e.g. "mysql:mysql-connector-java:8.0.29"
        packages.append(mysql_jdbc_package)

    packages_str = ",".join(packages)

    # 2️⃣ Build spark.jars list (GCS connector + optional MySQL JAR)
    jars = [gcs_connector_jar]
    if mysql_jdbc_jar:       # JAR path e.g. "gs://bucket/mysql-connector-j-8.0.33.jar"
        jars.append(mysql_jdbc_jar)

    jars_str = ",".join(jars)

    # 3️⃣ Create SparkSession
    spark = (
        SparkSession.builder
            .appName(app_name)

            # --- BigQuery & GCS Connectors ---
            .config("spark.jars.packages", packages_str)
            .config("spark.jars", jars_str)

            # --- BigQuery Required Config ---
            .config("spark.bigquery.project", project_id)
            .config("spark.bigquery.tempGcsBucket", gcs_temp_bucket)

            # --- Performance / Memory ---
            .config("spark.executor.memory", "4g")
            .config("spark.executor.memoryOverhead", "1g")
            .config("spark.driver.memory", "4g")

            # --- Recommended for BigQuery ---
            .config("spark.sql.shuffle.partitions", "200")
            .config("viewsEnabled", "true")
            .config("materializationDataset", "spark_temp_ds")

            .getOrCreate()
    )

    return spark

def get_local_spark_session(app_name: str = "LocalSparkApp"):

    spark = (
        SparkSession.builder
            .master("local[*]")          # run locally
            .appName(app_name)
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .config("spark.sql.shuffle.partitions", "20")
            .getOrCreate()
    )

    return spark

    
spark = get_spark_session(
    app_name="MyApp",
    project_id="my-gcp-project",
    gcs_temp_bucket="my-temp-bucket",
    gcs_connector_jar="gs://libs/gcs-connector-hadoop3-latest.jar",
    bigquery_package="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1",

    # Cloud SQL MySQL connector (Maven)
    mysql_jdbc_package="mysql:mysql-connector-java:8.0.33"
)

spark = get_spark_session(
    app_name="MyApp",
    project_id="my-gcp-project",
    gcs_temp_bucket="my-temp-bucket",
    gcs_connector_jar="gs://libs/gcs-connector-hadoop3-latest.jar",
    bigquery_package="com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.42.1",

    # Cloud SQL MySQL connector from GCS/local
    mysql_jdbc_jar="gs://libs/mysql-connector-j-8.0.33.jar"
)

df = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:mysql://<INSTANCE_IP>:3306/mydb") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("dbtable", "my_table") \
    .option("user", "my_user") \
    .option("password", "my_password") \
    .load()

"""