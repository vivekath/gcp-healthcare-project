CLUSTER_NAME="my-demo-cluster2"
REGION="us-east1"

gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --num-workers=2 \
  --worker-machine-type=n1-standard-2 \
  --worker-boot-disk-size=50 \
  --master-machine-type=n1-standard-2 \
  --master-boot-disk-size=50 \
  --image-version=2.0-debian10 \
  --enable-component-gateway \
  --optional-components=JUPYTER \
  --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
  --metadata bigquery-connector-version=1.2.0,spark-bigquery-connector-version=0.21.0
# ------------------------------------------------------------------------------
# Prerequisites, created quantum-episode-345713:temp_dataset.audit_log => Used in inittail query
# Remove columns name from csv file while uploading
# Mentioned schema explicitly while creation spark dataframe
# 0.0.0.0/0 add netwok in cloud sql
# optimise code
# CREATE TABLE avd-databricks-demo.temp_dataset.audit_log ( => table_name column correction
# project flow, all standard terminology, challenges/issues, configuration, improvement
# project setup on local, google cloud shell without dataproc jupyterlab
# Setting SQL instance => 4. Setting up the Data sources - SQL DBs, GCS, BQ, Configs => 3:07 / 48:20
# Setting Cloud Compose => 6. GCS Landing to Bronze Layer - GCS, BigQuery => 23:49 / 1:03:59
# Create Dags - Workflow Orchestration - Airflow => 19:30 / 28:46
# Setting Cloud Build trigger CICD => 10. CICD - Github, Cloud Build, Airflow => 13:50 / 1:20:26
# Setting Cloud SQL => 4. Setting up the Data sources - SQL DBs, GCS, BQ, Configs => 3:21 / 48:20
# Bigquery, MySql Instance, dataproc Cluster, Bucket storage, cloud build, Data Fusion, cloud composer => open airflow
# Delete all the resources 
  # Dataproc cluster 
  # Composer environment 
  # Cloud build trigger 
  # Cloud sql instances 
  # GCS buckets 
  # BigQuery datasets and Tables 

# Key Techniques Involved 
# Metadata-Driven Approach: (maintain config file for all models/tables, is_active is used to manage table)
# Slowly Changing Dimensions (SCD) Type 2 Implementation: 
# Common Data Model (CDM): 
# Medallion Architecture (Bronze → Silver → Gold Layers): 
# Logging and Monitoring:
# Error Handling: 
# CI/CD Implementation (Cloud Build Trigger): 
# Dataproc cluster 
# Composer environment (Cloud composer bucket used by airflow)
# Cloud sql instances 
# Cloud build trigger 
# GCS buckets (Datelake)
# BigQuery datasets and Tables (Datawarehouse)
# Airflow Orchestration (DAG's)
# Managed job dependencies using airflow
# Star schema => Dimention Table, Fact Tables 
# Python
# SQL
# PySpark
# Cross check validation by matching count
# Delta Load = Only loading changed data since the last run.
# Incremental load is similar to delta load — you load incremental changes, not the entire dataset.
# Full Load
# Loading data from multiple Datasources
# Job scheduler using airflow
# Landing Layer
# Curated Layer
# File formay types (csv parquet,json, avro, ORC)
# Blobs
# CI/CD
# BQ, GCS, Cloud SQl MySQL Connectors 
# Event logger
# Audit table (mostly using for incremental load using last load timestamp)
# Watermark column (to identify last modified date from source)
# Maintain data history using day vise file archive
# PySpark Dataframe schema (StructType, StructField)
# Load Bronze table data using external table instead of managed table (Create table)
# GCP services used => BQ, Cloud SQL, GCS, Dataproc, Cloud Build, Cloud Composer





# Data Fusion (Too heavy, takes more time to load, better pyspark, is more custimizable, to control) => No code ETL solution provided by google cloud, simply drag and drop UI based tool
# Cloud Functions => Event based compute system, serverless, event driven, small code snipets
# Dataflow => Stream processing, batch processing using Apache Beam 
# https://www.youtube.com/watch?v=kxV4_xDchCc&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=2
# (python (extract data, .py file) => cloud storage => cloud data fusion (first pipeline have to create manually on Data Fusion) => Bigquery) => Done by Airflow orchestration => Looker

# https://www.youtube.com/watch?v=UXJxcWgxwu0&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=3
# (python (extract data .py file) => Cloud storage ) => Done by Airflow => Cloud function (Will detect/track cloud storage, for any configured file upload/change will trigger dataflow job, will write code for this) => Dataflow (first will have to create job manually on Dataflow) => Bigquery => Looker 

# https://www.youtube.com/watch?v=_CQCOusfGrs&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=4
# Flask app GET, POST api => GCS => Cloud Function => BQ => Looker
# Flask app to o/p report once csv input 
# When we have file in GCS
# Trigger cloud function, there are multiple ways to transfer to BQ
  # 1. Using Cloud Fusion
  # 2. Using Dataflow
  # 3. Cloud Composer airflow
  # 4. Using Cloud Function only (Have code for both detect GCS file upload and upload to BQ), for simple use case no need of above services, that will make it complex

# https://www.youtube.com/watch?v=tRlJDx5vSFM&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=5
# BQ Data Transfer Service (DTS)
# Analyzing YouTube Channel Performance with BigQuery and Looker

# https://www.youtube.com/watch?v=b593huRgXic&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=6
# File landed in GCS => Cloud Function => Dataflow => BQ => Looker

# https://www.youtube.com/watch?v=L4Ad7RQYv4o&list=PLLrA_pU9-Gz2DaQDcY5g9aYczmipBQ_Ek&index=7
# MySQl => Datastream => BQ => Looker

# How Do ETL Tools Handle Large Datasets? - The Friendly Statistician
# 1. Selective extraction 
# 2. Parallel processing
# 3. Data Partitioning
# 4. Scalable architecture

# spark
# 02 How Spark Works - Driver & Executors & Task | How Spark divide Job in Stages | What is Shuffle in Spark

# Data lake with Pyspark through Dataproc GCP using Airflow => Medium article
# https://ilhamaulanap.medium.com/data-lake-with-pyspark-through-dataproc-gcp-using-airflow-d3d6517f8168
# https://github.com/vivekath/datalake-pyspark-airflow-gcp


# can we do partitionBy for other file formates
# If your workflow includes BigQuery, Dataproc, or analytics:
# ✔ Best:
# Parquet → most efficient + BigQuery friendly
# ORC
# ⚠️ OK but not ideal:
# Avro
# ❌ Not recommended with partitionBy:
# JSON, CSV
# They create too many small files and increase job time.

# Metadata-Driven Approach, maintain config file for all models/tables, is_active is used to manage table
# Slowly Changing Dimensions (SCD) Type 2 Implementation, take approval from client for full, incremental load tables