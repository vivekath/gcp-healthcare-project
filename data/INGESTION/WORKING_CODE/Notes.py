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
# -------------------------------------------------------------------------------
CLUSTER_NAME="my-demo-cluster2"
REGION="us-east1"

gcloud dataproc clusters create ${CLUSTER_NAME} \
  --region=${REGION} \
  --num-workers=0 \
  --master-machine-type=n1-standard-2 \
  --master-boot-disk-size=50 \
  --image-version=2.0-debian10 \
  --enable-component-gateway \
  --optional-components=JUPYTER \
  --initialization-actions=gs://goog-dataproc-initialization-actions-${REGION}/connectors/connectors.sh \
  --metadata bigquery-connector-version=1.2.0,spark-bigquery-connector-version=0.21.0
# ------------------------------------------------------------------------------
# Cheapest Dataproc setup (VERY IMPORTANT)
# Single-node cluster (learning/dev)
gcloud dataproc clusters create dev-single-node \
  --region=us-central1 \
  --num-workers=0 \
  --master-machine-type=e2-standard-2 \
  --master-boot-disk-size=30 \
  --image-version=2.0-debian10
# --------------------------------------------------------------------------------------------
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
# Common Data Model (CDM) (record added using surrogate key as primary key): 
# Medallion Architecture (Bronze layer (raw_data) → Silver (cleaned data) → Gold Layers (aggregated data business KPIs data)): 
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
# Star schema => Dimention Table (Upstream table), Fact Tables (Downstream table) 
# Python
# SQL
# PySpark
# Cross check validation by matching count
# Delta Load = Only loading changed data since the last run.
# Incremental load is similar to delta load — you load incremental changes, not the entire dataset.
# Full Load
# Loading data from multiple Datasources (Cloud SQL MySQL, GCS json files, Local CSV files, Request API's)
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
# Surrogate key (Unique key generated internally, not from source system, generally used in SCD Type 2)
# ------------------------------------------------------------------------------------------------------------
# Fully prepare this health project, this will help, prepare SQL, Pyhton, Real time scenarios questions and answers document
# --------------------------------------------------------------------------------------------------
# Explain project in short
# Requiremnt was to migrate from dbt to BQ, to create pipeline which run on daily basis to populate data on OLAP BQ datawarehouse
# 1. Data Ingestion 
# 2. Data storage
# 3. Data Processing 
# 4. Data Analysis
# 5. Data pipeline 
# 6. Data orchestration
# 7. Data Visualization
# 8. services used are: BQ, Cloud SQL, GCS, Dataproc, Cloud Build, Cloud Composer


# Batch Data injestion to landing layer from multiple sources (Cloud SQL MySQL, GCS json files), it's a query based fetch to get fresh data
# Before injestion move existing files to archive with date folder structure
# Extract data using pyspark from Cloud SQL MySQL, incremental load using watermark column and last load timestamp from audit table in BQ
# Save data to GCS landing in parquet format partitioned by load date
# Load data to BQ from GCS using external table for bronze layer
# Maintain event log for each step, save logs to GCS and BQ log table
# Orchestrate entire workflow using Airflow DAG's with proper dependencies
# Implement CI/CD using Cloud Build trigger on github repo commits
# Documentation of entire project with architecture diagram, steps to recreate the setup, code explanation, challenges faced and future improvements
# services used are: BQ, Cloud SQL, GCS, Dataproc, Cloud Build, Cloud Composer
# ------------------------------------------------------------------------------------------------------------
# How Do ETL Tools Handle Large Datasets? - The Friendly Statistician
# 1. Selective extraction 
# 2. Parallel processing
# 3. Data Partitioning
# 4. Scalable architecture

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
# --------------------------------------------------------------------------------------------
# vertical => union/unionall
# horizontal => joins (most of time inner join, left join used)
# --------------------------------------------------------------------------------------------
# Distributed Computing
# Parallel Computing
# Cloud Computing
# -------------------------------------------------------------------------------------------
# Data Fusion => low priority
# companies=> for batch job they preffer dataproc, for streaming job dataflow preferred
# If you are using spark you should go for only dataproc
# If you are using apache beam you should go for only dataflow
# spark faster than apache beam
# A batch job processes large volumes of data at once, usually based on a fixed schedule (hourly, daily, weekly).
# A streaming job processes continuous data as soon as it arrives (near real-time or real-time).
# -------------------------------------------------------------------------------------------
🧠 Split your work into 3 layers
1️⃣ Logic development (FREE / very cheap)
Where:
Google Colab
Local PySpark
Small sample data (local files)
What you do:
Transformations
Joins
Filters
Aggregations
Window functions
spark.read.csv("/content/sample.csv")
✔ No Dataproc
✔ Zero or near-zero cost

2️⃣ Integration testing (SHORT Dataproc usage)
Where:
Single-node Dataproc
What you test:
gs:// reads/writes
BigQuery connectors
Real schema
Performance sanity
⏱ Run for 10–30 minutes only
💰 Cost: ₹10–₹30 per run (approx)

3️⃣ Production runs
Where:
Scheduled Dataproc jobs
Composer / Airflow
# --------------------------Check initial tutorial how to use pyspark locally---------------------------------
Use Colab / local Spark for daily learning  
Spin up single-node Dataproc only when needed
Delete cluster immediately after testing
# -----------------------------------------------------------
# firts take 20% of data to local system
# do all development and testing on local system
# once everything is done move to cloud dataproc
# create dataproc cluster
# -----------------------------------------------------------
# Identify data sources 
# Understand data volume and velocity
# Choose between batch and stream processing
# Design data ingestion pipelines
# Set up data storage solutions
# Implement data processing workflows
# Establish data governance and security measures
# Monitor and optimize performance
# Plan for scalability and future growth
# Ensure cost-effectiveness
# Document the architecture and processes
# Train the team on the new system
# Continuously review and improve the data engineering practices
# Stay updated with the latest technologies and trends in data engineering
# Collaborate with stakeholders to align data strategies with business goals
# -----------------------------------------------------------
# Fisrt setup everything on local system using pyspark
# once everything is done move to cloud dataproc  
# create dataproc cluster
# -----------------------------------------------------------
# setup pyspark on local system
# setup GCP SDK on local system
# authenticate GCP account using gcloud auth login
# setup service account and download json key file
# -------------------------------------------111111--------------------------------
# First test everyhtong on local system using pyspark with limited data
# once everything is done move to cloud dataproc
# Now increase volume of data to test performance on local as well on cloud dataproc (check required configurations for data volume)
# check for improvements in code to optimize performance
# Again increase volume of data to test performance on local as well on cloud dataproc (check required configurations for data volume)
# once everything is done schedule job using airflow on cloud composer
# In short finalize everything on local system using pyspark  
# once everything is done move to cloud dataproc
# In short first set the required logic with small data (20%,30%) on local system using pyspark then, then scale up and do configurations on cloud dataproc with full data   
# -------------------------------------------
# I am using GCS, Bigquery, dataproc cluster, composer. cloud build
# How to set set location of all

"""
Keep everything in the SAME REGION (or compatible multi-region)

Below is a clear, practical guide for setting location service-by-service, plus a recommended architecture for your setup.

🔑 Golden Rule (Very Important)
Service	Location Rule
BigQuery	Dataset location must match job location
Dataproc	Cluster region ≈ GCS bucket region
Composer	Environment region ≈ Dataproc region
Cloud Build	Global (no issue)
GCS	Bucket location must match compute
"""

"""
✅ Recommended Setup for Your Use Case (India)

Since you already hit asia-south1 vs US issues, this is the best practice:

✅ Single Region Architecture
Region: asia-south1 (Mumbai)

Service	Location
GCS Buckets	asia-south1
BigQuery Datasets	asia-south1
Dataproc Cluster	asia-south1
Composer Environment	asia-south1
Cloud Build	Global (OK)
"""
# ---------------------------------------------------------------------------
# Bronze, silver, gold where to to partaion and clustring ?
🥉🥈🥇 Where to use Partitioning & Clustering
TL;DR (Golden Rule)
Layer	Storage	Partition	Cluster
Bronze	GCS / External table	❌ NO	❌ NO
Silver	BigQuery native table	✅ YES	⚠️ Optional
Gold	BigQuery native table	✅ YES	✅ YES
# ----------------------------------------------------------------------------
📌 Final Architecture
Dataproc → GCS (Parquet)
GCS → External Table (Bronze)
Bronze → Silver (Partitioned)
Silver → Gold (Partitioned + Clustered)
# ----------------------------------------------------------------------------
# How dataproc_serverless_pyspark_etl differs from below
1️⃣ High-level difference (one-line answer)
Aspect	Dataproc Serverless PySpark	Your Dataproc Cluster DAG
Infrastructure	Fully managed (no cluster)	You manage cluster lifecycle
Cluster creation	❌ Not required	✅ Required
Cost model	Pay per job (seconds)	Pay per VM uptime
Startup time	Faster	Slower (cluster spin-up)
Ops effort	Very low	High
Control	Limited	Full control