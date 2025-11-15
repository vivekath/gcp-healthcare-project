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
# 
# Setting Cloud Build trigger CICD => 10. CICD - Github, Cloud Build, Airflow => 13:50 / 1:20:26
