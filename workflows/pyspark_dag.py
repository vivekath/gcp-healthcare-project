from airflow import DAG
import os
import os
from airflow.models import Variable
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago

ENV = os.getenv("ENV", "DEV")

def get_var(key: str):
    return Variable.get(f"{ENV}_{key}")

def get_start_date():
    start_date_str = get_var("DAG_START_DATE")
    if start_date_str:
        return datetime.strptime(start_date_str, "%Y-%m-%d")
    return days_ago(1)

from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocStopClusterOperator,
    DataprocDeleteClusterOperator,
    ClusterGenerator,
    DataprocStartClusterOperator
)

# -----------------------
# Airflow Variables
# -----------------------
PROJECT_ID = get_var("PROJECT_ID")
REGION = get_var("REGION")
COMPOSER_BUCKET = get_var("COMPOSER_BUCKET")
CLUSTER_NAME = get_var("CLUSTER_NAME")
BQ_JAR = get_var("BQ_JAR")
GCS_BUCKET = get_var("GCS_BUCKET")
HOSPITAL_NAME_A = get_var("HOSPITAL_NAME_A")
HOSPITAL_NAME_B = get_var("HOSPITAL_NAME_B")
HOSPITAL_DB_A = get_var("HOSPITAL_DB_A")
HOSPITAL_DB_B = get_var("HOSPITAL_DB_B")
HOSPITAL_A_MYSQL_HOST = get_var("HOSPITAL_A_MYSQL_HOST")
HOSPITAL_A_MYSQL_PORT = get_var("HOSPITAL_A_MYSQL_PORT")
HOSPITAL_B_MYSQL_HOST = get_var("HOSPITAL_B_MYSQL_HOST")
HOSPITAL_B_MYSQL_PORT = get_var("HOSPITAL_B_MYSQL_PORT")
MASTER_MACHINE_TYPE = get_var("MASTER_MACHINE_TYPE")
WORKER_MACHINE_TYPE = get_var("WORKER_MACHINE_TYPE")
NUM_WORKERS = int(get_var("NUM_WORKERS"))
MASTER_DISK_SIZE = int(get_var("MASTER_DISK_SIZE"))
WORKER_DISK_SIZE = int(get_var("WORKER_DISK_SIZE"))
IMAGE_VERSION = get_var("IMAGE_VERSION")
INITIALIZATION_ACTIONS = get_var("INITIALIZATION_ACTIONS")
SPARK_BIGQUERY_CONNECTOR_VERSION = get_var("SPARK_BIGQUERY_CONNECTOR_VERSION")
# -----------------------
# PySpark job function
# -----------------------
def pyspark_job(file_path, job_args=None):
    job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": file_path,
            "jar_file_uris": [BQ_JAR],
            "python_file_uris": [f"gs://{COMPOSER_BUCKET}/data/common/common_lib.zip"]
        },
    }
    if job_args:
        job["pyspark_job"]["args"] = job_args
    return job

# -----------------------
# Cluster configuration
# -----------------------
CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    master_machine_type=MASTER_MACHINE_TYPE,
    worker_machine_type=WORKER_MACHINE_TYPE,
    num_workers=NUM_WORKERS,
    master_disk_size=MASTER_DISK_SIZE,
    worker_disk_size=WORKER_DISK_SIZE,
    image_version=IMAGE_VERSION,
    optional_components=["JUPYTER"],
    enable_component_gateway=True,
    initialization_actions=[
        INITIALIZATION_ACTIONS
    ],
    metadata={
        "spark-bigquery-connector-version": SPARK_BIGQUERY_CONNECTOR_VERSION
    }
).make()

# -----------------------
# DAG default args
# -----------------------
ARGS = {
    "owner": get_var("OWNER"),
    "start_date": get_start_date(),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": get_var("EMAIL").split(","),
    "email_on_success": False,
    "retries": int(get_var("RETRIES")),
    "retry_delay": timedelta(minutes=int(get_var("RETRY_DELAY_MINUTES"))),
}

# -----------------------
# DAG Definition
# -----------------------
with DAG(
    dag_id=get_var("PYSPARK_DAG_ID"),
    default_args=ARGS,
    schedule_interval=None,
    description=get_var("PYSPARK_DAG_DESC"),
    catchup=False,
    tags=get_var("PYSPARK_DAG_TAGS").split(",")
) as dag:

    # Create cluster
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    #     # define the Tasks
    # start_cluster = DataprocStartClusterOperator(
    #     task_id="start_cluster",
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     cluster_name=CLUSTER_NAME,
    # )

    # Hospital A ingestion
    task_1 = DataprocSubmitJobOperator(
        task_id="hospitalA_ingestion",
        job=pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py",
            job_args=[f"--gcs_bucket={GCS_BUCKET}", f"--project_id={PROJECT_ID}", f"--hospital_name={HOSPITAL_NAME_A}", f"--hospital_db={HOSPITAL_DB_A}", f"--mysql_host={HOSPITAL_A_MYSQL_HOST}", f"--mysql_port={HOSPITAL_A_MYSQL_PORT}"]
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # Hospital B ingestion
    task_2 = DataprocSubmitJobOperator(
        task_id="hospitalB_ingestion",
        job=pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py",
            job_args=[f"--gcs_bucket={GCS_BUCKET}", f"--project_id={PROJECT_ID}", f"--hospital_name={HOSPITAL_NAME_B}", f"--hospital_db={HOSPITAL_DB_B}", f"--mysql_host={HOSPITAL_B_MYSQL_HOST}", f"--mysql_port={HOSPITAL_B_MYSQL_PORT}"]
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # # Claims ingestion
    task_3 = DataprocSubmitJobOperator(
        task_id="claims_ingestion",
        job=pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py",
            job_args=[f"--gcs_bucket={GCS_BUCKET}", f"--project_id={PROJECT_ID}"]
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # # CPT Codes ingestion
    task_4 = DataprocSubmitJobOperator(
        task_id="cpt_codes_ingestion",
        job=pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py",
            job_args=[f"--gcs_bucket={GCS_BUCKET}", f"--project_id={PROJECT_ID}"]
        ),
        region=REGION,
        project_id=PROJECT_ID,
    )

    # # Stop cluster
    # stop_cluster = DataprocStopClusterOperator(
    #     task_id="stop_cluster",
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     cluster_name=CLUSTER_NAME,
    # )

    # # Delete cluster
    # delete_cluster = DataprocDeleteClusterOperator(
    #     task_id="delete_cluster",
    #     project_id=PROJECT_ID,
    #     region=REGION,
    #     cluster_name=CLUSTER_NAME,
    # )

    # -----------------------
    # Task Dependencies
    # -----------------------
    # create_cluster >> task_1 >> task_2 >> task_3 >> task_4 >> stop_cluster >> delete_cluster
    create_cluster >> task_1 >> task_2 >> task_3 >> task_4



# ✅ Recommended Cluster Configuration for 100M rows/day
"""
CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,

    # Bigger machines
    master_machine_type="n1-standard-4",
    worker_machine_type="n1-standard-4",

    # More workers for parallelism
    num_workers=3,

    # Add preemptible for cheap compute
    num_preemptible_workers=2,
    preemptible_worker_machine_type="n1-standard-4",

    # Disks
    master_disk_size=100,
    worker_disk_size=100,

    # Image version
    image_version="2.1-debian11",

    optional_components=["JUPYTER"],
    enable_component_gateway=True,

    initialization_actions=[
        "gs://goog-dataproc-initialization-actions-us-east1/connectors/connectors.sh"
    ],

    metadata={
        "bigquery-connector-version": "1.2.0",
        "spark-bigquery-connector-version": "0.34.0",  # latest stable
    },

).make()
"""