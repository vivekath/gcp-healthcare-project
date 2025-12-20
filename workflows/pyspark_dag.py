from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocStopClusterOperator,
    DataprocDeleteClusterOperator,
    ClusterGenerator
)

PROJECT_ID = "quantum-episode-345713"
REGION = "us-east1"
CLUSTER_NAME = "my-demo-cluster2"
COMPOSER_BUCKET = "us-central1-demo-instance-708d54bc-bucket"

BQ_JAR = "gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar"

def pyspark_job(file_path):
    return {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {
            "main_python_file_uri": file_path,
            "jar_file_uris": [BQ_JAR],
        },
    }

CLUSTER_CONFIG = ClusterGenerator(
    project_id=PROJECT_ID,
    region=REGION,
    cluster_name=CLUSTER_NAME,
    master_machine_type="n1-standard-2",
    worker_machine_type="n1-standard-2",
    num_workers=2,
    master_disk_size=50,
    worker_disk_size=50,
    image_version="2.0-debian10",
    optional_components=["JUPYTER"],
    enable_component_gateway=True,
    initialization_actions=[
        "gs://goog-dataproc-initialization-actions/connectors/connectors.sh"
    ],
    metadata={
        "spark-bigquery-connector-version": "0.36.1"
    }
).make()

ARGS = {
    "owner": "vivek_athilkar",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="pyspark_dag",
    default_args=ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["pyspark", "dataproc", "etl"]
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    task_1 = DataprocSubmitJobOperator(
        task_id="hospitalA_ingestion",
        job=pyspark_job(f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_2 = DataprocSubmitJobOperator(
        task_id="hospitalB_ingestion",
        job=pyspark_job(f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_3 = DataprocSubmitJobOperator(
        task_id="claims_ingestion",
        job=pyspark_job(f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    task_4 = DataprocSubmitJobOperator(
        task_id="cpt_codes_ingestion",
        job=pyspark_job(f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"),
        region=REGION,
        project_id=PROJECT_ID,
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id="stop_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    create_cluster >> task_1 >> task_2 >> task_3 >> task_4 >> stop_cluster >> delete_cluster

# define the task dependencies
# create_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster >> delete_cluster
# pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4
# create_cluster >> start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster >> delete_cluster

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

    # Enable autoscaling (very important for 100M+)
    autoscaling_config="projects/{}/regions/{}/autoscalingPolicies/{}".format(
        PROJECT_ID,
        REGION,
        "basic-spark-autoscale"
    )

).make()
"""

"""
2️⃣ Pass environment values using Dataproc job properties

Instead of hardcoding:

GCS_BUCKET = "heathcare-bucket-12112025"
BQ_PROJECT = "quantum-episode-345713"

🔹 Use environment variables
Modify your PySpark job submission
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "properties": {
            "spark.executorEnv.GCS_BUCKET": "heathcare-bucket-12112025",
            "spark.executorEnv.BQ_PROJECT": "quantum-episode-345713",
            "spark.executorEnv.HOSPITAL_NAME": "hospital-a"
        }
    }
}

3️⃣ Read env variables inside your PySpark code

Update hospitalA_mysqlToLanding.py:

import os

GCS_BUCKET = os.getenv("GCS_BUCKET")
BQ_PROJECT = os.getenv("BQ_PROJECT")
HOSPITAL_NAME = os.getenv("HOSPITAL_NAME")


Then reuse everywhere:

LANDING_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/"
ARCHIVE_PATH = f"gs://{GCS_BUCKET}/landing/{HOSPITAL_NAME}/archive/"


✔ No hardcoding
✔ Different environments (dev/qa/prod) supported
✔ Same job runs everywhere

4️⃣ Even better: Use Composer Variables (Recommended)

Since you are using Cloud Composer, this is cleaner.

In Composer UI:
Admin → Variables


Add:

GCS_BUCKET = heathcare-bucket-12112025
BQ_PROJECT = quantum-episode-345713
HOSPITAL_NAME = hospital-a

Pass them into Dataproc job dynamically
from airflow.models import Variable

PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "properties": {
            "spark.executorEnv.GCS_BUCKET": Variable.get("GCS_BUCKET"),
            "spark.executorEnv.BQ_PROJECT": Variable.get("BQ_PROJECT"),
            "spark.executorEnv.HOSPITAL_NAME": Variable.get("HOSPITAL_NAME")
        }
    }
}


👉 This is how production pipelines are done
"""

"""
✅ How to read these variables in your DAG / PySpark code
In Airflow DAG
from airflow.models import Variable

GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_PROJECT = Variable.get("BQ_PROJECT")
HOSPITAL_NAME = Variable.get("HOSPITAL_NAME")

In PySpark job (Dataproc Serverless / Cluster)

You have two correct options:

Option 1️⃣ Pass as job arguments (BEST PRACTICE)

In DAG:

job=build_pyspark_job(
    f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py",
    args=[
        f"--gcs_bucket={Variable.get('GCS_BUCKET')}",
        f"--bq_project={Variable.get('BQ_PROJECT')}",
        f"--hospital_name={Variable.get('HOSPITAL_NAME')}"
    ]
)


In PySpark:

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--gcs_bucket")
parser.add_argument("--bq_project")
parser.add_argument("--hospital_name")
args = parser.parse_args()

GCS_BUCKET = args.gcs_bucket
BQ_PROJECT = args.bq_project
HOSPITAL_NAME = args.hospital_name
"""