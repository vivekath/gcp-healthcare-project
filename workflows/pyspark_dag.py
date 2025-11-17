# import all modules
import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocStartClusterOperator,
    DataprocStopClusterOperator,
    DataprocSubmitJobOperator,
)
from data.configs.constants import Constants

# define the variables
PROJECT_ID = Constants.GCP.BQ_PROJECT_ID
REGION = Constants.GCP.REGION
CLUSTER_NAME = Constants.GCP.CLUSTER_NAME
COMPOSER_BUCKET = Constants.GCP.COMPOSER_BUCKET

GCS_JOB_FILE_1 = Constants.Pyspark_DAG.PY_SPARK_AIRFLOW_COMPOSER_BUCKET_FILE_PATH.format(composer_bucket=COMPOSER_BUCKET,job_name=Constants.Jobs.HOSPITALA_MYSQLTOLANDING)
PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_1},
}

GCS_JOB_FILE_2 = Constants.Pyspark_DAG.PY_SPARK_AIRFLOW_COMPOSER_BUCKET_FILE_PATH.format(composer_bucket=COMPOSER_BUCKET,job_name=Constants.Jobs.HOSPITALB_MYSQLTOLANDING)
PYSPARK_JOB_2 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_2},
}

GCS_JOB_FILE_3 = Constants.Pyspark_DAG.PY_SPARK_AIRFLOW_COMPOSER_BUCKET_FILE_PATH.format(composer_bucket=COMPOSER_BUCKET,job_name=Constants.Jobs.CLAIMS)
PYSPARK_JOB_3 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_3},
}

GCS_JOB_FILE_4 = Constants.Pyspark_DAG.PY_SPARK_AIRFLOW_COMPOSER_BUCKET_FILE_PATH.format(composer_bucket=COMPOSER_BUCKET,job_name=Constants.Jobs.CPT_CODES)
PYSPARK_JOB_4 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {"main_python_file_uri": GCS_JOB_FILE_4},
}

ARGS = {
    "owner": Constants.DAG_ARGS.OWNER,
    "start_date": Constants.DAG_ARGS.START_DATE,
    "depends_on_past": Constants.DAG_ARGS.DEPENDS_ON_PAST,
    "email_on_failure": Constants.DAG_ARGS.EMAIL_ON_FAILURE,
    "email_on_retry": Constants.DAG_ARGS.EMAIL_ON_RETRY,
    "email": Constants.DAG_ARGS.EMAIL,
    "email_on_success": Constants.DAG_ARGS.EMAIL_ON_SUCCESS,
    "retries": Constants.DAG_ARGS.RETRIES,
    "retry_delay": Constants.DAG_ARGS.RETRY_DELAY
}

# define the dag
with DAG(
    dag_id=Constants.Pyspark_DAG.DAG_ID,
    schedule_interval=Constants.Pyspark_DAG.SCHEDULE_INTERVAL,
    description=Constants.Pyspark_DAG.DESCRIPTION,
    default_args=ARGS,
    tags=Constants.Pyspark_DAG.TAGS
) as dag:
    
    # define the Tasks
    start_cluster = DataprocStartClusterOperator(
        task_id=Constants.Pyspark_DAG.START_CLUSTER,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_task_1 = DataprocSubmitJobOperator(
        task_id=Constants.Pyspark_DAG.PYSPARK_TASK_1, 
        job=PYSPARK_JOB_1, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_2 = DataprocSubmitJobOperator(
        task_id=Constants.Pyspark_DAG.PYSPARK_TASK_2, 
        job=PYSPARK_JOB_2, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_3 = DataprocSubmitJobOperator(
        task_id=Constants.Pyspark_DAG.PYSPARK_TASK_3, 
        job=PYSPARK_JOB_3, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    pyspark_task_4 = DataprocSubmitJobOperator(
        task_id=Constants.Pyspark_DAG.PYSPARK_TASK_4, 
        job=PYSPARK_JOB_4, 
        region=REGION, 
        project_id=PROJECT_ID
    )

    stop_cluster = DataprocStopClusterOperator(
        task_id=Constants.Pyspark_DAG.STOP_CLUSTER,
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

# define the task dependencies
start_cluster >> pyspark_task_1 >> pyspark_task_2 >> pyspark_task_3 >> pyspark_task_4 >> stop_cluster