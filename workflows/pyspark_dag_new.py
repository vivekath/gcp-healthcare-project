"""
BEST-PRACTICE AIRFLOW DAG
Dataproc Serverless PySpark ETL
- No cluster lifecycle
- No hard-coded values
- Config-driven
- Cost-optimized
"""

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.dataproc import DataprocSubmitJobOperator
from airflow.models import Variable
from datetime import timedelta

# -------------------------------------------------------------------
# Read configuration from Airflow Variables (Admin → Variables)
# -------------------------------------------------------------------
PROJECT_ID = Variable.get("PROJECT_ID")
REGION = Variable.get("REGION")
COMPOSER_BUCKET = Variable.get("COMPOSER_BUCKET")

SPARK_RUNTIME_VERSION = Variable.get("SPARK_RUNTIME_VERSION", default_var="2.1")
EXECUTOR_INSTANCES = Variable.get("SPARK_EXECUTOR_INSTANCES", default_var="2")
EXECUTOR_MEMORY = Variable.get("SPARK_EXECUTOR_MEMORY", default_var="4g")
DRIVER_MEMORY = Variable.get("SPARK_DRIVER_MEMORY", default_var="2g")

# -------------------------------------------------------------------
# Common Dataproc Serverless PySpark configuration
# -------------------------------------------------------------------
COMMON_JOB_CONFIG = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"batch": {}},
    "pyspark_job": {
        "jar_file_uris": [
            "gs://spark-lib/bigquery/spark-bigquery-latest.jar"
        ]
    },
    "runtime_config": {
        "version": SPARK_RUNTIME_VERSION,
        "properties": {
            "spark.executor.instances": EXECUTOR_INSTANCES,
            "spark.executor.memory": EXECUTOR_MEMORY,
            "spark.driver.memory": DRIVER_MEMORY
        }
    }
}


def build_pyspark_job(job_file: str) -> dict:
    """Reusable job factory"""
    job = COMMON_JOB_CONFIG.copy()
    job["pyspark_job"] = job["pyspark_job"].copy()
    job["pyspark_job"]["main_python_file_uri"] = job_file
    return job

# -------------------------------------------------------------------
# DAG default arguments
# -------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "data-platform",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

# -------------------------------------------------------------------
# DAG definition
# -------------------------------------------------------------------
with DAG(
    dag_id="dataproc_serverless_pyspark_etl",
    default_args=DEFAULT_ARGS,
    schedule_interval=None,
    catchup=False,
    tags=["dataproc-serverless", "pyspark", "etl"]
) as dag:

    hospitalA = DataprocSubmitJobOperator(
        task_id="hospitalA_mysql_to_landing",
        project_id=PROJECT_ID,
        region=REGION,
        job=build_pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"
        )
    )

    hospitalB = DataprocSubmitJobOperator(
        task_id="hospitalB_mysql_to_landing",
        project_id=PROJECT_ID,
        region=REGION,
        job=build_pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/hospitalB_mysqlToLanding.py"
        )
    )

    claims = DataprocSubmitJobOperator(
        task_id="claims_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=build_pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/claims.py"
        )
    )

    cpt_codes = DataprocSubmitJobOperator(
        task_id="cpt_codes_job",
        project_id=PROJECT_ID,
        region=REGION,
        job=build_pyspark_job(
            f"gs://{COMPOSER_BUCKET}/data/INGESTION/cpt_codes.py"
        )
    )

    # Task dependencies
    hospitalA >> hospitalB >> claims >> cpt_codes

"""
🔴 What was wrong in the original DAG
Issue	Why it’s bad
Cluster create/start/stop	Expensive, slow, unnecessary
Hard-coded project, region, bucket	Breaks dev/qa/prod portability
Cluster sizing in DAG	Tight coupling + cost risk
Multiple duplicated job configs	Hard to maintain
Manual connector setup	Serverless already handles this
Idle cluster cost	You pay even when nothing runs
"""