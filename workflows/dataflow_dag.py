from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

# =============================================================================
# ENV & helpers
# =============================================================================
ENV = Variable.get("ENV")

def get_var(key):
    return Variable.get(f"{ENV}_{key}")

def get_start_date():
    start_date_str = get_var("DAG_START_DATE")
    if start_date_str:
        return datetime.strptime(start_date_str, "%Y-%m-%d")
    return days_ago(1)

# =============================================================================
# Airflow Variables
# =============================================================================
PROJECT_ID = get_var("PROJECT_ID")
REGION = get_var("REGION")
COMPOSER_BUCKET = get_var("COMPOSER_BUCKET")
GCS_BUCKET = get_var("DATAFLOW_GCS_BUCKET")

# =============================================================================
# DAG default args
# =============================================================================
ARGS = {
    "owner": get_var("OWNER"),
    "start_date": get_start_date(),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email_on_success": False,
    "email": get_var("EMAIL").split(","),
    "retries": int(get_var("RETRIES")),
    "retry_delay": timedelta(minutes=int(get_var("RETRY_DELAY_MINUTES"))),
}

# =============================================================================
# DAG definition
# =============================================================================
with DAG(
    dag_id="dataflow_dag",
    default_args=ARGS,
    schedule_interval=get_var("SCHEDULE_INTERVAL"),   # or cron
    description="This is a dataflow dag",
    catchup=False,
    tags=["dataflow", "beam"],
) as dag:

    # -------------------------------------------------------------------------
    # Transactions pipeline
    # -------------------------------------------------------------------------
    transactions_pipeline = BeamRunPythonPipelineOperator(
        task_id="transactions_dataflow_job",
        py_file=f"gs://{COMPOSER_BUCKET}/data/INGESTION/transactions_pipeline.py",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "runner": "DataflowRunner",
            "job_name": "transactions",
            "temp_location": f"gs://{GCS_BUCKET}/temp/",
            "staging_location": f"gs://{GCS_BUCKET}/staging/",
        },
    )

    # -------------------------------------------------------------------------
    # Retail sales pipeline
    # -------------------------------------------------------------------------
    retail_sales_pipeline = BeamRunPythonPipelineOperator(
        task_id="retail_sales_dataflow_job",
        py_file=f"gs://{COMPOSER_BUCKET}/data/INGESTION/retail_sales_pipeline.py",
        pipeline_options={
            "project": PROJECT_ID,
            "region": REGION,
            "runner": "DataflowRunner",
            "job_name": "transactions",
            "temp_location": f"gs://{GCS_BUCKET}/temp/",
            "staging_location": f"gs://{GCS_BUCKET}/staging/",
        },
    )

    # -------------------------------------------------------------------------
    # Task dependencies
    # -------------------------------------------------------------------------
    transactions_pipeline >> retail_sales_pipeline
