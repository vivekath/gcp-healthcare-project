# DEV ETL time => 28 mins
# PROD ETL time => 24 mins
import airflow
from airflow import DAG
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.models import Variable
import os
# ENV = os.getenv("ENV", "DEV")
ENV = Variable.get("ENV")

def get_var(key: str):
    return Variable.get(f"{ENV}_{key}")

def get_start_date():
    start_date_str = get_var("DAG_START_DATE")
    if start_date_str:
        return datetime.strptime(start_date_str, "%Y-%m-%d")
    return days_ago(1)

# Define default arguments
PARENT_ARGS = {
    "owner": get_var("OWNER"),
    "start_date": get_start_date(),
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "email": get_var("EMAIL").split(","),
    "email_on_success": False,
    "retries": int(get_var("RETRIES")),
    "retry_delay": timedelta(minutes=int(get_var("RETRY_DELAY_MINUTES"))),
}

# Define the parent DAG
with DAG(
    dag_id=get_var("PARENT_DAG_ID"),
    schedule_interval=get_var("SCHEDULE_INTERVAL"),
    description=get_var("PARENT_DAG_DESC"),
    default_args=PARENT_ARGS,
    catchup=False,
    tags=get_var("PARENT_DAG_TAGS").split(",")
) as dag:
    
    # Task to trigger Dataflow DAG
    trigger_dataflow_dag = TriggerDagRunOperator(
        task_id="trigger_dataflow_dag",
        trigger_dag_id="dataflow_dag",
        wait_for_completion=True,
    )

    # Task to trigger PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id="trigger_pyspark_dag",
        trigger_dag_id=get_var("PYSPARK_DAG_ID"),
        wait_for_completion=True,
    )

    # Task to trigger BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id="trigger_bigquery_dag",
        trigger_dag_id=get_var("BQ_DAG_ID"),
        wait_for_completion=True,
    )

    # Define dependencies
    trigger_dataflow_dag >> trigger_pyspark_dag >> trigger_bigquery_dag