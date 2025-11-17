import airflow
from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from data.configs.constants import Constants
# Define default arguments
ARGS = {
    "owner": Constants.DAG_ARGS.OWNER,
    "start_date": Constants.DAG_ARGS.START_DATE_DAYS_AGO,
    "depends_on_past": Constants.DAG_ARGS.DEPENDS_ON_PAST,
    "email_on_failure": Constants.DAG_ARGS.EMAIL_ON_FAILURE,
    "email_on_retry": Constants.DAG_ARGS.EMAIL_ON_RETRY,
    "email": Constants.DAG_ARGS.EMAIL,
    "email_on_success": Constants.DAG_ARGS.EMAIL_ON_SUCCESS,
    "retries": Constants.DAG_ARGS.RETRIES,
    "retry_delay": Constants.DAG_ARGS.RETRY_DELAY
}

# Define the parent DAG
with DAG(
    dag_id=Constants.Parent_DAG.DAG_ID,
    schedule_interval=Constants.Parent_DAG.SCHEDULE_INTERVAL,
    description=Constants.Parent_DAG.DESCRIPTION,
    default_args=ARGS,
    tags=Constants.Parent_DAG.TAGS
) as dag:

    # Task to trigger PySpark DAG
    trigger_pyspark_dag = TriggerDagRunOperator(
        task_id=Constants.Parent_DAG.TRIGGER_PYSPARK_DAG,
        trigger_dag_id=Constants.Pyspark_DAG.DAG_ID,
        wait_for_completion=True,
    )

    # Task to trigger BigQuery DAG
    trigger_bigquery_dag = TriggerDagRunOperator(
        task_id=Constants.Parent_DAG.TRIGGER_BIGQUERY_DAG,
        trigger_dag_id=Constants.BQ_DAG.DAG_ID,
        wait_for_completion=True,
    )

# Define dependencies
trigger_pyspark_dag >> trigger_bigquery_dag