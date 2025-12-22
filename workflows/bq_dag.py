import airflow
from airflow import DAG
from datetime import timedelta,datetime
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
import os
from airflow.models import Variable

ENV = os.getenv("ENV", "DEV")

def get_var(key: str):
    return Variable.get(f"{ENV}_{key}")

# Define constants
SQL_FILE_PATH_1 = "/home/airflow/gcs/data/BQ/bronze.sql"
SQL_FILE_PATH_2 = "/home/airflow/gcs/data/BQ/silver.sql"
SQL_FILE_PATH_3 = "/home/airflow/gcs/data/BQ/gold.sql"

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

def get_start_date():
    start_date_str = get_var("DAG_START_DATE")
    if start_date_str:
        return datetime.strptime(start_date_str, "%Y-%m-%d")
    return days_ago(1)

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
ARGS = {
     "owner": get_var("OWNER"),
    "start_date": get_start_date(),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": get_var("EMAIL").split(","),
    "email_on_success": False,
    "retries": get_var("RETRIES"),
    "retry_delay": timedelta(minutes=get_var("RETRY_DELAY_MINUTES")),
}

# Define the DAG
with DAG(
    dag_id=get_var("BQ_DAG_ID"),
    schedule_interval=None,
    description=get_var("BQ_DAG_DESC"),
    default_args=ARGS,
    catchup=False,
    tags=get_var("BQ_DAG_TAGS").split(",")
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id="bronze_tables",
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id="silver_tables",
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id="gold_tables",
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables
