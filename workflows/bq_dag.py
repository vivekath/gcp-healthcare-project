import airflow
from airflow import DAG
from datetime import timedelta
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from data.configs.constants import Constants
# Define constants
PROJECT_ID = Constants.GCP.BQ_PROJECT_ID
LOCATION = Constants.GCP.LOCATION
SQL_FILE_PATH_1 = Constants.BQ_DAG.BQ_SQL_AIRFLOW_COMPOSER_FILE_PATH.format(job_name=Constants.Jobs.BRONZE) 
SQL_FILE_PATH_2 = Constants.BQ_DAG.BQ_SQL_AIRFLOW_COMPOSER_FILE_PATH.format(job_name=Constants.Jobs.SILVER)
SQL_FILE_PATH_3 = Constants.BQ_DAG.BQ_SQL_AIRFLOW_COMPOSER_FILE_PATH.format(job_name=Constants.Jobs.GOLD)

# Read SQL query from file
def read_sql_file(file_path):
    with open(file_path, "r") as file:
        return file.read()

BRONZE_QUERY = read_sql_file(SQL_FILE_PATH_1)
SILVER_QUERY = read_sql_file(SQL_FILE_PATH_2)
GOLD_QUERY = read_sql_file(SQL_FILE_PATH_3)

# Define default arguments
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

# Define the DAG
with DAG(
    dag_id=Constants.BQ_DAG.DAG_ID,
    schedule_interval=Constants.BQ_DAG.SCHEDULE_INTERVAL,
    description=Constants.BQ_DAG.DESCRIPTION,
    default_args=ARGS,
    tags=Constants.BQ_DAG.TAGS
) as dag:

    # Task to create bronze table
    bronze_tables = BigQueryInsertJobOperator(
        task_id=Constants.BQ_DAG.BRONZE_TABLES,
        configuration={
            "query": {
                "query": BRONZE_QUERY,
                "useLegacySql": Constants.BQ_DAG.USELEGACYSQL,
                "priority": Constants.BQ_DAG.PRIORITY,
            }
        },
    )

    # Task to create silver table
    silver_tables = BigQueryInsertJobOperator(
        task_id=Constants.BQ_DAG.SILVER_TABLES,
        configuration={
            "query": {
                "query": SILVER_QUERY,
                "useLegacySql": Constants.BQ_DAG.USELEGACYSQL,
                "priority": Constants.BQ_DAG.PRIORITY,
            }
        },
    )

    # Task to create gold table
    gold_tables = BigQueryInsertJobOperator(
        task_id=Constants.BQ_DAG.GOLD_TABLES,
        configuration={
            "query": {
                "query": GOLD_QUERY,
                "useLegacySql": Constants.BQ_DAG.USELEGACYSQL,
                "priority": Constants.BQ_DAG.PRIORITY,
            }
        },
    )

# Define dependencies
bronze_tables >> silver_tables >> gold_tables
