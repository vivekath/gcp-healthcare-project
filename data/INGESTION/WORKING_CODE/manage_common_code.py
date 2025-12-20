✅ BEST PRACTICE (RECOMMENDED)
Common Python module in GCS + py-files
1️⃣ Create a common file in GCS
gs://<COMPOSER_BUCKET>/data/common/common_utils.py

# common_utils.py
from pyspark.sql import SparkSession

def get_spark(app_name: str) -> SparkSession:
    return SparkSession.builder.appName(app_name).getOrCreate()

def log_event(msg: str):
    print(msg)

2️⃣ Use py-files in DataprocSubmitJobOperator
COMMON_PY_FILES = [
    f"gs://{COMPOSER_BUCKET}/data/common/common_utils.py"
]

PYSPARK_JOB_1 = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE_1,
        "python_file_uris": COMMON_PY_FILES
    }
}

3️⃣ Import normally in job file
# hospitalA_mysqlToLanding.py
from common_utils import get_spark, log_event

spark = get_spark("hospitalA")
log_event("Job started")


✅ No package install
✅ No cluster rebuild
✅ Works on Dataproc & Dataproc Serverless


"""
✅ What should go into utils.py (common code)

Put only reusable, stateless logic:

✅ Good candidates

Spark session creation

GCS read/write helpers

BigQuery read/write helpers

Logging helpers

Config / env variable readers

Audit helpers

Common transformations

Date / watermark helpers

❌ Avoid in utils

Job-specific business logic

Hard-coded paths

DAG logic

if __name__ == "__main__" blocks

✅ Recommended structure (GCS-based, no package install)
gs://<COMPOSER_BUCKET>/data/
├── common/
│   ├── __init__.py
│   ├── spark_utils.py
│   ├── gcs_utils.py
│   ├── bq_utils.py
│   ├── log_utils.py
│   └── config_utils.py
└── INGESTION/
    ├── hospitalA_mysqlToLanding.py
    ├── hospitalB_mysqlToLanding.py
    └── claims.py

Why common is __init__  package ?
Without __init__.py, Python treats common/ as just a directory, not importable code.
Final mental model

GCS is just storage
Python still needs packages

So:

Folder + __init__.py = importable package
"""


"""
✅ Example: spark_utils.py
from pyspark.sql import SparkSession

def get_spark(app_name: str):
    return (
        SparkSession.builder
        .appName(app_name)
        .getOrCreate()
    )

✅ Example: config_utils.py
import os

def get_env(name: str, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required env var: {name}")
    return value

✅ Example: gcs_utils.py
from google.cloud import storage

def upload_string(bucket, path, data, content_type="application/json"):
    client = storage.Client()
    blob = client.bucket(bucket).blob(path)
    blob.upload_from_string(data, content_type=content_type)

✅ Use utilities in job files
from common.spark_utils import get_spark
from common.config_utils import get_env
from common.gcs_utils import upload_string

spark = get_spark("hospitalA")

GCS_BUCKET = get_env("GCS_BUCKET", required=True)

✅ Attach common utils to Dataproc job (Composer DAG)
Option A — ZIP (BEST PRACTICE)
PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": GCS_JOB_FILE,
        "python_file_uris": [
            f"gs://{COMPOSER_BUCKET}/data/common/common_lib.zip"
        ]
    }
}
"""