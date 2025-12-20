# 6️⃣ Environment Variables is okay, but you suggested we can add as variable in Airflow UI

✅ Recommended Setup (Best Practice)
In Composer → Admin → Variables
GCS_BUCKET = heathcare-bucket-12112025
BQ_PROJECT = quantum-episode-345713
HOSPITAL_NAME = hospital-a

In DAGs
from airflow.models import Variable

GCS_BUCKET = Variable.get("GCS_BUCKET")
BQ_PROJECT = Variable.get("BQ_PROJECT")
HOSPITAL_NAME = Variable.get("HOSPITAL_NAME")

In Composer → Environment Variables
ENV = prod
GCP_REGION = us-east1

🎯 Final Recommendation for Your Case

Since:

You want flexibility

You are orchestrating multiple jobs

Values may change per environment

👉 Use Airflow Variables, not Environment Variables
👉 Environment Variables only for platform-level constants


# -------------------------------------------
# Provide a dev / qa / prod variable strategy
Below is a clean, production-grade dev / qa / prod variable strategy for Cloud Composer (Airflow) that scales well and avoids hard-coding.

No fluff, this is how it’s done in real projects.

✅ Dev / QA / Prod Variable Strategy (Composer / Airflow)
1️⃣ Single DAG, Multiple Environments (Golden Rule)

Same DAG code runs in:

dev

qa

prod

Only variables change, not code.

2️⃣ Define Environment Identifier (ONE place only)
Composer → Environment Variables
ENV = dev     # or qa / prod


Access in DAG:

import os
ENV = os.getenv("ENV", "dev")

3️⃣ Variable Naming Convention (Critical)

Prefix every Airflow Variable with environment.

dev_GCS_BUCKET
qa_GCS_BUCKET
prod_GCS_BUCKET

dev_BQ_PROJECT
qa_BQ_PROJECT
prod_BQ_PROJECT


This prevents cross-environment leaks.

4️⃣ Airflow Variables (Admin → Variables)
DEV
dev_GCS_BUCKET = heathcare-dev-bucket
dev_BQ_PROJECT = quantum-episode-dev
dev_HOSPITAL_NAME = hospital-a

QA
qa_GCS_BUCKET = heathcare-qa-bucket
qa_BQ_PROJECT = quantum-episode-qa
qa_HOSPITAL_NAME = hospital-a

PROD
prod_GCS_BUCKET = heathcare-prod-bucket
prod_BQ_PROJECT = quantum-episode-prod
prod_HOSPITAL_NAME = hospital-a

5️⃣ Central Variable Loader (Best Practice)

Create one helper function (inside DAG or utils.py):

from airflow.models import Variable
import os

ENV = os.getenv("ENV", "dev")

def get_var(key: str):
    return Variable.get(f"{ENV}_{key}")

6️⃣ Use Variables Everywhere (Clean DAG Code)
GCS_BUCKET = get_var("GCS_BUCKET")
BQ_PROJECT = get_var("BQ_PROJECT")
HOSPITAL_NAME = get_var("HOSPITAL_NAME")


No if env == dev logic anywhere else.

7️⃣ Dataproc Job Config Example
GCS_JOB_FILE = f"gs://{GCS_BUCKET}/data/INGESTION/hospitalA_mysqlToLanding.py"


Works automatically across environments.

8️⃣ BigQuery Usage Example
BQ_AUDIT_TABLE = f"{BQ_PROJECT}.temp_dataset.audit_log"

9️⃣ What Goes WHERE (Very Important)
Item	Where
Project IDs	Airflow Variables
Bucket names	Airflow Variables
Dataset names	Airflow Variables
ENV flag	Composer Env Var
Secrets (DB pwd)	Secret Manager
Machine types	DAG constants
# ---------------------------------------
🔐 Secrets Strategy (Do NOT skip)
Secret Manager
dev_mysql_password
qa_mysql_password
prod_mysql_password


Access:

from google.cloud import secretmanager

def get_secret(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{BQ_PROJECT}/secrets/{ENV}_{secret_id}/versions/latest"
    return client.access_secret_version(name=name).payload.data.decode()

10️⃣ Folder Structure (Recommended)
dags/
├── ingestion/
│   └── hospital_dag.py
├── utils/
│   ├── variables.py
│   ├── secrets.py
│   └── dataproc.py

11️⃣ Promotion Flow (Dev → QA → Prod)
Step	Action
Dev	Add dev_* variables
QA	Copy & rename to qa_*
Prod	Copy & rename to prod_*
Code	NO CHANGE
🎯 Final Best-Practice Summary

✅ One DAG
✅ One ENV variable
✅ Env-prefixed Airflow Variables
✅ Central variable loader
✅ Secrets in Secret Manager
✅ Zero hard-coding