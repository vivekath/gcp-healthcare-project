# add both logging
# Mentain common code utils (airflow + pyspark) environment, varianles
# set dev/qa/prod variables in airflow and manage common code in one place dynamically
# don't hard code anythong, add as env variable or airflow variable and pass in dag/pyspark job
# make proper configuration for pyspark job and airflow dag
# review new/modiefied and make required corrections as per that 
# Use schema evaluation code as reference for pyspark job structure and logging
# use secretmanager to fetch sensitive information like db credentials
# Use util common code for below types of tasks
# Instead of json gcs can we use parquet for landing zone (performance optimization)

# Spark session creation
# GCS read/write helpers
# BigQuery read/write helpers
# Logging helpers
# Config / env variable readers
# Audit helpers
# Common transformations
# Date / watermark helpers
# Check how can optimise BQ tables (partitioning/clustering) while writing from pyspark job or anything check docs
# Check how to use airflow operators for dataproc job submission and monitoring
# Check how can create sparkbuilder object with required configurations
# GCS optimizations for read/write
# BQ optimizations for read/write
# Query optimizations
# logging optimizations
# Finally work on multiple use cases using pyspark (to fine tune spark code optimizations, query optimizations) and airflow (to fine tune orchestration optimizations)

# implement below optimizations as part of this task (micro concept of each topic, use all feature of below))
# BQ, Dataproc optimizations, Airflow optimizations, GCS optimizations, Logging optimizations, Query optimizations, Spark code optimizations

# after all these start all from local spark and move to dataproc serverless and airflow orchestration (all point mentioned in 111111 notes file)