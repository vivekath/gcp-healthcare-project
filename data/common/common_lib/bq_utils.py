from google.cloud import bigquery
from google.api_core.exceptions import NotFound

def get_bq_client(project_id: str = None):
    return bigquery.Client(project=project_id)

def dataset_exists(client, dataset_id: str) -> bool:
    try:
        client.get_dataset(dataset_id)
        return True
    except NotFound:
        return False

def create_dataset(client, dataset_id: str, location="US"):
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = location
    client.create_dataset(dataset, exists_ok=True)

def table_exists(client, table_id: str) -> bool:
    try:
        client.get_table(table_id)
        return True
    except Exception:
        return False

def run_query(client, query: str):
    job = client.query(query)
    return job.result()

def create_table_from_query(client, query: str, table_id: str):
    job_config = bigquery.QueryJobConfig(
        destination=table_id,
        write_disposition="WRITE_TRUNCATE"
    )
    job = client.query(query, job_config=job_config)
    job.result()

def load_gcs_to_bq(
    client,
    source_uri: str,
    table_id: str,
    source_format=bigquery.SourceFormat.PARQUET,
    autodetect=True
):
    job_config = bigquery.LoadJobConfig(
        source_format=source_format,
        autodetect=autodetect
    )
    load_job = client.load_table_from_uri(
        source_uri, table_id, job_config=job_config
    )
    load_job.result()


def create_external_table(
    client,
    table_id: str,
    source_uris: list,
    source_format="PARQUET"
):
    table = bigquery.Table(table_id)
    external_config = bigquery.ExternalConfig(source_format)
    external_config.source_uris = source_uris
    table.external_data_configuration = external_config
    client.create_table(table, exists_ok=True)

def delete_table(client, table_id: str):
    client.delete_table(table_id, not_found_ok=True)

def get_row_count(client, table_id: str) -> int:
    table = client.get_table(table_id)
    return table.num_rows

def create_partitioned_table(
    client,
    table_id: str,
    schema,
    partition_field: str,
    cluster_fields: list = None
):
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(
        field=partition_field
    )
    if cluster_fields:
        table.clustering_fields = cluster_fields
    client.create_table(table, exists_ok=True)
