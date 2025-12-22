from google.cloud import storage
from google.api_core.exceptions import NotFound

def get_gcs_client(project_id: str = None):
    return storage.Client(project=project_id)

def bucket_exists(client, bucket_name: str) -> bool:
    try:
        client.get_bucket(bucket_name)
        return True
    except NotFound:
        return False

def create_bucket(client, bucket_name: str, location="US"):
    bucket = client.bucket(bucket_name)
    client.create_bucket(bucket, location=location)

def blob_exists(client, bucket_name: str, blob_path: str) -> bool:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.exists()

def upload_file(
    client,
    bucket_name: str,
    source_file: str,
    destination_blob: str
):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_filename(source_file)

def upload_string(
    client,
    bucket_name: str,
    destination_blob: str,
    data: str,
    content_type="text/plain"
):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob)
    blob.upload_from_string(data, content_type=content_type)

def download_file(
    client,
    bucket_name: str,
    blob_path: str,
    destination_file: str
):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.download_to_filename(destination_file)

def read_blob_as_string(client, bucket_name: str, blob_path: str) -> str:
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    return blob.download_as_text()

def list_blobs(client, bucket_name: str, prefix: str = None) -> list:
    return [blob.name for blob in client.list_blobs(bucket_name, prefix=prefix)]

def delete_blob(client, bucket_name: str, blob_path: str):
    bucket = client.bucket(bucket_name)
    bucket.blob(blob_path).delete()

def move_blob(
    client,
    bucket_name: str,
    source_blob: str,
    destination_blob: str
):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(source_blob)
    bucket.rename_blob(blob, destination_blob)

def copy_blob(
    client,
    source_bucket: str,
    source_blob: str,
    destination_bucket: str,
    destination_blob: str
):
    src_bucket = client.bucket(source_bucket)
    dst_bucket = client.bucket(destination_bucket)
    blob = src_bucket.blob(source_blob)
    src_bucket.copy_blob(blob, dst_bucket, destination_blob)

def delete_prefix(client, bucket_name: str, prefix: str):
    blobs = client.list_blobs(bucket_name, prefix=prefix)
    for blob in blobs:
        blob.delete()
