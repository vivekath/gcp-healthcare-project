from google.cloud import storage

# Initialize client
client = storage.Client()

# Get bucket
bucket = client.bucket("your-bucket-name")

# Get blob reference
blob = bucket.blob("path/to/blob.txt")

blob.upload_from_filename("local_file.txt")
blob.upload_from_string("Hello, GCS!")
with open("local_file.txt", "rb") as f:
    blob.upload_from_file(f)
import json
data = {"name": "ChatGPT", "type": "AI"}
blob.upload_from_string(json.dumps(data), content_type="application/json")
blob.download_to_filename("downloaded.txt")
content = blob.download_as_text()
data = blob.download_as_bytes()
with open("downloaded.txt", "wb") as f:
    blob.download_to_file(f)
exists = storage.Blob(bucket=bucket, name="path/to/blob.txt").exists(client)
blob.reload()  # refresh metadata
print(blob.content_type, blob.size, blob.updated)
blob.delete()
new_blob = bucket.rename_blob(blob, "new/path/newname.txt")
destination_bucket = client.bucket("other-bucket")
new_blob = bucket.copy_blob(blob, destination_bucket, "copied_blob.txt")
for blob in bucket.list_blobs():
    print(blob.name)
for blob in bucket.list_blobs(prefix="folder/subfolder/"):
    print(blob.name)
