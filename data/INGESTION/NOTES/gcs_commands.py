# ✅ Common gsutil commands
# Task	Command
"""
List all buckets	gsutil ls
List contents of a bucket	gsutil ls gs://BUCKET_NAME
List with detail	gsutil ls -l gs://BUCKET_NAME
Create a bucket	gsutil mb gs://BUCKET_NAME/
Upload a file	gsutil cp localfile.jpg gs://BUCKET_NAME/
Upload many files (wildcard)	gsutil cp *.pdf gs://BUCKET_NAME/
Download a file	gsutil cp gs://BUCKET_NAME/file.ext ./localfolder/
Copy object between buckets	gsutil cp gs://SOURCE_BUCKET/file gs://DEST_BUCKET/file
Move or rename object	gsutil mv gs://BUCKET/oldname gs://BUCKET/newname
Delete object	gsutil rm gs://BUCKET/file.txt
Delete many objects	gsutil -m rm gs://BUCKET/*.txt
Delete bucket + all contents	gsutil rm -r gs://BUCKET_NAME
Make object publicly readable	gsutil acl ch -u AllUsers:R gs://BUCKET_NAME/file
Make whole bucket publicly readable	gsutil iam ch allUsers:objectViewer gs://BUCKET_NAME
"""
