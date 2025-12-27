In YOUR architecture (best practice)
Stage	Runner
Local dev / Colab DirectRunner
CI tests	DirectRunner
Prod batch	DataflowRunner
Prod streaming	DataflowRunner
# ----------------------------------------------------------
4️⃣ FlinkRunnerrunner="FlinkRunner"

✔ Streaming-heavy workloads
✔ Stateful processing


3️⃣ SparkRunner (Apache Spark)
runner="SparkRunner"


✔ Runs on Spark clusters (Dataproc, EMR, etc.)

❌ Rare in GCP when Dataflow exists

2️⃣ DataflowRunner (Google Cloud)
runner="DataflowRunner"


✔ Fully managed
✔ Auto-scaling
✔ Batch & Streaming
✔ Production-ready

Used for:

Large datasets

Production pipelines

Streaming jobs

1️⃣ DirectRunner (Local)
runner="DirectRunner"


✔ Runs locally
✔ Used for:

Development

Testing

Debugging

Unit tests

❌ Not scalable
❌ No distributed processing