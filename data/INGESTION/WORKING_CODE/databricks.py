# Databricks Free Edition is completely managed by Databricks, not by any cloud provider.

# That means:

# Feature	Free Edition
# Runs on GCP / AWS / Azure => ❌ No
# Access GCS buckets => ❌ No
# Use GCP IAM / Service Accounts => ❌ No
# Create cloud clusters => ❌ No
# Attach VPC / networking => ❌ No
# Billing account => ❌ Not required

# Free Edition runs in Databricks’ own managed environment.

# --------------------------------------------------------------------------
# What you CAN do in Free Edition (for learning)

# ✅ PySpark / Spark SQL
# ✅ Delta Lake concepts
# ✅ Notebooks & workflows
# ✅ Basic data engineering practice
# ✅ Small datasets
# ✅ Databricks UI, jobs, SQL editor

# 👉 Perfect for learning Spark & Databricks concepts
# --------------------------------------------------------------------------
# which things will cost me on databricks ?
💸 What Costs Money in Databricks (GCP)
🔥 1️⃣ Compute (BIGGEST COST)

This is where most of your money goes.

What counts as compute

Clusters (all-purpose & job clusters)

SQL Warehouses

Jobs that run Spark

Photon-enabled compute

How you are charged

You pay for:

DBUs (Databricks Units) + GCP VM cost


📌 Even if your Spark job is idle, running clusters still cost money.

🧮 2️⃣ DBUs (Databricks Units)

DBUs are Databricks’ usage metric.

DBUs depend on:

Cluster type

VM size

Runtime

Number of nodes

Example:

Small cluster × 1 hour = X DBUs

Large cluster × 5 hours = $$$

👉 DBUs are charged per second while compute is running.

☁️ 3️⃣ GCP Infrastructure (Hidden but real)

These are GCP charges, billed separately:

Compute Engine VMs

Persistent disks

Network egress

Load balancers (if any)

Even if DBUs are low, VMs still cost.

📊 4️⃣ SQL Warehouses

Often forgotten and expensive.

Auto-start warehouses

BI tools keep them running

Charged even when queries are light

⚠️ Leave this ON → silent billing.

🔁 5️⃣ Jobs & Workflows

Scheduled jobs

Triggered pipelines

Retrying jobs

Each run = compute + DBUs.

🧠 6️⃣ Advanced / Enterprise Features

May add extra DBU cost:

Photon

Unity Catalog

Serverless compute

MLflow tracking (compute part)

❌ What Does NOT Cost Money

These are free:

Creating notebooks

Writing code

Workspace UI

Git integration

Stopping clusters

Browsing logs

🧾 Cost summary table
Component	Cost?
Notebook creation	❌
Idle cluster (running)	✅
Stopped cluster	❌
SQL Warehouse	✅
DBUs	✅
GCS storage	✅ (GCP)
Jobs	✅
Free Edition	❌
🚨 Common beginner mistakes (avoid these)

❌ Leaving cluster running overnight
❌ Forgetting SQL warehouse
❌ Using multi-node cluster for practice
❌ No auto-termination
❌ Photon enabled unnecessarily
# --------------------------------------------------------------------------
👉 No cluster = no Python execution environment.
👉 Use Databricks Free Edition or local Jupyter
# --------------------------------------------------------------------------
# catalog metastore, workspace creation, githubt configuration, notebook creation, gcs configuration will cost
💰 Cost Breakdown (Your Exact List)
1️⃣ Catalog / Metastore (Unity Catalog)

❌ No direct Databricks cost

Creating a catalog

Creating a schema

Creating a metastore

Assigning metastore to workspace

👉 Free by itself

⚠️ BUT:

If you query tables → compute cost

If metadata is stored in GCS → GCS storage cost (tiny)

Verdict:
🟢 Creation = FREE
🔴 Usage (queries) = COST (compute)

2️⃣ Workspace Creation

❌ No cost

Creating Databricks workspace

Deleting workspace

Workspace settings

👉 No DBUs, no GCP compute used.

Verdict: 🟢 FREE

3️⃣ GitHub Configuration (Repos / Git integration)

❌ No cost

Linking GitHub repo

Pull / push code

Using Databricks Repos

👉 This is purely control-plane.

Verdict: 🟢 FREE

4️⃣ Notebook Creation

❌ No cost

Creating notebooks

Writing code

Editing notebooks

Saving notebooks

⚠️ Cost only happens when:

You attach notebook to a cluster

You run a cell

Verdict:
🟢 Create/Edit = FREE
🔴 Run = COST (compute)

5️⃣ GCS Configuration
a) Databricks ↔ GCS connection setup

❌ No Databricks cost

Creating external locations

Storage credentials

IAM/service accounts

👉 Control-plane only.

Verdict: 🟢 FREE

b) Actual GCS usage

✅ GCP cost applies

Storing files

Reading/writing data

Network egress (rare)

⚠️ GCS is cheap but not free.

Verdict:
🟢 Setup = FREE
🔴 Data usage = COST (GCP)

✅ Final Cost Summary Table
Item	Costs Money?
Metastore creation	❌
Catalog / Schema	❌
Workspace creation	❌
GitHub integration	❌
Notebook creation	❌
Running notebooks	✅
Cluster running	✅
GCS setup	❌
GCS storage	✅
SQL warehouse	✅
# ------------------------------------------------------------------------------------------
