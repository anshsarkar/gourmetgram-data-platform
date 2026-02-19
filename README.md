# GourmetGram Data Platform — Lab 4

End-to-end MLOps lab: real-time data ingestion, batch ETL, streaming model training, and live inference on a social food platform.

---

## Architecture Overview

![GourmetGram Data Platform Architecture](Architecture.png)

```
[Food-11 Dataset]
       │
       ▼
[Data Generator] ──► [FastAPI] ──┬──► Postgres (users, images, comments, flags)
                                  │
                                  └──► Kafka (views/comments/uploads/flags)
                                                   │
                                                   ▼
                                          [Stream Consumer]
                                        Redis (rolling window
                                           aggregations)
                                                   │
                                                   │ viral/suspicious alert
                                                   ▼
                                       Kafka: moderation_requests
                                                   │
                                                   ▼
                                         [Inference Service]
                                          heuristic or model
                                                   │
                                                   ▼
                                       Postgres: moderation_decisions


[Airflow ETL] ──reads──► Postgres
      │
      └──► Iceberg tables (MinIO)
                  │
                  ▼
        [Training Notebook]
                  │
                  └──► model artifacts (MinIO)
                                │
                     [Inference Service] ◄───┘
                      (hot-reloads model)
```

**Services and UIs:**

| Service | Purpose | URL |
|---------|---------|-----|
| FastAPI | REST API — upload images, record views/comments/flags | http://[FLOATING_IP]:8000/docs |
| Airflow | Batch ETL DAGs — extract Postgres → feature engineering → Iceberg | http://[FLOATING_IP]:8080 |
| Adminer | PostgreSQL UI — browse all tables | http://[FLOATING_IP]:5050 |
| Redpanda Console | Kafka topic browser | http://[FLOATING_IP]:8090 |
| Redis Insight | Redis key browser — rolling window counts | http://[FLOATING_IP]:8081 |
| MinIO Console | Object storage — raw data, Iceberg warehouse, model artifacts | http://[FLOATING_IP]:9001 |
| Nimtable | Iceberg table browser | http://[FLOATING_IP]:3000 |

---

## Step 1 — Provision a Chameleon Instance

### 1a. Create a lease

Go to the [KVM@TACC dashboard](https://kvm.tacc.chameleoncloud.org/project/) and make sure you have selected your course project from the project dropdown at the top of the page.

From the left sidebar, navigate to **Reservations → Leases** and click **Create Lease**.

Fill in the lease details:
- **Lease Name**: `lab-4-data-platform`
- **Start Date**: today's date | **Start Time**: now (current time)
- **End Date / End Time**: 4 hours from now
- Make sure all times are in **UTC**

Click **Next** to go to the Flavors page. Check the **Reserve Flavors** checkbox, then select `m1.xlarge` from the list. Click **Next**. On the Networks page leave everything as default and click **Next**, then click **Create Lease**.

Wait for the lease status to show **Active** before proceeding.

### 1b. Launch the instance

From the left sidebar, go to **Compute → Instances** and click **Launch Instance**.

Work through each page of the wizard:

1. **Details** — Set the instance name to `lab-4-data-platform-[NETID]` (replace `[NETID]` with your NYU NetID). Click **Next**.
2. **Source** — Search for `Ubuntu24.04` in the image list and select it. Click **Next**.
3. **Flavor** — Search for your lease name (`lab-4-data-platform`) in the list and select it. This ties the instance to your reserved hardware. Click **Next**.
4. **Networks** — Add `sharednet1` to the selected networks list. Click **Next**.
5. **Network Ports** — Leave as default. Click **Next**.
6. **Key Pair** — Select the SSH key pair you set up for this course. Click **Next**.
7. **Configuration** and **Scheduler Hints** — Leave as default.

Click **Launch Instance**. The instance will appear in the list with status **Spawning**, then transition to **Active** after about a minute.

### 1c. Associate a floating IP

A floating IP is a publicly routable address that lets you reach the instance (and all its services) from your local machine.

In the Instances list, click the **dropdown arrow** next to the **Create Snapshot** button for your instance and select **Associate Floating IP**.

In the dialog, select an available floating IP from the dropdown and click **Associate**. The floating IP will now appear in the instance row under the **IP Address** column.

Note this IP address — replace `[FLOATING_IP]` with it everywhere in this lab.

Open a terminal on your local machine and SSH in to verify:

```bash
ssh -i ~/.ssh/id_rsa_chameleon cc@[FLOATING_IP]
```

If you stored your key under a different path:
```bash
ssh -i [PATH_TO_YOUR_KEY] cc@[FLOATING_IP]
```

You should see a welcome message from Ubuntu. You are now inside the Chameleon instance.

### 1d. Open security group ports

By default, the instance only allows SSH traffic. You need to open ports for each service in this lab.

From the Instances list, click the dropdown next to **Create Snapshot** → **Edit Security Groups**.

Add each of the following security groups by clicking the **+** next to their name. If a group is missing from the list, see the note below.

| Security Group | Port | Service |
|---------------|------|---------|
| `allow-8000` | 8000 | FastAPI |
| `allow-8080` | 8080 | Airflow |
| `allow-5050` | 5050 | Adminer |
| `allow-8090` | 8090 | Redpanda Console |
| `allow-8081` | 8081 | Redis Insight |
| `allow-9001` | 9001 | MinIO Console |
| `allow-3000` | 3000 | Nimtable |
| `allow-8888` | 8888 | Jupyter (training notebook) |

Click **Save**.

> **If a security group is missing:** Go to **Network → Security Groups** from the left sidebar and click **Create Security Group**. Give it the name `allow-[PORT]` (e.g. `allow-8000`). Once created, click the dropdown next to it → **Manage Rules** → **Add Rule**. Leave everything default except set **Port** to the port number you want to open (e.g. `8000`). Click **Add**. Then go back to your instance and add the newly created group via **Edit Security Groups**.

---

## Step 2 — Set Up the Instance

Clone the repository and run the setup scripts:

```bash
git clone https://github.com/anshsarkar/gourmetgram-data-platform.git
cd gourmetgram-data-platform
bash ./scripts/setup.sh    # installs Docker and dependencies
```

Log out and back in to apply Docker group permissions, then:

```bash
cd gourmetgram-data-platform
bash ./scripts/conda.sh    # sets up Conda environment
```

Log out and back in again, then install the simulation script dependencies:

```bash
pip install kafka-python psycopg2-binary pandas numpy scikit-learn
```


## Step 3 — Bring Up the Platform

Start all services from the `docker/` directory:

```bash
cd gourmetgram-data-platform/docker
docker compose up -d
```

This single command starts everything: Postgres, MinIO, Redpanda (Kafka), Redis, FastAPI, the data generator, stream consumer, inference service, Airflow, and all UI tools.

**First-time startup takes 3–5 minutes** — the `extract-data` and `transform-data` containers download and organize the Food-11 dataset (~1.2 GB) before the data generator starts.

Wait for all services to be healthy:
```bash
docker compose ps
```

All services should show `running` or `healthy`. You should see output like:
```
NAME                 STATUS
adminer              running
airflow-scheduler    running
airflow-webserver    running (healthy)
api                  running (healthy)
generator            running
inference-service    running
minio                running (healthy)
postgres             running (healthy)
redpanda             running (healthy)
redis                running (healthy)
redisinsight         running
stream-consumer      running
```

---

## Step 4 — Watch Data Flowing Through the Platform

The data generator is already running and simulating user activity on GourmetGram. Let's look at what each layer of the platform is doing.

### 4a. Kafka — Event Stream

Open **Redpanda Console** → http://[FLOATING_IP]:8090 → **Topics**

You'll see four event topics being populated in real time:
- `gourmetgram.views` — image view events
- `gourmetgram.comments` — comment events
- `gourmetgram.uploads` — new image upload events
- `gourmetgram.flags` — content flag events

Click on any topic → **Messages** to see the raw JSON events flowing through.

### 4b. Redis — Real-time Aggregations

Open **Redis Insight** → http://[FLOATING_IP]:8081

The stream consumer reads Kafka events and maintains **sliding window counts** for every image. Browse the keys:
- `image:<uuid>:views` — sorted set of view timestamps (score = unix timestamp)
- `image:<uuid>:comments` — sorted set of comment timestamps
- `image:<uuid>:metadata` — hash with category, caption length, upload time

The stream consumer uses `ZCOUNT` with a time range to count events within any window (1 min, 5 min, 1 hr) without storing pre-aggregated counters — the window is computed on read.

### 4c. PostgreSQL — Persistent State

Open **Adminer** → http://[FLOATING_IP]:5050

Login:
- **System**: PostgreSQL
- **Server**: postgres
- **Username**: user
- **Password**: password
- **Database**: gourmetgram

Browse these tables to see data accumulating:
- `users` — registered user accounts
- `images` — uploaded images with metadata (category, caption, MinIO path)
- `comments` — comments on images
- `flags` — user-reported content flags
- `image_milestones` — when each image hit 10 / 100 / 1000 total views

### 4d. Stream Consumer Logs

```bash
docker logs -f stream-consumer
```

Watch for milestone and alert messages:
```
🎯 MILESTONE REACHED! Image a1b2c3d4... hit 10 total views
⭐ POPULAR POST! Image e5f6g7h8... received 50 views in 1 hour
```

### 4e. FastAPI — API Documentation

Open **http://[FLOATING_IP]:8000/docs** to browse the REST API. The data generator uses this API to upload images and simulate user interactions.

---

## Step 5 — Trigger Heuristic Inference

The inference service is running but waiting for moderation requests. It operates in **heuristic mode** (no trained model yet) — a rule-based scorer that works out of the box.

### 5a. Stop the data generator

The data generator runs at a moderate pace and won't naturally trigger a viral alert quickly. Stop it first:
```bash
docker compose stop data_generator
```

### 5b. Simulate a viral event burst

Run the simulation script from the project root. It picks a real image from the database and publishes 110 view events to Kafka — just over the viral threshold (100 views in 5 minutes):

```bash
cd ..   # if you're in docker/
pip install kafka-python psycopg2-binary   # install on host if not present
python3 scripts/simulate_viral.py
```

You'll see:
```
Simulating viral burst for image: a1b2c3d4-...
Publishing 110 view events to 'gourmetgram.views'...
Done. 110 view events published.
```

### 5c. Watch the viral alert fire

```bash
docker logs -f stream-consumer
```

Within a few seconds:
```
🔥 VIRAL CONTENT ALERT! Image a1b2c3d4... received 110 views in 5 minutes (threshold: 100)
Published moderation request for a1b2c3d4... (trigger: viral)
```

The stream consumer published a message to `gourmetgram.moderation_requests`. Now the inference service picks it up.

### 5d. Watch the heuristic inference decision

```bash
docker logs -f inference-service
```

```
[HEURISTIC] Image a1b2c3d4... → probability=0.42, recommendation=REVIEW
Stored moderation decision for a1b2c3d4...
```

### 5e. Query the decision in Adminer

In **Adminer** → `moderation_decisions` table:

```sql
SELECT image_id, inference_mode, moderation_probability, recommendation, trigger_type, created_at
FROM moderation_decisions
ORDER BY created_at DESC
LIMIT 10;
```

You'll see the decision with `inference_mode = heuristic`.

**How heuristic scoring works** (`inference_service/heuristic_predictor.py`):

| Signal | Score |
|--------|-------|
| Image has flags | +0.4 |
| Comment spam (10+/min) | +0.3 |
| High comment-to-view ratio (>0.5) | +0.2 |
| Bot-like pattern | +0.3 |

Scores above 0.7 → **FLAG**, 0.3–0.7 → **REVIEW**, below 0.3 → **SAFE**.

---

## Step 6 — Run the Batch ETL to Build Training Data

The inference service works heuristically, but we want a trained ML model. First we need training data — that's what the Airflow ETL pipeline creates.

### 6a. Restart the data generator

Let it run for a few minutes to accumulate data before triggering ETL:
```bash
docker compose start data_generator
```

Wait 2–3 minutes, then check how much data has accumulated:
```sql
-- In Adminer
SELECT COUNT(*) FROM images;
SELECT COUNT(*) FROM comments;
SELECT COUNT(*) FROM flags;
```

### 6b. Open Airflow

Go to **http://[FLOATING_IP]:8080** — login: `admin` / `admin`

You'll see two DAGs, all paused by default:

| DAG | Schedule | Purpose |
|-----|----------|---------|
| `redpanda_event_aggregation` | Hourly | Kafka → windowed Iceberg tables |
| `etl_moderation` | Daily | Postgres → feature engineering → training dataset |


### 6c. Run the event aggregation DAG first

In Airflow UI → **DAGs** → `redpanda_event_aggregation` → Toggle ON (unpause) from toggle option towards the left of the DAG → This should automatically trigger and run immediately since it's scheduled hourly. If not, click **Trigger DAG**.

This reads the last hour of Kafka events and writes aggregated 5-minute windows to three Iceberg tables in MinIO.

Watch it complete in the **Graph** view — all tasks should turn green.

### 6d. Run the main ETL DAG

DAGs → `etl_moderation` → Toggle ON (unpause) from toggle option towards the left of the DAG → This should trigger immediately since it's scheduled daily. If not, click **Trigger DAG**.

This runs three tasks:
1. **`extract_data`** — dumps `users`, `images`, `comments`, `flags`, `image_milestones` from Postgres to MinIO as Parquet files at `s3://gourmetgram-datalake/raw/`
2. **`transform_features`** — joins the tables, computes 26 features per image at multiple time snapshots (0, 5, 30 min after upload), labels each row (flagged within 24h = 1)
3. **`load_iceberg`** — writes the processed dataset to Iceberg table `moderation.training_data`

Watch the task graph turn green. Each task logs detail — click a task → **Logs** to see what it processed.

### 6e. Verify the training data

```bash
docker exec airflow-scheduler python /opt/airflow/dags/verify_pipeline.py
```

Output:
```
Table: moderation.training_data
Row count: 1247
Sample rows: ...
```

### 6f. Browse the Iceberg table in Nimtable

Open **Nimtable** → http://[FLOATING_IP]:3000 — login: `admin` / `admin`

Nimtable starts with no catalogs configured. Connect it to the platform:

1. From the left sidebar, click **Catalogs** → **Add Catalog** → **Connect Catalog**
2. Fill in the form:

   | Field | Value |
   |-------|-------|
   | Connection Method | `JDBC + S3` |
   | Catalog Name | `gourmetgram` |
   | Catalog Endpoint | `jdbc:postgresql://postgres:5432/gourmetgram` |
   | Warehouse | `s3://gourmetgram-datalake/warehouse` |
   | S3 Endpoint | `http://minio:9000` |
   | Access Key | `admin` |
   | Secret Key | `password` |

3. Click **Advanced Options** and add:

   | Property | Value |
   |----------|-------|
   | `jdbc.user` | `user` |
   | `jdbc.password` | `password` |

4. Click **Connect Catalog**. You should see a success message.

Navigate to namespace `moderation` → table `training_data` to browse the schema and preview rows. Iceberg stores the data as Parquet files in MinIO — Nimtable is just a catalog browser.

You can also see the raw Parquet files at **MinIO Console** → http://[FLOATING_IP]:9001 → bucket `gourmetgram-datalake` → `warehouse/moderation/training_data/`.

---

## Step 7 — Train a Model

Now we'll train a moderation classifier on the Iceberg data. The training notebook has **four scenarios** that demonstrate a key MLOps concept: what happens when your training data doesn't fit in memory.

### 7a. Generate a large dataset for the OOM demo

This creates a synthetic 10M-row table in Iceberg to simulate a production-scale dataset:

```bash
cd model_training
docker build -t gourmetgram-trainer:latest .

docker run --rm --network host \
  -e PYICEBERG_CATALOG__GOURMETGRAM__TYPE=sql \
  -e PYICEBERG_CATALOG__GOURMETGRAM__URI=postgresql+psycopg2://user:password@localhost:5432/gourmetgram \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT=http://localhost:9000 \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ACCESS_KEY_ID=admin \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__SECRET_ACCESS_KEY=password \
  -e PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE=s3://gourmetgram-datalake/warehouse \
  gourmetgram-trainer:latest \
  python3 generate_large_dataset.py
```

### 7b. Launch the training notebook

The `--memory="512m"` flag caps the container at 512 MB — this is intentional. It forces the OOM crash in Scenario 2 so you can see what happens when you try to load a 6M+ row dataset into a Pandas DataFrame.

```bash
docker run --rm --memory="1g" \
  --network host \
  -p 8888:8888 \
  -v $(pwd)/models:/app/models \
  -e PYICEBERG_CATALOG__GOURMETGRAM__TYPE=sql \
  -e PYICEBERG_CATALOG__GOURMETGRAM__URI=postgresql+psycopg2://user:password@localhost:5432/gourmetgram \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT=http://localhost:9000 \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ACCESS_KEY_ID=admin \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__SECRET_ACCESS_KEY=password \
  -e PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE=s3://gourmetgram-datalake/warehouse \
  -e S3_ENDPOINT_URL=http://localhost:9000 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  gourmetgram-trainer:latest
```

Open the Jupyter link from the terminal output (`http://127.0.0.1:8888/tree?token=...`), then open `training_large_data_demo.ipynb`. Replace 127.0.0.1 with you Chameleon instance's floating IP when accessing from your host machine.

### 7c. Run the four scenarios in order

**Scenario 1 — Normal load**

Loads the small `moderation.training_data` table into a Pandas DataFrame and trains a `LogisticRegression`. Works fine — the dataset fits in memory.

**Scenario 2 — Memory crash (intentional)**

Tries to load `moderation.large_training_data` (6M+ rows) all at once with `table.scan().to_pandas()`. With the 512 MB container limit, this **crashes with an OOM error**. This is the point — loading big datasets directly into Pandas doesn't scale.

**Scenario 3 — Streaming solution**

Uses PyIceberg's `ArrowBatchReader` to stream the large dataset in batches of ~10K rows. Trains an `SGDClassifier` with `partial_fit()` — one batch at a time, without ever loading the full dataset. Memory usage stays flat regardless of dataset size. Saves artifacts to `models/` inside the container (which is bind-mounted to `model_training/models/` on your host).

**Scenario 4 — Train on real data and upload to MinIO**

Now switch to `training.ipynb`, which trains on the real `moderation.training_data` table produced by the ETL pipeline. Run the cells in order.

> **Note:** Make sure cells 3 and 5 remain commented out — those are the OOM demo cells from the large dataset exercise and are not needed here.

The final cell uploads `model.joblib`, `scaler.joblib`, `encoder.joblib`, and `metadata.json` to MinIO. The inference service polls for `metadata.json` every 30 seconds — uploading it triggers an automatic model reload.

### 7d. Verify artifacts in MinIO

Open **MinIO Console** → http://[FLOATING_IP]:9001 → bucket `gourmetgram-datalake` → `models/moderation/`

You should see four files: `model.joblib`, `scaler.joblib`, `encoder.joblib`, `metadata.json`.

---

## Step 8 — Switch to Model Inference

The inference service is still running in heuristic mode. Now we'll switch it to use the trained model.

### 8a. Edit the toggle in moderator.py

Open `inference_service/moderator.py` and find the toggle at the top of the file:

```bash
cd gourmetgram-data-platform/inference_service
nano moderator.py
```

```python
# ============================================================
# INFERENCE MODE TOGGLE
# Students: Change this to True AFTER training a model
# ============================================================
USE_MODEL = False   # <-- CHANGE TO True TO USE TRAINED MODEL
# ============================================================
```

Change `False` → `True` and save.

Pess `Ctrl+X`, then `Y`, then `Enter` to save and exit Nano.

### 8b. Rebuild and restart the inference service

```bash
cd ../docker
docker compose up -d --build inference_service
```

### 8c. Watch the model load

```bash
docker logs -f inference-service
```

You'll see:
```
Checking MinIO for new model artifacts...
Downloading model from s3://gourmetgram-datalake/models/moderation/
Model loaded successfully (version: 1, trained on N rows)
```

### 8d. Simulate another viral event

```bash
cd ..
python3 simulate_viral.py
```

```bash
docker logs -f inference-service
```

This time the log shows model-based inference:
```
[MODEL] Image a1b2c3d4... → probability=0.61, recommendation=REVIEW (mode=model, version=1)
Stored moderation decision for a1b2c3d4...
```

### 8e. Compare heuristic vs model decisions in Adminer

```sql
SELECT inference_mode, COUNT(*), AVG(moderation_probability),
       COUNT(*) FILTER (WHERE recommendation = 'FLAG') AS flagged,
       COUNT(*) FILTER (WHERE recommendation = 'REVIEW') AS review,
       COUNT(*) FILTER (WHERE recommendation = 'SAFE') AS safe
FROM moderation_decisions
GROUP BY inference_mode;
```

You'll see two rows — one for `heuristic`, one for `model` — with different distributions across SAFE / REVIEW / FLAG.

---

## Service Reference

| Component | Container | Purpose |
|-----------|-----------|---------|
| `api` | FastAPI | REST API — image upload, views, comments, flags |
| `generator` | Data generator | Simulates user activity using Food-11 dataset |
| `stream-consumer` | Stream consumer | Kafka → Redis aggregations, viral/suspicious detection |
| `inference-service` | Inference service | Moderation decisions (heuristic or model) |
| `airflow-scheduler` | Airflow | Runs batch ETL DAGs on schedule |
| `airflow-webserver` | Airflow | DAG management UI |
| `postgres` | PostgreSQL | Platform data + Airflow metadata + Iceberg catalog |
| `minio` | MinIO | Raw Parquet files, Iceberg warehouse, model artifacts |
| `redpanda` | Kafka-compatible broker | All event streaming |
| `redis` | Redis | Real-time rolling window feature store |

## Useful Commands

```bash
# Tail logs for any service
docker logs -f <container-name>

# Check all container statuses
docker compose ps

# Restart a single service
docker compose restart <service>

# Stop everything (keeps volumes / data)
docker compose down

# Full reset — DELETES all data
docker compose down -v
```
