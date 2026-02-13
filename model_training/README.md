# Moderation Model Training

This module trains a Logistic Regression model to classify whether an image needs manual moderation. It is designed to demonstrate **MLOps best practices** by handling large datasets that do not fit in memory.

##  Structure
- `training.ipynb`: The main notebook. Demonstrates Normal Load (Success) vs. Crash (OOM) vs. Streaming (Success).
- `generate_large_dataset.py`: Helper script to generate a 2GB+ synthetic dataset in Iceberg for the demo.
- `Dockerfile`: Reproducible environment with Jupyter and dependencies.

##  How to Run the Demo

### 1. Build the Docker Image
```bash
cd model_training
docker build -t gourmetgram-trainer:latest .
```

### 2. Generate Data (One-Time Setup)
This creates the `moderation.large_training_data` table (10M rows) in Iceberg/MinIO.
```bash
docker run --rm \
  --network host \
  -e PYICEBERG_CATALOG__GOURMETGRAM__TYPE=sql \
  -e PYICEBERG_CATALOG__GOURMETGRAM__URI=postgresql+psycopg2://user:password@localhost:5432/gourmetgram \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT=http://localhost:9000 \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ACCESS_KEY_ID=admin \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__SECRET_ACCESS_KEY=password \
  -e PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE=s3://gourmetgram-datalake/warehouse \
  gourmetgram-trainer:latest \
  python3 generate_large_dataset.py
```

### 3. Run the Notebook (With Memory Limits)
Run the container with **512MB RAM** to force the "Crash" scenario.
```bash
docker run --rm --memory="512m" \
  --network host \
  -p 8888:8888 \
  -v $(pwd)/models:/app/models \
  -e PYICEBERG_CATALOG__GOURMETGRAM__TYPE=sql \
  -e PYICEBERG_CATALOG__GOURMETGRAM__URI=postgresql+psycopg2://user:password@localhost:5432/gourmetgram \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT=http://localhost:9000 \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__ACCESS_KEY_ID=admin \
  -e PYICEBERG_CATALOG__GOURMETGRAM__S3__SECRET_ACCESS_KEY=password \
  -e PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE=s3://gourmetgram-datalake/warehouse \
  gourmetgram-trainer:latest
```

### 4. Demo Steps
1.  Open the Jupyter link (e.g., `http://127.0.0.1:8888/tree?token=...`).
2.  Open `training.ipynb`.
3.  **Run Scenario 1**: Success (Loads small data).
4.  **Run Scenario 2**: **CRASH** (Kernel dies due to OOM on 2GB data).
5.  **Run Scenario 3**: **SUCCESS** (Streams 2GB data in batches).
