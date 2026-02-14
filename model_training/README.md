# Model Training

Jupyter notebook that trains a moderation classifier on the GourmetGram dataset. Designed to demonstrate out-of-core (streaming) training for datasets that don't fit in memory, using PyIceberg as the data source and MinIO for storing model artifacts.

## What It Does

Trains an `SGDClassifier` to predict whether an image will need moderation within 24 hours. The model is trained on the 26-feature vectors produced by the ETL pipeline and uploaded to MinIO so the inference service can pick it up automatically.

## Input

- **Iceberg table** `moderation.training_data` — created by the `etl_moderation` Airflow DAG
- **Iceberg table** `moderation.large_training_data` — created by `generate_large_dataset.py` for the OOM demo

> Run the Airflow ETL DAGs first — the notebook needs data in Iceberg to train on.

## Output / What Gets Stored

| Artifact | Location |
|----------|----------|
| `model.joblib` | MinIO → `s3://gourmetgram-datalake/models/moderation/model.joblib` |
| `scaler.joblib` | MinIO → `s3://gourmetgram-datalake/models/moderation/scaler.joblib` |
| `encoder.joblib` | MinIO → `s3://gourmetgram-datalake/models/moderation/encoder.joblib` |
| `metadata.json` | MinIO → `s3://gourmetgram-datalake/models/moderation/metadata.json` |

The inference service watches `metadata.json` every 30 seconds — uploading it triggers an automatic model reload.

## Notebook Scenarios

| Scenario | What Happens |
|----------|-------------|
| 1 — Normal Load | Loads small dataset into memory, trains `LogisticRegression`. Works fine. |
| 2 — Memory Crash | Tries to load the large dataset (6M+ rows) all at once. **Intentionally crashes** to show OOM. |
| 3 — Streaming Solution | Uses `SGDClassifier.partial_fit()` with `ArrowBatchReader` to stream the large dataset in batches. Saves artifacts locally to `models/`. |
| 4 — Upload to MinIO | Uploads saved artifacts to MinIO and writes `metadata.json` to notify the inference service. |

## How to Run

### 1. Build the image
```bash
cd model_training
docker build -t gourmetgram-trainer:latest .
```

### 2. (One-time) Generate large dataset for the OOM demo
```bash
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

### 3. Run the notebook (512MB limit forces the OOM crash in Scenario 2)
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
  -e S3_ENDPOINT_URL=http://localhost:9000 \
  -e AWS_ACCESS_KEY_ID=admin \
  -e AWS_SECRET_ACCESS_KEY=password \
  gourmetgram-trainer:latest
```

Open the Jupyter link from terminal output (`http://127.0.0.1:8888/tree?token=...`), then run scenarios in order.

## Dependencies

```
pandas, numpy                      # data manipulation
scikit-learn                       # SGDClassifier, StandardScaler, OneHotEncoder
pyiceberg[s3fs,sql-postgres]       # read training data from Iceberg
joblib                             # save/load model artifacts
pyarrow                            # Arrow batch reading for streaming
boto3                              # upload artifacts to MinIO
jupyter, notebook                  # notebook runtime
```

## Files

- `training.ipynb` — the main notebook (4 scenarios)
- `generate_large_dataset.py` — creates a 10M-row synthetic Iceberg table for the OOM demo
- `Dockerfile` — builds the `gourmetgram-trainer` image
- `requirements.txt` — Python dependencies
