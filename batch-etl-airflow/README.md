# GourmetGram Batch ETL Pipeline

Airflow DAGs and configuration for the GourmetGram moderation training data pipeline. It extracts data from Postgres, processes it into features, and loads it into an Iceberg table backed by MinIO.

## Pipeline Overview

The `moderation_training_etl` DAG performs the following steps daily:

1.  **Extract:** Dumps `users`, `images`, `comments`, `flags`, and `image_milestones` tables from Postgres to S3 (MinIO) as raw Parquet files.
2.  **Transform:** Reads raw data, calculates feature vectors (view counts, comment counts, flag counts) at specific decision points (0, 5, 30 mins after upload), and determines the moderation label (flagged within 24h).
3.  **Load:** Writes the processed training data into an Apache Iceberg table (`gourmetgram.moderation.training_data`) stored in MinIO.

## Architecture & Design Decisions

### Dependency Isolation
The `load_iceberg` task runs in a dedicated **virtual environment** using `PythonVirtualenvOperator` to resolve a hard dependency conflict:
*   **Airflow 2.10.5** requires `sqlalchemy<2.0`.
*   **PyIceberg 0.8.0** (needed for SQL Catalog) requires `sqlalchemy>=2.0`.
*   **Pandas** is pinned to `<2.2` to avoid `fsspec` incompatibilities.

### Iceberg Catalog
*   **Type:** SQL (Postgres backed)
*   **Warehouse:** `s3://gourmetgram-datalake/warehouse`
*   **Catalog Name:** `gourmetgram`
*   **Namespace:** `moderation`
*   **Table:** `training_data`

## Prerequisites

*   Docker and Docker Compose
*   Access to the `docker/` directory in the parent repository to run services.

## Setup & Configuration

The environment is configured via `docker-compose.yaml`. Key environment variables for Airflow include:

```yaml
PYICEBERG_CATALOG__GOURMETGRAM__TYPE: sql
PYICEBERG_CATALOG__GOURMETGRAM__URI: postgresql+psycopg2://user:password@postgres:5432/gourmetgram
PYICEBERG_CATALOG__GOURMETGRAM__S3__ENDPOINT: http://minio:9000
PYICEBERG_CATALOG__GOURMETGRAM__WAREHOUSE: s3://gourmetgram-datalake/warehouse
```

## How to Execute

1.  **Start Services:**
    From the `docker/` directory:
    ```bash
    docker-compose up -d
    ```

2.  **Access Airflow UI:**
    Navigate to [http://localhost:8080](http://localhost:8080).
    *   **User:** `admin`
    *   **Password:** `admin`

3.  **Trigger DAG:**
    Enable and trigger the `moderation_training_etl` DAG.

## Verification

To verify that data has been correctly loaded into the Iceberg table, use the provided verification script. This script handles the complex dependency environment automatically.

```bash
docker exec airflow_scheduler python /opt/airflow/dags/verify_pipeline.py
```

**Expected Output:**
```text
Loading catalog 'gourmetgram'...
Table moderation.training_data found.
...
Total rows: 285
Sample Data (first 5 rows):
...
```

## Troubleshooting

*   **Task Failed with `UniqueViolation`:** If multiple runs execute `load_iceberg` simultaneously, Postgres may throw a key violation during table creation. **Solution:** Clear the failed task and ensure only one run is executing at a time.
*   **Dependency Errors:** If modifying `load_iceberg`, remember to update the `requirements` list in `etl_moderation.py`, NOT just the main `requirements.txt`.
