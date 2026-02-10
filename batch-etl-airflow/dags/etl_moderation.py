from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import logging

default_args = {
    'owner': 'gourmetgram',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'moderation_training_etl',
    default_args=default_args,
    description='ETL pipeline for moderation model training data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
)

def extract_data(**kwargs):
    logging.info("Extracting data from Postgres...")
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    import s3fs
    
    # Tables to extract
    tables = ['users', 'images', 'comments', 'flags', 'image_milestones']
    
    # Postgres Hook
    pg_hook = PostgresHook(postgres_conn_id='gourmetgram_postgres')
    engine = pg_hook.get_sqlalchemy_engine()
    
    # MinIO (S3) configuration
    s3_endpoint = 'http://minio:9000'
    s3 = s3fs.S3FileSystem(
        key='admin',
        secret='password',
        client_kwargs={'endpoint_url': s3_endpoint}
    )
    bucket = 'gourmetgram-datalake'
    
    # Create bucket if not exists
    if not s3.exists(bucket):
        s3.mkdir(bucket)
    
    # Extraction Loop
    for table in tables:
        logging.info(f"Extracting table: {table}")
        df = pd.read_sql_table(table, engine)
        
        # Add extraction metadata? No, just raw dump for now.
        
        # Define output path
        # Using a fixed path for simplicity in this batch job
        output_path = f"s3://{bucket}/raw/{table}/latest.parquet"
        
        logging.info(f"Writing {len(df)} rows to {output_path}")
        df.to_parquet(
            output_path,
            index=False,
            storage_options={
                "key": "admin",
                "secret": "password",
                "client_kwargs": {"endpoint_url": s3_endpoint}
            }
        )


def transform_features(**kwargs):
    logging.info("Transforming features...")
    import pandas as pd
    import s3fs
    import numpy as np
    
    s3_endpoint = 'http://minio:9000'
    bucket = 'gourmetgram-datalake'
    storage_options = {
        "key": "admin",
        "secret": "password",
        "client_kwargs": {"endpoint_url": s3_endpoint}
    }
    
    # Load Raw Data
    logging.info("Loading raw data from MinIO...")
    images = pd.read_parquet(f"s3://{bucket}/raw/images/latest.parquet", storage_options=storage_options)
    comments = pd.read_parquet(f"s3://{bucket}/raw/comments/latest.parquet", storage_options=storage_options)
    flags = pd.read_parquet(f"s3://{bucket}/raw/flags/latest.parquet", storage_options=storage_options)
    milestones = pd.read_parquet(f"s3://{bucket}/raw/image_milestones/latest.parquet", storage_options=storage_options)
    
    # Ensure timestamps are datetime
    images['uploaded_at'] = pd.to_datetime(images['uploaded_at'], utc=True)
    comments['created_at'] = pd.to_datetime(comments['created_at'], utc=True)
    flags['created_at'] = pd.to_datetime(flags['created_at'], utc=True)
    milestones['reached_at'] = pd.to_datetime(milestones['reached_at'], utc=True)
    
    # Initialize Feature List
    features_list = []
    
    # Decision Points (minutes after upload)
    decision_points = [0, 5, 30]
    
    logging.info(f"Processing {len(images)} images for decision points {decision_points}...")
    
    for _, img in images.iterrows():
        image_id = img['id']
        upload_time = img['uploaded_at']
        
        # 1. Calculate Label: Flagged within 24h?
        # Check flags for this image
        img_flags = flags[flags['image_id'] == image_id]
        # Filter flags created between upload_time and upload_time + 24h
        flags_24h = img_flags[
            (img_flags['created_at'] >= upload_time) & 
            (img_flags['created_at'] <= upload_time + pd.Timedelta(hours=24))
        ]
        label = 1 if len(flags_24h) > 0 else 0
        
        # 2. Calculate Features at each Decision Point
        for minutes in decision_points:
            cutoff_time = upload_time + pd.Timedelta(minutes=minutes)
            
            # Filter events prior to cutoff
            # Comments
            img_comments = comments[comments['image_id'] == image_id]
            count_comments = len(img_comments[img_comments['created_at'] <= cutoff_time])
            
            # Flags (prior to decision point - rarely happens for 0/5m but possible)
            count_flags = len(img_flags[img_flags['created_at'] <= cutoff_time])
            
            # Views (from milestones)
            # Milestones store accumulated views. We want the max value reached before cutoff.
            img_milestones = milestones[
                (milestones['image_id'] == image_id) & 
                (milestones['milestone_type'] == 'views') & 
                (milestones['reached_at'] <= cutoff_time)
            ]
            if not img_milestones.empty:
                # Max milestone value roughly approximates view count
                # Note: This is lower bound, but best we have from milestones
                view_count = img_milestones['milestone_value'].max()
            else:
                view_count = 0
            
            features_list.append({
                'image_id': str(image_id),
                'decision_point_minutes': minutes,
                'feature_view_count': int(view_count),
                'feature_comment_count': int(count_comments),
                'feature_flag_count': int(count_flags),
                'feature_uploaded_at_hour': upload_time.hour,
                'label_needs_moderation_24h': label
            })
            
    # Create DataFrame
    df_features = pd.DataFrame(features_list)
    
    # Save Processed Data
    output_path = f"s3://{bucket}/processed/training_data.parquet"
    logging.info(f"Saving {len(df_features)} training rows to {output_path}")
    
    df_features.to_parquet(
        output_path,
        index=False,
        storage_options=storage_options
    )


def load_iceberg(**kwargs):
    import logging
    from pyiceberg.catalog import load_catalog
    import pandas as pd
    import pyarrow as pa
    import s3fs
    
    logging.info("Loading into Iceberg...")
    
    # Check if we have data to load
    s3_endpoint = 'http://minio:9000'
    bucket = 'gourmetgram-datalake'
    storage_options = {
        "key": "admin",
        "secret": "password",
        "client_kwargs": {"endpoint_url": s3_endpoint}
    }
    
    input_path = f"s3://{bucket}/processed/training_data.parquet"
    
    try:
        df = pd.read_parquet(input_path, storage_options=storage_options)
    except Exception as e:
        logging.warning(f"No data found at {input_path} or error reading: {e}")
        return

    if df.empty:
        logging.info("No data to load.")
        return

    # Convert to PyArrow Table
    pa_table = pa.Table.from_pandas(df)
    
    # Load Catalog
    # Uses env vars passed via env_vars param in operator
    logging.info("Connecting to catalog...")
    catalog = load_catalog("gourmetgram")
    
    # Namespace and Table
    namespace = "moderation"
    table_name = "training_data"
    identifier = f"{namespace}.{table_name}"
    
    # Create Namespace if not exists
    try:
        catalog.create_namespace(namespace)
        logging.info(f"Namespace '{namespace}' created or exists.")
    except Exception:
        # Ignore if exists
        pass
        
    # Create or Append to Table
    try:
        table = catalog.load_table(identifier)
        logging.info(f"Table {identifier} exists. Appending data...")
        table.append(pa_table)
    except Exception: # NoSuchTableError ideally, but generic catch for now
        logging.info(f"Table {identifier} does not exist. Creating...")
        table = catalog.create_table(
            identifier,
            schema=pa_table.schema
        )
        table.append(pa_table)
        
    logging.info(f"Successfully loaded {len(df)} rows into Iceberg table {identifier}")


t1 = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='transform_features',
    python_callable=transform_features,
    dag=dag,
)

from airflow.operators.python import PythonVirtualenvOperator
import os

# Capture necessary environment variables for PyIceberg
iceberg_env_vars = {
    key: os.environ.get(key)
    for key in os.environ
    if key.startswith("PYICEBERG_CATALOG__")
}

t3 = PythonVirtualenvOperator(
    task_id='load_iceberg',
    python_callable=load_iceberg,
    requirements=[
        "pyiceberg[s3fs,sql-postgres]==0.8.0",
        "pandas<2.2",
        "pyarrow",
        "sqlalchemy>=2.0"
    ],
    system_site_packages=False,
    env_vars=iceberg_env_vars,
    dag=dag,
)

t1 >> t2 >> t3
