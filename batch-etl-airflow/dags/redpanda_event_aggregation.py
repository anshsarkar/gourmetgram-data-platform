from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
import logging

# DAG default arguments
default_args = {
    'owner': 'gourmetgram',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'redpanda_event_aggregation',
    default_args=default_args,
    description='Aggregate Redpanda events into 5-minute windows for training',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'redpanda', 'iceberg', 'windowing'],
    # CRITICAL FIX: This allows XComs to be passed as real lists, not strings
    render_template_as_native_obj=True
)

def consume_and_aggregate_events(**kwargs):
    import json
    from kafka import KafkaConsumer
    from collections import defaultdict
    from datetime import datetime, timezone, timedelta

    logging.info("=== Starting Redpanda Event Aggregation ===")

    # Calculate time range: last hour
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=1)

    logging.info(f"Processing events from {start_time} to {end_time}")

    # Initialize Kafka consumer
    bootstrap_servers = 'redpanda:9092'
    topics = ['gourmetgram.views', 'gourmetgram.comments', 'gourmetgram.flags']

    consumer = KafkaConsumer(
        *topics,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=30000,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        group_id='airflow_event_aggregator'
    )

    # Get partitions and seek
    partitions = consumer.assignment()
    if not partitions:
        consumer.poll(timeout_ms=1000)
        partitions = consumer.assignment()

    start_timestamp_ms = int(start_time.timestamp() * 1000)
    offset_dict = consumer.offsets_for_times({tp: start_timestamp_ms for tp in partitions})

    for tp, offset_and_timestamp in offset_dict.items():
        if offset_and_timestamp:
            consumer.seek(tp, offset_and_timestamp.offset)

    # Aggregation buckets
    view_windows = defaultdict(lambda: defaultdict(int))
    comment_windows = defaultdict(lambda: defaultdict(int))
    flag_windows = defaultdict(lambda: defaultdict(int))

    for message in consumer:
        try:
            event = message.value
            topic = message.topic
            
            if topic == 'gourmetgram.views':
                ts_str = event.get('viewed_at')
                img_id = event.get('image_id')
            elif topic == 'gourmetgram.comments':
                ts_str = event.get('created_at')
                img_id = event.get('image_id')
            elif topic == 'gourmetgram.flags':
                ts_str = event.get('created_at')
                img_id = event.get('image_id')
                if not img_id: continue
            else:
                continue

            # Parse timestamp
            if isinstance(ts_str, str):
                if ts_str.endswith('Z'):
                    timestamp = datetime.fromisoformat(ts_str.replace('Z', '+00:00'))
                else:
                    timestamp = datetime.fromisoformat(ts_str)
                    if timestamp.tzinfo is None:
                        timestamp = timestamp.replace(tzinfo=timezone.utc)
            else:
                timestamp = ts_str

            if timestamp < start_time: continue
            if timestamp >= end_time: break

            # Bucket (5 mins)
            bucket_start = timestamp.replace(minute=(timestamp.minute // 5) * 5, second=0, microsecond=0)

            if topic == 'gourmetgram.views':
                view_windows[str(img_id)][bucket_start] += 1
            elif topic == 'gourmetgram.comments':
                comment_windows[str(img_id)][bucket_start] += 1
            elif topic == 'gourmetgram.flags':
                flag_windows[str(img_id)][bucket_start] += 1

        except Exception as e:
            logging.error(f"Error: {e}")
            continue

    consumer.close()

    # Helper to format records
    def to_records(windows_dict):
        rows = []
        for img_id, buckets in windows_dict.items():
            for bucket_start, count in buckets.items():
                rows.append({
                    'image_id': img_id,
                    'window_start': bucket_start.isoformat(),
                    'window_end': (bucket_start + timedelta(minutes=5)).isoformat(),
                    'event_count': count,
                    'processed_at': datetime.now(timezone.utc).isoformat()
                })
        return rows

    # Push to XCom using the Context (ti) which IS allowed in standard PythonOperator
    ti = kwargs['ti']
    ti.xcom_push(key='view_windows', value=to_records(view_windows))
    ti.xcom_push(key='comment_windows', value=to_records(comment_windows))
    ti.xcom_push(key='flag_windows', value=to_records(flag_windows))


# REFACTORED: Single reusable function that takes DATA as arguments
# No 'kwargs' or 'ti' here!
def write_to_iceberg_task(table_name, records):
    import logging
    import pandas as pd
    import pyarrow as pa
    from pyiceberg.catalog import load_catalog

    logging.info(f"=== Writing {table_name} to Iceberg ===")

    if not records:
        logging.info("No records to write.")
        return

    logging.info(f"Received {len(records)} records.")

    # Convert to DataFrame
    df = pd.DataFrame(records)
    df['window_start'] = pd.to_datetime(df['window_start'], utc=True)
    df['window_end'] = pd.to_datetime(df['window_end'], utc=True)
    df['processed_at'] = pd.to_datetime(df['processed_at'], utc=True)
    
    # Convert to PyArrow
    pa_table = pa.Table.from_pandas(df)

    # Load Catalog
    catalog = load_catalog("gourmetgram")
    namespace = "event_aggregations"
    identifier = f"{namespace}.{table_name}"

    try:
        catalog.create_namespace(namespace)
    except Exception:
        pass

    try:
        table = catalog.load_table(identifier)
        table.append(pa_table)
        logging.info(f"Appended to {identifier}")
    except Exception:
        logging.info(f"Creating new table {identifier}")
        schema = pa.schema([
            pa.field('image_id', pa.string()),
            pa.field('window_start', pa.timestamp('us', tz='UTC')),
            pa.field('window_end', pa.timestamp('us', tz='UTC')),
            pa.field('event_count', pa.int64()),
            pa.field('processed_at', pa.timestamp('us', tz='UTC'))
        ])
        table = catalog.create_table(identifier, schema=schema)
        table.append(pa_table)

# Define Tasks
t1_consume = PythonOperator(
    task_id='consume_and_aggregate_events',
    python_callable=consume_and_aggregate_events,
    dag=dag,
)

# Common requirements for virtualenv tasks
iceberg_reqs = ['pyiceberg[s3fs,sql-postgres]==0.8.0', 'pandas', 'pyarrow']

t2_write_views = PythonVirtualenvOperator(
    task_id='write_view_windows',
    python_callable=write_to_iceberg_task,
    requirements=iceberg_reqs,
    system_site_packages=True,
    op_kwargs={
        'table_name': 'view_windows_5min',
        # Jinja Template to fetch XCom data BEFORE the task starts
        'records': "{{ ti.xcom_pull(task_ids='consume_and_aggregate_events', key='view_windows') }}"
    },
    dag=dag,
)

t3_write_comments = PythonVirtualenvOperator(
    task_id='write_comment_windows',
    python_callable=write_to_iceberg_task,
    requirements=iceberg_reqs,
    system_site_packages=True,
    op_kwargs={
        'table_name': 'comment_windows_5min',
        'records': "{{ ti.xcom_pull(task_ids='consume_and_aggregate_events', key='comment_windows') }}"
    },
    dag=dag,
)

t4_write_flags = PythonVirtualenvOperator(
    task_id='write_flag_windows',
    python_callable=write_to_iceberg_task,
    requirements=iceberg_reqs,
    system_site_packages=True,
    op_kwargs={
        'table_name': 'flag_windows_5min',
        'records': "{{ ti.xcom_pull(task_ids='consume_and_aggregate_events', key='flag_windows') }}"
    },
    dag=dag,
)

t1_consume >> [t2_write_views, t3_write_comments, t4_write_flags]