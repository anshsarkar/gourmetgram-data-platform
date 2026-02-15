import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import pyarrow.parquet as pq
import time

# Configuration
NUM_ROWS = 6_000_000  # 6 Million Rows ~ 1.2GB in memory
BATCH_SIZE = 500_000  # Write in smaller chunks so each Parquet file has manageable row groups
ROW_GROUP_SIZE = 50_000  # Rows per Parquet row group — controls streaming batch size
TABLE_NAME = "moderation.large_training_data"

def generate_batch(size):
    data = {
        'time_since_upload_seconds': np.random.randint(0, 100000, size),
        'hour_of_day': np.random.randint(0, 24, size),
        'day_of_week': np.random.randint(0, 7, size),
        'views_5min': np.random.randint(0, 1000, size),
        'views_1hr': np.random.randint(0, 10000, size),
        'comments_5min': np.random.randint(0, 100, size),
        'comments_1hr': np.random.randint(0, 1000, size),
        'view_velocity_per_min': np.random.uniform(0, 100, size),
        'comment_to_view_ratio': np.random.uniform(0, 1, size),
        'recent_engagement_score': np.random.uniform(0, 10, size),
        'caption_length': np.random.randint(0, 500, size),
        'user_image_count': np.random.randint(0, 1000, size),
        'user_age_days': np.random.randint(0, 3650, size),
        'is_weekend': np.random.choice([True, False], size),
        'has_caption': np.random.choice([True, False], size),
        'category': np.random.choice(['food', 'travel', 'lifestyle', 'tech', 'art'], size),
        'label_needs_moderation_24h': np.random.randint(0, 2, size) # Target
    }
    return pd.DataFrame(data)

def main():
    print(f"Generating {NUM_ROWS} rows of synthetic data...")
    try:
        catalog = load_catalog("gourmetgram")
        
        # Check if table exists, if so drop it to start fresh
        try:
            catalog.drop_table(TABLE_NAME)
            print(f"Dropped existing table {TABLE_NAME}")
        except:
            pass
            
        # Create schema from first batch
        df_sample = generate_batch(10)
        pa_schema = pa.Schema.from_pandas(df_sample)
        
        print(f"Creating table {TABLE_NAME}...")
        table = catalog.create_table(
            TABLE_NAME, 
            schema=pa_schema
        )
        
        # Generate and Append Data
        start_time = time.time()
        for i in range(0, NUM_ROWS, BATCH_SIZE):
            print(f"Processing batch {i // BATCH_SIZE + 1} / {NUM_ROWS // BATCH_SIZE}...")
            df_batch = generate_batch(BATCH_SIZE)
            arrow_table = pa.Table.from_pandas(df_batch)
            # Split into smaller PyArrow tables so each Parquet file has one manageable row group
            # This ensures to_arrow_batch_reader() yields ~ROW_GROUP_SIZE rows per batch
            for offset in range(0, len(arrow_table), ROW_GROUP_SIZE):
                table.append(arrow_table.slice(offset, ROW_GROUP_SIZE))
            
        print(f"✅ Success! Generated {NUM_ROWS} rows in {time.time() - start_time:.2f} seconds.")
        print(f"Table {TABLE_NAME} is ready for OOM testing.")

    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    main()
