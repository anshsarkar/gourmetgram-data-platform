import pandas as pd
import numpy as np
from pyiceberg.catalog import load_catalog
import pyarrow as pa
import time

# Configuration
NUM_ROWS = 6_000_000  # 6 Million Rows ~ 1.2GB in memory
BATCH_SIZE = 1_000_000 # Process in chunks to avoid OOM during generation
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
            table.append(pa.Table.from_pandas(df_batch))
            
        print(f"✅ Success! Generated {NUM_ROWS} rows in {time.time() - start_time:.2f} seconds.")
        print(f"Table {TABLE_NAME} is ready for OOM testing.")

    except Exception as e:
        print(f"Failed: {e}")

if __name__ == "__main__":
    main()
