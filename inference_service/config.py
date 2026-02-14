#!/usr/bin/env python3
import os


class Config:
    def __init__(self):
        # Kafka Configuration
        self.kafka_bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
        self.kafka_consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "gourmetgram-moderation")
        self.moderation_topic = os.getenv("MODERATION_TOPIC", "gourmetgram.moderation_requests")

        # Redis Configuration
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))

        # PostgreSQL Configuration
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@localhost:5432/gourmetgram"
        )

        # MinIO / S3 Configuration
        self.s3_endpoint = os.getenv("S3_ENDPOINT_URL", "http://minio:9000")
        self.s3_access_key = os.getenv("AWS_ACCESS_KEY_ID", "admin")
        self.s3_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "password")
        self.model_bucket = os.getenv("MODEL_BUCKET", "gourmetgram-datalake")
        self.model_prefix = os.getenv("MODEL_PREFIX", "models/moderation/")

        # Model check interval (seconds)
        self.model_check_interval = int(os.getenv("MODEL_CHECK_INTERVAL", "30"))

        # Prediction Configuration
        self.moderation_threshold = float(os.getenv("MODERATION_THRESHOLD", "0.7"))

        # Window Sizes (in seconds)
        self.window_sizes = {
            '1min': 60,
            '5min': 300,
            '1hr': 3600
        }

        # Food-11 Dataset Categories (in order: class_00 to class_10)
        self.food_categories = [
            "Bread",           # class_00
            "Dairy product",   # class_01
            "Dessert",         # class_02
            "Egg",             # class_03
            "Fried food",      # class_04
            "Meat",            # class_05
            "Noodles/Pasta",   # class_06
            "Rice",            # class_07
            "Seafood",         # class_08
            "Soup",            # class_09
            "Vegetable/Fruit"  # class_10
        ]

        # Feature Names (26 total - no 1-min windows, no redundant totals)
        self.feature_names = [
            # Temporal (4)
            'time_since_upload_seconds',
            'hour_of_day',
            'day_of_week',
            'is_weekend',

            # Window Aggregates (4 - no 1-min)
            'views_5min',
            'views_1hr',
            'comments_5min',
            'comments_1hr',

            # Engagement Ratios (3)
            'view_velocity_per_min',
            'comment_to_view_ratio',
            'recent_engagement_score',

            # Content Features (2)
            'caption_length',
            'has_caption',

            # User Features (2)
            'user_image_count',
            'user_age_days',

            # Category One-Hot (11)
            'category_Bread',
            'category_Dairy',
            'category_Dessert',
            'category_Egg',
            'category_Fried',
            'category_Meat',
            'category_Noodles',
            'category_Rice',
            'category_Seafood',
            'category_Soup',
            'category_Veggie'
        ]

        # Logging Configuration
        self.log_level = os.getenv("LOG_LEVEL", "INFO")


# Global config instance
config = Config()
