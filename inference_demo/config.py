#!/usr/bin/env python3
"""
Configuration for Real-Time Inference Demo
"""
import os

class Config:
    def __init__(self):
        # Redis Configuration
        self.redis_host = os.getenv("REDIS_HOST", "localhost")
        self.redis_port = int(os.getenv("REDIS_PORT", "6379"))
        self.redis_db = int(os.getenv("REDIS_DB", "0"))

        # PostgreSQL Configuration
        self.database_url = os.getenv(
            "DATABASE_URL",
            "postgresql://user:password@localhost:5432/gourmetgram"
        )

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

        # Feature Names (30 total)
        self.feature_names = [
            # Temporal (4)
            'time_since_upload_seconds',
            'hour_of_day',
            'day_of_week',
            'is_weekend',

            # Totals (3)
            'total_views',
            'total_comments',
            'total_flags',

            # Window Aggregates (6)
            'views_1min',
            'views_5min',
            'views_1hr',
            'comments_1min',
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


# Global config instance
config = Config()
