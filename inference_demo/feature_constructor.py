#!/usr/bin/env python3
"""
Feature Constructor - Transforms raw features into ML-ready vectors
"""
import logging
from typing import Dict, Any, List
from datetime import datetime
import numpy as np

from config import config

logger = logging.getLogger(__name__)


class FeatureConstructor:
    """Constructs feature vectors from raw features"""

    def __init__(self):
        self.categories = config.food_categories
        self.feature_names = config.feature_names
        logger.info(f"FeatureConstructor initialized ({len(self.feature_names)} features)")

    def construct_feature_vector(self, raw_features: Dict[str, Any]) -> np.ndarray:
        """
        Convert raw features to 31-dimensional feature vector.

        Feature Vector Structure (31 dimensions):
        [0-3]:   Temporal (4)
        [4-6]:   Totals (3)
        [7-12]:  Windows (6)
        [13-15]: Ratios (3)
        [16-17]: Content (2)
        [18-19]: User (2)
        [20-30]: Category One-Hot (11)

        Returns numpy array of shape (31,)
        """
        features = []

        # === Temporal Features (4) ===
        # 1. time_since_upload_seconds
        features.append(raw_features.get('time_since_upload_seconds', 0))

        # 2. hour_of_day (0-23)
        uploaded_at = raw_features.get('uploaded_at')
        if uploaded_at:
            if isinstance(uploaded_at, str):
                uploaded_at = datetime.fromisoformat(uploaded_at.replace('Z', '+00:00'))
            features.append(uploaded_at.hour)
        else:
            features.append(0)

        # 3. day_of_week (0-6, Monday=0)
        if uploaded_at:
            features.append(uploaded_at.weekday())
        else:
            features.append(0)

        # 4. is_weekend (0 or 1)
        if uploaded_at:
            features.append(1 if uploaded_at.weekday() >= 5 else 0)
        else:
            features.append(0)

        # === Totals (3) ===
        # 5-7. total_views, total_comments, total_flags
        features.append(raw_features.get('total_views', 0))
        features.append(raw_features.get('total_comments', 0))
        features.append(raw_features.get('total_flags', 0))

        # === Window Aggregates (6) ===
        # 8-10. views_1min, views_5min, views_1hr
        features.append(raw_features.get('views_1min', 0))
        features.append(raw_features.get('views_5min', 0))
        features.append(raw_features.get('views_1hr', 0))

        # 11-13. comments_1min, comments_5min, comments_1hr
        features.append(raw_features.get('comments_1min', 0))
        features.append(raw_features.get('comments_5min', 0))
        features.append(raw_features.get('comments_1hr', 0))

        # === Engagement Ratios (3) ===
        total_views = raw_features.get('total_views', 0)
        total_comments = raw_features.get('total_comments', 0)
        views_5min = raw_features.get('views_5min', 0)
        views_1min = raw_features.get('views_1min', 0)
        comments_1min = raw_features.get('comments_1min', 0)

        # 14. view_velocity_per_min = views_5min / 5
        features.append(views_5min / 5.0 if views_5min > 0 else 0.0)

        # 15. comment_to_view_ratio = total_comments / max(total_views, 1)
        features.append(total_comments / max(total_views, 1))

        # 16. recent_engagement_score = views_1min + (comments_1min * 5)
        features.append(views_1min + (comments_1min * 5))

        # === Content Features (2) ===
        # 17. caption_length
        features.append(raw_features.get('caption_length', 0))

        # 18. has_caption (0 or 1)
        features.append(raw_features.get('has_caption', 0))

        # === User Features (2) ===
        # 19. user_image_count
        features.append(raw_features.get('user_image_count', 0))

        # 20. user_age_days
        features.append(raw_features.get('user_age_days', 0))

        # === Category One-Hot Encoding (11) ===
        category = raw_features.get('category', 'Unknown')

        # Normalize category name for matching
        category_normalized = category.strip()

        for cat in self.categories:
            features.append(1 if cat == category_normalized else 0)

        # Convert to numpy array
        feature_vector = np.array(features, dtype=np.float32)

        # Validate
        if len(feature_vector) != 31:
            logger.error(f"Feature vector has {len(feature_vector)} dimensions, expected 31")
            raise ValueError(f"Feature vector dimension mismatch: {len(feature_vector)} != 31")

        if np.any(np.isnan(feature_vector)):
            logger.warning("Feature vector contains NaN values")
            # Replace NaN with 0
            feature_vector = np.nan_to_num(feature_vector, nan=0.0)

        if np.any(np.isinf(feature_vector)):
            logger.warning("Feature vector contains inf values")
            # Replace inf with large number
            feature_vector = np.nan_to_num(feature_vector, posinf=1e6, neginf=-1e6)

        return feature_vector

    def get_feature_names(self) -> List[str]:
        """Return ordered list of feature names matching vector positions"""
        return self.feature_names.copy()

    def format_feature_summary(self, feature_vector: np.ndarray, compact: bool = False) -> str:
        """
        Pretty-print feature vector with names and values.

        Args:
            feature_vector: 30-dimensional numpy array
            compact: If True, show only non-zero features

        Returns formatted string
        """
        if len(feature_vector) != len(self.feature_names):
            return f"Error: Feature vector has {len(feature_vector)} dims, expected {len(self.feature_names)}"

        lines = []

        for i, (name, value) in enumerate(zip(self.feature_names, feature_vector)):
            # Skip zero values in compact mode (except for important features)
            if compact and value == 0:
                # Always show these features even if zero
                important_features = ['total_views', 'total_comments', 'total_flags']
                if not any(imp in name for imp in important_features):
                    continue

            # Format value
            if 'category_' in name:
                value_str = "Yes" if value == 1 else "No"
            elif isinstance(value, float):
                value_str = f"{value:.2f}"
            else:
                value_str = f"{int(value)}"

            lines.append(f"  [{i:2d}] {name:30s} = {value_str}")

        return "\n".join(lines)

    def get_feature_dict(self, feature_vector: np.ndarray) -> Dict[str, float]:
        """Convert feature vector to dictionary for easier inspection"""
        if len(feature_vector) != len(self.feature_names):
            raise ValueError(f"Feature vector dimension mismatch")

        return {
            name: float(value)
            for name, value in zip(self.feature_names, feature_vector)
        }
