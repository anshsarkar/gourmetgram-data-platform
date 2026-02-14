#!/usr/bin/env python3
"""
Model Manager - Loads and manages ML model artifacts from MinIO.

The inference service checks MinIO periodically for new model versions.
When a new model is detected (via metadata.json), it downloads and loads
the artifacts (model, scaler, encoder).
"""
import json
import logging
import time
import tempfile
import os

import boto3
import joblib
import numpy as np

from config import config

logger = logging.getLogger(__name__)


class ModelManager:

    def __init__(self):
        self.model = None
        self.scaler = None
        self.encoder = None
        self.model_version = None
        self.model_loaded_at = None
        self._s3_client = None
        self._last_check = 0
        self._last_metadata_hash = None

    def _get_s3_client(self):
        if self._s3_client is None:
            self._s3_client = boto3.client(
                's3',
                endpoint_url=config.s3_endpoint,
                aws_access_key_id=config.s3_access_key,
                aws_secret_access_key=config.s3_secret_key,
            )
        return self._s3_client

    def check_for_new_model(self):
        """Check MinIO for updated model artifacts. Call this periodically."""
        now = time.time()
        if now - self._last_check < config.model_check_interval:
            return False

        self._last_check = now
        s3 = self._get_s3_client()

        try:
            response = s3.get_object(
                Bucket=config.model_bucket,
                Key=f"{config.model_prefix}metadata.json"
            )
            metadata = json.loads(response['Body'].read())
            metadata_hash = metadata.get('updated_at', '')

            if metadata_hash == self._last_metadata_hash:
                return False

            logger.info(f"New model detected! Version: {metadata.get('version')}")
            self._load_model_from_minio(s3, metadata)
            self._last_metadata_hash = metadata_hash
            return True

        except s3.exceptions.NoSuchKey:
            logger.debug("No model found in MinIO yet (expected before training)")
            return False
        except Exception as e:
            logger.warning(f"Error checking for model: {e}")
            return False

    def _load_model_from_minio(self, s3, metadata):
        """Download and load model artifacts from MinIO."""
        artifacts = {
            'model.joblib': 'model',
            'scaler.joblib': 'scaler',
            'encoder.joblib': 'encoder',
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            for filename, attr_name in artifacts.items():
                key = f"{config.model_prefix}{filename}"
                local_path = os.path.join(tmpdir, filename)

                logger.info(f"Downloading {key}...")
                s3.download_file(config.model_bucket, key, local_path)

                obj = joblib.load(local_path)
                setattr(self, attr_name, obj)

        self.model_version = metadata.get('version', 0)
        self.model_loaded_at = time.time()
        logger.info(f"Model v{self.model_version} loaded successfully!")

    # Indices of the 13 numeric features in the 26-dim vector that the
    # training notebook's StandardScaler was fitted on.
    # (is_weekend at index 3 and has_caption at index 12 are treated as
    # categorical in the training notebook, so they are excluded here.)
    NUMERIC_INDICES = [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11, 13, 14]

    # Food-11 categories in order (class_00..class_10) — must match training
    CATEGORY_NAMES = [
        "Bread", "Dairy product", "Dessert", "Egg", "Fried food",
        "Meat", "Noodles/Pasta", "Rice", "Seafood", "Soup", "Vegetable/Fruit"
    ]

    def predict(self, feature_vector: np.ndarray) -> dict:
        """Run model inference on a 26-dim feature vector.

        The feature vector layout (from feature_constructor.py):
          [0:15]  = 15 values (13 numeric + is_weekend@3 + has_caption@12)
          [15:26] = 11 category one-hot flags

        The training notebook splits these as:
          numeric_features  (13) → StandardScaler
          categorical_features (is_weekend, has_caption, category) → OneHotEncoder
        """
        if self.model is None:
            raise RuntimeError("No model loaded")

        # 1. Extract the 13 numeric features the scaler expects
        numeric = feature_vector[self.NUMERIC_INDICES].reshape(1, -1)
        X_scaled = self.scaler.transform(numeric)

        # 2. Reconstruct the 3 categorical values as strings (matching training)
        is_weekend = str(int(feature_vector[3]))     # "0" or "1"
        has_caption = str(int(feature_vector[12]))    # "0" or "1"

        # Decode category from one-hot (indices 15-25)
        cat_onehot = feature_vector[15:26]
        cat_idx = np.argmax(cat_onehot)
        if cat_onehot[cat_idx] == 1 and cat_idx < len(self.CATEGORY_NAMES):
            category = self.CATEGORY_NAMES[cat_idx]
        else:
            category = "Unknown"

        cat_array = np.array([[is_weekend, has_caption, category]])
        X_cat = self.encoder.transform(cat_array)

        # 3. Concatenate scaled numeric + encoded categorical
        X = np.hstack([X_scaled, X_cat])

        prob = self.model.predict_proba(X)[0][1]  # P(needs_moderation)

        if prob < 0.3:
            recommendation = "SAFE"
        elif prob < config.moderation_threshold:
            recommendation = "REVIEW"
        else:
            recommendation = "FLAG"

        return {
            'moderation_probability': float(prob),
            'recommendation': recommendation,
            'model_version': self.model_version,
        }

    @property
    def is_model_available(self):
        return self.model is not None
