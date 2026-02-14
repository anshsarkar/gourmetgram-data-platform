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

    def predict(self, feature_vector: np.ndarray) -> dict:
        """Run model inference on a 26-dim feature vector.

        The feature vector layout:
          [0:15]  = numeric features (scaled by StandardScaler)
          [15:26] = category one-hot (already binary, no scaling needed)
        """
        if self.model is None:
            raise RuntimeError("No model loaded")

        numeric = feature_vector[:15].reshape(1, -1)
        category_onehot = feature_vector[15:].reshape(1, -1)

        # Scale numeric features using the training scaler
        X_scaled = self.scaler.transform(numeric)

        # Concatenate: scaled numeric + raw one-hot categories
        # Note: The training notebook's OneHotEncoder produced similar binary columns.
        # Since feature_constructor.py already one-hot encodes categories in the same
        # order as training, we use them directly instead of re-encoding.
        X = np.hstack([X_scaled, category_onehot])

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
