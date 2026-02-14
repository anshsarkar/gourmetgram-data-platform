#!/usr/bin/env python3
"""
Moderator - Orchestrates the moderation inference pipeline.

This is the key file for the teaching workflow. Students start with
heuristic-based inference and later switch to model-based inference
by changing the USE_MODEL flag below.
"""
import logging
import time
from typing import Dict, Any

from config import config
from feature_fetcher import FeatureFetcher
from feature_constructor import FeatureConstructor
from heuristic_predictor import HeuristicModerationPredictor
from model_manager import ModelManager

logger = logging.getLogger(__name__)


# ============================================================
# INFERENCE MODE TOGGLE
# ============================================================
#
# Students: Change this to True AFTER you have:
#   1. Run the training notebook to train a model
#   2. Verified the model was uploaded to MinIO
#      (check MinIO Console at localhost:9001 -> gourmetgram-datalake/models/)
#
# When False: Uses rule-based heuristic scoring (no model needed)
# When True:  Uses the trained SGDClassifier from MinIO
#
# ============================================================
USE_MODEL = False   # <-- CHANGE TO True TO USE TRAINED MODEL
# ============================================================


class Moderator:

    def __init__(self):
        self.fetcher = FeatureFetcher()
        self.constructor = FeatureConstructor()
        self.heuristic = HeuristicModerationPredictor()
        self.model_manager = ModelManager()

        logger.info(f"Moderator initialized | Mode: {'MODEL' if USE_MODEL else 'HEURISTIC'}")

    def run_inference(self, image_id: str, trigger: str) -> Dict[str, Any]:
        """Run moderation inference for a given image."""
        start = time.time()

        # Step 1: Fetch features from Redis + Postgres
        raw_features = self.fetcher.fetch_all_features(image_id)
        if 'error' in raw_features:
            return {'error': raw_features['error'], 'image_id': image_id}

        # Step 2: Build 26-dim feature vector
        feature_vector = self.constructor.construct_feature_vector(raw_features)

        # Step 3: Choose inference mode
        # ============================================================
        # HEURISTIC MODE (default -- works without a trained model)
        # ============================================================
        if not USE_MODEL:
            prediction = self.heuristic.predict(
                feature_vector, self.constructor.get_feature_names()
            )
            inference_mode = "heuristic"
            model_version = None

        # ============================================================
        # MODEL MODE (change USE_MODEL above to enable)
        # ============================================================
        else:
            # Check for new model versions in MinIO
            self.model_manager.check_for_new_model()

            if self.model_manager.is_model_available:
                prediction = self.model_manager.predict(feature_vector)
                inference_mode = "model"
                model_version = prediction.get('model_version')
            else:
                # Fallback to heuristic if model not yet in MinIO
                logger.warning(
                    "USE_MODEL=True but no model found in MinIO. "
                    "Falling back to heuristic. Train a model first!"
                )
                prediction = self.heuristic.predict(
                    feature_vector, self.constructor.get_feature_names()
                )
                inference_mode = "heuristic_fallback"
                model_version = None

        latency_ms = (time.time() - start) * 1000

        result = {
            'image_id': image_id,
            'inference_mode': inference_mode,
            'model_version': model_version,
            'moderation_probability': prediction['moderation_probability'],
            'recommendation': prediction['recommendation'],
            'reasoning': prediction.get('reasoning', []),
            'trigger': trigger,
            'latency_ms': latency_ms,
        }

        logger.info(
            f"Moderation: image={image_id[:8]}... "
            f"mode={inference_mode} "
            f"prob={prediction['moderation_probability']:.3f} "
            f"rec={prediction['recommendation']} "
            f"latency={latency_ms:.1f}ms"
        )

        return result

    def close(self):
        self.fetcher.close()
