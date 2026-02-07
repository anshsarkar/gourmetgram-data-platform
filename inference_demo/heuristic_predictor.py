#!/usr/bin/env python3
"""
Heuristic Moderation Predictor - Rule-based predictions until real model is trained
"""
import logging
from typing import Dict, Any, List
import numpy as np

from config import config

logger = logging.getLogger(__name__)


class HeuristicModerationPredictor:
    """
    Simple rule-based predictor using domain knowledge.

    Provides interpretable baseline for moderation predictions.
    """

    def __init__(self, threshold: float = None):
        self.threshold = threshold or config.moderation_threshold
        logger.info(f"HeuristicPredictor initialized (threshold: {self.threshold})")

    def predict(self, feature_vector: np.ndarray, feature_names: List[str]) -> Dict[str, Any]:
        """
        Make moderation prediction using rule-based scoring.

        Risk Signals:
        - HIGH RISK:
          - total_flags > 0 → +0.4 (reported content)
          - comments_1min > 10 → +0.3 (suspicious spam)
          - comment_to_view_ratio > 0.5 → +0.2 (unusual engagement)
          - total_comments > 50 AND total_views < 100 → +0.3 (bot-like)

        - MEDIUM RISK:
          - view_velocity > 20/min → +0.2 (viral, needs review)
          - recent_engagement_score > 100 → +0.15 (high activity)

        - LOW RISK:
          - user_image_count > 10 → -0.1 (established user)
          - total_flags == 0 AND total_comments < 5 → baseline 0.1

        Returns:
        {
            'moderation_probability': float (0-1),
            'recommendation': str ('SAFE', 'REVIEW', 'FLAG'),
            'reasoning': List[str] (which rules triggered),
            'risk_factors': Dict[str, float] (individual contributions)
        }
        """
        # Convert feature vector to dict for easier access
        features = {name: float(value) for name, value in zip(feature_names, feature_vector)}

        # Initialize score
        base_score = 0.3
        score = base_score
        reasoning = []
        risk_factors = {'base': base_score}

        # === HIGH RISK SIGNALS ===

        # Flags reported
        if features.get('total_flags', 0) > 0:
            contribution = 0.4
            score += contribution
            risk_factors['flags_reported'] = contribution
            reasoning.append(f"⚠️  Content has been flagged {int(features['total_flags'])} time(s)")

        # Suspicious comment spam
        comments_1min = features.get('comments_1min', 0)
        if comments_1min > 10:
            contribution = 0.3
            score += contribution
            risk_factors['comment_spam'] = contribution
            reasoning.append(f"⚠️  Unusual comment activity ({int(comments_1min)} comments in 1 minute)")

        # Unusual engagement pattern
        comment_ratio = features.get('comment_to_view_ratio', 0)
        if comment_ratio > 0.5:
            contribution = 0.2
            score += contribution
            risk_factors['unusual_engagement'] = contribution
            reasoning.append(f"⚠️  High comment-to-view ratio ({comment_ratio:.2f})")

        # Bot-like behavior
        total_comments = features.get('total_comments', 0)
        total_views = features.get('total_views', 0)
        if total_comments > 50 and total_views < 100:
            contribution = 0.3
            score += contribution
            risk_factors['bot_pattern'] = contribution
            reasoning.append(f"⚠️  Suspicious pattern: {int(total_comments)} comments but only {int(total_views)} views")

        # === MEDIUM RISK SIGNALS ===

        # Viral content (needs human review)
        view_velocity = features.get('view_velocity_per_min', 0)
        if view_velocity > 20:
            contribution = 0.2
            score += contribution
            risk_factors['viral_content'] = contribution
            reasoning.append(f"📈 Viral content ({view_velocity:.1f} views/min) - may need review")

        # High recent engagement
        recent_engagement = features.get('recent_engagement_score', 0)
        if recent_engagement > 100:
            contribution = 0.15
            score += contribution
            risk_factors['high_activity'] = contribution
            reasoning.append(f"📊 High recent activity (score: {int(recent_engagement)})")

        # === LOW RISK SIGNALS (reduce score) ===

        # Established user
        user_image_count = features.get('user_image_count', 0)
        if user_image_count > 10:
            contribution = -0.1
            score += contribution
            risk_factors['established_user'] = contribution
            reasoning.append(f"✅ Established user ({int(user_image_count)} images)")

        # Low activity, no flags
        if features.get('total_flags', 0) == 0 and total_comments < 5:
            if not reasoning:  # Only if no other signals
                reasoning.append("✅ Normal activity, no flags")

        # Clamp score to [0, 1]
        score = max(0.0, min(1.0, score))

        # Determine recommendation
        if score < 0.3:
            recommendation = "SAFE"
        elif score < self.threshold:
            recommendation = "REVIEW"
        else:
            recommendation = "FLAG"

        # Add default reasoning if empty
        if not reasoning:
            reasoning.append("No significant risk signals detected")

        result = {
            'moderation_probability': score,
            'recommendation': recommendation,
            'reasoning': reasoning,
            'risk_factors': risk_factors
        }

        logger.debug(f"Prediction: {score:.2f} ({recommendation}) - {len(reasoning)} factors")

        return result

    def explain(self, prediction: Dict[str, Any]) -> str:
        """
        Generate human-readable explanation of prediction.

        Returns formatted string with probability, recommendation, and reasoning.
        """
        lines = []

        # Header
        prob = prediction['moderation_probability']
        rec = prediction['recommendation']

        lines.append("=" * 70)
        lines.append("MODERATION PREDICTION")
        lines.append("=" * 70)
        lines.append(f"Probability: {prob:.3f}")
        lines.append(f"Recommendation: {rec}")
        lines.append("")

        # Reasoning
        lines.append("Reasoning:")
        for reason in prediction['reasoning']:
            lines.append(f"  {reason}")

        lines.append("")

        # Risk factors breakdown
        lines.append("Risk Factor Contributions:")
        risk_factors = prediction['risk_factors']

        for factor, contribution in sorted(risk_factors.items(), key=lambda x: abs(x[1]), reverse=True):
            sign = "+" if contribution >= 0 else ""
            lines.append(f"  {factor:25s}: {sign}{contribution:+.2f}")

        lines.append("")
        lines.append(f"Final Score: {prob:.3f}")
        lines.append("=" * 70)

        return "\n".join(lines)
