#!/usr/bin/env python3
"""
Real-Time Inference Demo - End-to-end ML inference pipeline demonstration
"""
import sys
import argparse
import logging
import time
import csv
from pathlib import Path
from datetime import datetime
from typing import Optional, Dict, Any
import psycopg2
from tabulate import tabulate

from config import config
from feature_fetcher import FeatureFetcher
from feature_constructor import FeatureConstructor
from heuristic_predictor import HeuristicModerationPredictor


def setup_logging(verbose: bool = False):
    """Configure logging"""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )


def select_active_image(db_url: str) -> Optional[str]:
    """
    Query Postgres for an image with recent activity.

    Returns image_id or None if no images found.
    """
    try:
        conn = psycopg2.connect(db_url)
        cursor = conn.cursor()

        query = """
            SELECT id FROM images
            WHERE views > 0
            ORDER BY uploaded_at DESC
            LIMIT 1
        """

        cursor.execute(query)
        result = cursor.fetchone()

        cursor.close()
        conn.close()

        if result:
            return str(result[0])
        else:
            return None

    except psycopg2.Error as e:
        print(f"❌ Database error: {e}")
        return None


def run_inference(image_id: str, fetcher: FeatureFetcher, constructor: FeatureConstructor, predictor: HeuristicModerationPredictor) -> Dict[str, Any]:
    """
    Complete inference pipeline:
    1. Fetch features (Redis + Postgres)
    2. Construct feature vector
    3. Make prediction
    4. Measure total latency

    Returns dict with all results and timing.
    """
    start_time = time.time()

    # Step 1: Fetch features
    raw_features = fetcher.fetch_all_features(image_id)

    if 'error' in raw_features:
        return {'error': raw_features['error']}

    # Step 2: Construct feature vector
    feature_construction_start = time.time()
    feature_vector = constructor.construct_feature_vector(raw_features)
    feature_construction_ms = (time.time() - feature_construction_start) * 1000

    # Step 3: Make prediction
    prediction_start = time.time()
    prediction = predictor.predict(feature_vector, constructor.get_feature_names())
    prediction_ms = (time.time() - prediction_start) * 1000

    # Total latency
    total_latency_ms = (time.time() - start_time) * 1000

    return {
        'image_id': image_id,
        'raw_features': raw_features,
        'feature_vector': feature_vector,
        'prediction': prediction,
        'latency': {
            'redis_ms': raw_features.get('redis_latency_ms', 0),
            'postgres_ms': raw_features.get('postgres_latency_ms', 0),
            'feature_construction_ms': feature_construction_ms,
            'prediction_ms': prediction_ms,
            'total_ms': total_latency_ms
        }
    }


def display_results(results: Dict[str, Any], constructor: FeatureConstructor, show_features: bool = False):
    """
    Pretty-print inference results.

    Shows:
    - Image ID
    - Latency breakdown
    - Feature vector (if requested)
    - Prediction + reasoning
    """
    image_id = results['image_id']
    latency = results['latency']
    prediction = results['prediction']
    feature_vector = results['feature_vector']

    print("\n" + "=" * 70)
    print(f"  Real-Time Inference Demo - Image {image_id[:16]}...")
    print("=" * 70)

    # Latency breakdown
    print("\nFeature Retrieval:")
    print(f"   Redis fetch:            {latency['redis_ms']:6.2f} ms")
    print(f"   Postgres fetch:         {latency['postgres_ms']:6.2f} ms")
    print(f"   Feature construction:   {latency['feature_construction_ms']:6.2f} ms")
    print(f"   Prediction:             {latency['prediction_ms']:6.2f} ms")
    print(f"   ─────────────────────────────────────")
    print(f"   Total:                  {latency['total_ms']:6.2f} ms", end="")

    # Latency check
    if latency['total_ms'] < 50:
        print(" [OK] (target: <50ms)")
    else:
        print(" [WARNING] (target: <50ms)")

    # Feature vector
    if show_features:
        print("\nFeature Vector (31 dimensions):")
        print(constructor.format_feature_summary(feature_vector, compact=False))

    else:
        # Show compact summary
        print("\nKey Features:")
        feature_dict = constructor.get_feature_dict(feature_vector)

        # Select interesting features to display
        interesting = [
            ('total_views', 'Total Views'),
            ('total_comments', 'Total Comments'),
            ('total_flags', 'Total Flags'),
            ('views_5min', 'Views (5min)'),
            ('comments_5min', 'Comments (5min)'),
            ('view_velocity_per_min', 'View Velocity'),
            ('comment_to_view_ratio', 'Comment Ratio'),
        ]

        for feat_name, display_name in interesting:
            value = feature_dict.get(feat_name, 0)
            print(f"   {display_name:20s}: {value:>8.2f}")

        # Show active category
        for i, name in enumerate(constructor.get_feature_names()):
            if 'category_' in name and feature_vector[i] == 1:
                category = name.replace('category_', '')
                print(f"   {'Category':20s}: {category}")
                break

    # Prediction
    print("\nPrediction:")
    prob = prediction['moderation_probability']
    rec = prediction['recommendation']

    # Format recommendation
    if rec == "SAFE":
        rec_display = f"[OK] {rec}"
    elif rec == "REVIEW":
        rec_display = f"[WARN] {rec}"
    else:
        rec_display = f"[FLAG] {rec}"

    print(f"   Moderation Probability: {prob:.3f}")
    print(f"   Recommendation: {rec_display}")

    print("\n   Reasoning:")
    for reason in prediction['reasoning']:
        print(f"      {reason}")

    print("\n   Risk Factor Contributions:")
    for factor, contribution in sorted(prediction['risk_factors'].items(), key=lambda x: abs(x[1]), reverse=True):
        sign = "+" if contribution >= 0 else ""
        print(f"      {factor:25s}: {sign}{contribution:+.2f}")

    print("\n" + "=" * 70)


def save_features_to_csv(results: Dict[str, Any], constructor: FeatureConstructor, csv_path: str):
    # Prepare row data
    feature_dict = constructor.get_feature_dict(results['feature_vector'])
    prediction = results['prediction']

    row = {
        'timestamp': datetime.now().isoformat(),
        'image_id': results['image_id'],
        # Add all 31 features
        **feature_dict,
        # Add prediction results
        'moderation_probability': prediction['moderation_probability'],
        'recommendation': prediction['recommendation'],
        # Add latency metrics
        'total_latency_ms': results['latency']['total_ms'],
        'redis_latency_ms': results['latency']['redis_ms'],
        'postgres_latency_ms': results['latency']['postgres_ms']
    }

    # Check if file exists to determine if we need headers
    file_exists = Path(csv_path).exists()

    # Write to CSV
    with open(csv_path, 'a', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=row.keys())

        # Write header if new file
        if not file_exists:
            writer.writeheader()

        writer.writerow(row)

    print(f"\nFeatures saved to: {csv_path}")


def run_latency_benchmark(n_runs: int, image_id: str, fetcher: FeatureFetcher, constructor: FeatureConstructor, predictor: HeuristicModerationPredictor):
    print(f"\nRunning latency benchmark ({n_runs} iterations)...")

    latencies = []

    for i in range(n_runs):
        start = time.time()
        results = run_inference(image_id, fetcher, constructor, predictor)
        latency_ms = (time.time() - start) * 1000
        latencies.append(latency_ms)

        if (i + 1) % 10 == 0:
            print(f"   Progress: {i+1}/{n_runs}", end='\r')

    print()  # New line after progress

    # Calculate statistics
    import numpy as np
    latencies = np.array(latencies)

    print("\nLatency Statistics:")
    print(f"   Mean:   {np.mean(latencies):.2f} ms")
    print(f"   Median: {np.median(latencies):.2f} ms")
    print(f"   Min:    {np.min(latencies):.2f} ms")
    print(f"   Max:    {np.max(latencies):.2f} ms")
    print(f"   P50:    {np.percentile(latencies, 50):.2f} ms")
    print(f"   P95:    {np.percentile(latencies, 95):.2f} ms")
    print(f"   P99:    {np.percentile(latencies, 99):.2f} ms")

    # Check against target
    p99 = np.percentile(latencies, 99)
    if p99 < 50:
        print(f"\n   [OK] P99 latency ({p99:.2f}ms) meets target (<50ms)")
    else:
        print(f"\n   [WARNING] P99 latency ({p99:.2f}ms) exceeds target (<50ms)")


def main():
    parser = argparse.ArgumentParser(
        description="Real-Time Inference Demo for GourmetGram Moderation",
        formatter_class=argparse.RawDescriptionHelpFormatter
    )

    parser.add_argument(
        '--image-id',
        type=str,
        help="Specific image ID to run inference on (default: select random active image)"
    )

    parser.add_argument(
        '--show-features',
        action='store_true',
        help="Display full feature vector (all 30 features)"
    )

    parser.add_argument(
        '--latency-test',
        type=int,
        metavar='N',
        help="Run latency benchmark with N iterations"
    )

    parser.add_argument(
        '--verbose',
        action='store_true',
        help="Enable debug logging"
    )

    parser.add_argument(
        '--save-csv',
        type=str,
        metavar='PATH',
        default='inference_features.csv',
        help="Path to CSV file for saving features (default: inference_features.csv)"
    )

    args = parser.parse_args()

    # Setup logging
    setup_logging(args.verbose)

    print("=" * 70)
    print("  GourmetGram Real-Time Inference Demo")
    print("=" * 70)
    print("\nPhase 2: Demonstrating end-to-end ML inference pipeline")
    print("  ✓ Feature retrieval from Redis (sorted sets)")
    print("  ✓ Metadata fetching from Postgres")
    print("  ✓ Feature engineering (31-dimensional vector)")
    print("  ✓ Heuristic-based moderation prediction")

    # Initialize components
    print("\nInitializing components...")
    try:
        fetcher = FeatureFetcher()
        constructor = FeatureConstructor()
        predictor = HeuristicModerationPredictor()
        print("   [OK] All components initialized")
    except Exception as e:
        print(f"   [ERROR] Initialization failed: {e}")
        sys.exit(1)

    # Get image ID
    if args.image_id:
        image_id = args.image_id
        print(f"\nUsing specified image: {image_id}")
    else:
        print("\nSelecting active image from database...")
        image_id = select_active_image(config.database_url)

        if not image_id:
            print("   [ERROR] No images found with activity. Please run the streaming test first.")
            print("\n   Hint: ./tests/run_test.sh")
            fetcher.close()
            sys.exit(1)

        print(f"   [OK] Selected image: {image_id}")

    # Run inference
    if args.latency_test:
        # Latency benchmark mode
        run_latency_benchmark(args.latency_test, image_id, fetcher, constructor, predictor)

    else:
        # Single inference mode
        print("\nRunning inference pipeline...")
        results = run_inference(image_id, fetcher, constructor, predictor)

        if 'error' in results:
            print(f"\n[ERROR] Inference failed: {results['error']}")
            fetcher.close()
            sys.exit(1)

        # Display results
        display_results(results, constructor, show_features=args.show_features)

        # Save to CSV
        save_features_to_csv(results, constructor, args.save_csv)

    # Cleanup
    fetcher.close()
    print("\n[OK] Inference demo complete!\n")


if __name__ == "__main__":
    main()
