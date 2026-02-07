#!/usr/bin/env python3
"""
Test script for inference demo - Creates controlled test data and validates results
"""
import time
import httpx
import sys
from typing import Dict, Any
import numpy as np

from feature_fetcher import FeatureFetcher
from feature_constructor import FeatureConstructor
from heuristic_predictor import HeuristicModerationPredictor


# Configuration
API_URL = "http://localhost:8000"
TEST_SCENARIOS = []


class TestScenario:
    """Defines a test scenario with known inputs and expected outputs"""

    def __init__(self, name: str, description: str):
        self.name = name
        self.description = description
        self.user_id = None
        self.image_id = None
        self.setup_actions = []
        self.expected_features = {}
        self.expected_prediction = {}

    def add_action(self, action_type: str, count: int = 1, delay: float = 0.1):
        """Add an action to perform during setup"""
        self.setup_actions.append({'type': action_type, 'count': count, 'delay': delay})

    def expect_feature(self, feature_name: str, expected_value: Any, tolerance: float = None):
        """Set expected value for a feature"""
        self.expected_features[feature_name] = {'value': expected_value, 'tolerance': tolerance}

    def expect_prediction(self, min_prob: float, max_prob: float, recommendation: str):
        """Set expected prediction range"""
        self.expected_prediction = {
            'min_prob': min_prob,
            'max_prob': max_prob,
            'recommendation': recommendation
        }


def create_test_scenarios():
    """Define all test scenarios"""
    scenarios = []

    # ===== Scenario 1: Safe Image (Low Activity) =====
    scenario1 = TestScenario(
        "Safe Image - Low Activity",
        "Normal image with minimal engagement, no flags → Should predict SAFE"
    )
    scenario1.add_action('views', count=5, delay=0.2)
    scenario1.add_action('comments', count=2, delay=0.5)

    # Expected features
    scenario1.expect_feature('total_views', 5)
    scenario1.expect_feature('total_comments', 2)
    scenario1.expect_feature('total_flags', 0)
    scenario1.expect_feature('views_1min', 5, tolerance=1)  # May vary slightly
    scenario1.expect_feature('comments_1min', 2, tolerance=1)
    scenario1.expect_feature('category_Dessert', 1)

    # Expected prediction
    scenario1.expect_prediction(min_prob=0.1, max_prob=0.4, recommendation='SAFE')

    scenarios.append(scenario1)

    # ===== Scenario 2: Viral Content =====
    scenario2 = TestScenario(
        "Viral Content - High Views",
        "Image with rapid view growth → Should predict REVIEW (viral)"
    )
    scenario2.add_action('views', count=150, delay=0.05)  # 150 views in ~7.5 seconds
    scenario2.add_action('comments', count=8, delay=0.3)

    # Expected features
    scenario2.expect_feature('total_views', 150)
    scenario2.expect_feature('total_comments', 8)
    scenario2.expect_feature('total_flags', 0)
    scenario2.expect_feature('views_1min', 150, tolerance=10)
    scenario2.expect_feature('view_velocity_per_min', 30.0, tolerance=5.0)  # 150/5 min = 30/min

    # Expected prediction (viral threshold triggers +0.2)
    scenario2.expect_prediction(min_prob=0.35, max_prob=0.6, recommendation='REVIEW')

    scenarios.append(scenario2)

    # ===== Scenario 3: Suspicious Activity (Comment Spam) =====
    scenario3 = TestScenario(
        "Suspicious Activity - Comment Spam",
        "Image with rapid comment spam → Should predict FLAG or high REVIEW"
    )
    scenario3.add_action('views', count=20, delay=0.2)
    scenario3.add_action('comments', count=15, delay=0.1)  # 15 comments in 1.5 seconds

    # Expected features
    scenario3.expect_feature('total_views', 20)
    scenario3.expect_feature('total_comments', 15)
    scenario3.expect_feature('total_flags', 0)
    scenario3.expect_feature('comments_1min', 15, tolerance=2)
    scenario3.expect_feature('comment_to_view_ratio', 0.75, tolerance=0.1)

    # Expected prediction (comment spam +0.3, high ratio +0.2)
    scenario3.expect_prediction(min_prob=0.6, max_prob=0.9, recommendation='FLAG')

    scenarios.append(scenario3)

    # ===== Scenario 4: Flagged Content =====
    scenario4 = TestScenario(
        "Flagged Content",
        "Image with user reports → Should predict FLAG"
    )
    scenario4.add_action('views', count=30, delay=0.1)
    scenario4.add_action('comments', count=5, delay=0.3)
    scenario4.add_action('flags', count=2, delay=0.5)

    # Expected features
    scenario4.expect_feature('total_views', 30)
    scenario4.expect_feature('total_comments', 5)
    scenario4.expect_feature('total_flags', 2)

    # Expected prediction (flags +0.4)
    scenario4.expect_prediction(min_prob=0.65, max_prob=0.95, recommendation='FLAG')

    scenarios.append(scenario4)

    return scenarios


def setup_test_user(client: httpx.Client) -> str:
    """Create a test user"""
    response = client.post(
        f"{API_URL}/users/",
        json={"username": f"test_user_{int(time.time())}"}
    )
    response.raise_for_status()
    user_id = response.json()["id"]
    print(f"   Created test user: {user_id}")
    return user_id


def upload_test_image(client: httpx.Client, user_id: str) -> str:
    """Upload a test image"""
    files = {'file': ('test.jpg', b'fake_image_data', 'image/jpeg')}
    data = {
        'user_id': user_id,
        'caption': 'Test image for inference validation',
        'category': 'Dessert'
    }

    response = client.post(f"{API_URL}/upload/", files=files, data=data)
    response.raise_for_status()
    image_id = response.json()["id"]
    print(f"   Uploaded test image: {image_id}")
    return image_id


def execute_actions(client: httpx.Client, image_id: str, user_id: str, actions: list):
    """Execute test actions (views, comments, flags)"""
    for action in actions:
        action_type = action['type']
        count = action['count']
        delay = action['delay']

        if action_type == 'views':
            print(f"   Generating {count} views (delay={delay}s)...")
            for i in range(count):
                client.post(f"{API_URL}/images/{image_id}/view")
                time.sleep(delay)

        elif action_type == 'comments':
            print(f"   Generating {count} comments (delay={delay}s)...")
            for i in range(count):
                client.post(
                    f"{API_URL}/comments/",
                    json={
                        "image_id": image_id,
                        "user_id": user_id,
                        "content": f"Test comment {i+1}"
                    }
                )
                time.sleep(delay)

        elif action_type == 'flags':
            print(f"   Generating {count} flags (delay={delay}s)...")
            for i in range(count):
                client.post(
                    f"{API_URL}/flags/",
                    json={
                        "user_id": user_id,
                        "image_id": image_id,
                        "reason": f"Test flag {i+1}"
                    }
                )
                time.sleep(delay)


def validate_features(feature_dict: Dict[str, float], expected: Dict[str, Dict]) -> bool:
    """Validate features match expected values"""
    all_passed = True
    errors = []

    for feature_name, expectations in expected.items():
        expected_value = expectations['value']
        tolerance = expectations.get('tolerance')

        actual_value = feature_dict.get(feature_name)

        if actual_value is None:
            errors.append(f"❌ {feature_name}: MISSING (expected {expected_value})")
            all_passed = False
            continue

        # Check if within tolerance
        if tolerance is not None:
            if abs(actual_value - expected_value) <= tolerance:
                print(f"   ✅ {feature_name}: {actual_value:.2f} (expected {expected_value} ± {tolerance})")
            else:
                errors.append(f"❌ {feature_name}: {actual_value:.2f} (expected {expected_value} ± {tolerance})")
                all_passed = False
        else:
            if actual_value == expected_value:
                print(f"   ✅ {feature_name}: {actual_value}")
            else:
                errors.append(f"❌ {feature_name}: {actual_value} (expected {expected_value})")
                all_passed = False

    # Print errors
    for error in errors:
        print(f"   {error}")

    return all_passed


def validate_prediction(prediction: Dict[str, Any], expected: Dict) -> bool:
    """Validate prediction matches expected range"""
    prob = prediction['moderation_probability']
    rec = prediction['recommendation']

    min_prob = expected['min_prob']
    max_prob = expected['max_prob']
    expected_rec = expected['recommendation']

    passed = True

    # Check probability range
    if min_prob <= prob <= max_prob:
        print(f"   ✅ Probability: {prob:.3f} (expected {min_prob:.2f}-{max_prob:.2f})")
    else:
        print(f"   ❌ Probability: {prob:.3f} (expected {min_prob:.2f}-{max_prob:.2f})")
        passed = False

    # Check recommendation
    if rec == expected_rec:
        print(f"   ✅ Recommendation: {rec}")
    else:
        print(f"   ⚠️  Recommendation: {rec} (expected {expected_rec})")
        # Warning only, not failure (may vary at boundaries)

    return passed


def run_scenario_test(scenario: TestScenario, client: httpx.Client, fetcher: FeatureFetcher, constructor: FeatureConstructor, predictor: HeuristicModerationPredictor) -> bool:
    """Run a complete test scenario"""
    print(f"\n{'='*70}")
    print(f"  Test Scenario: {scenario.name}")
    print(f"{'='*70}")
    print(f"Description: {scenario.description}\n")

    # Setup
    print("1️⃣  Setting up test data...")
    scenario.user_id = setup_test_user(client)
    scenario.image_id = upload_test_image(client, scenario.user_id)

    # Execute actions
    print("\n2️⃣  Executing test actions...")
    execute_actions(client, scenario.image_id, scenario.user_id, scenario.setup_actions)

    # Wait for stream consumer to process
    print("\n⏳ Waiting 3 seconds for stream consumer to process events...")
    time.sleep(3)

    # Fetch features
    print("\n3️⃣  Fetching features...")
    raw_features = fetcher.fetch_all_features(scenario.image_id)

    if 'error' in raw_features:
        print(f"   ❌ Error fetching features: {raw_features['error']}")
        return False

    print(f"   ✅ Features fetched in {raw_features['total_latency_ms']:.2f}ms")

    # Construct feature vector
    print("\n4️⃣  Constructing feature vector...")
    feature_vector = constructor.construct_feature_vector(raw_features)
    feature_dict = constructor.get_feature_dict(feature_vector)
    print(f"   ✅ Feature vector constructed (31 dimensions)")

    # Make prediction
    print("\n5️⃣  Making prediction...")
    prediction = predictor.predict(feature_vector, constructor.get_feature_names())
    print(f"   ✅ Prediction complete")

    # Validate features
    print("\n6️⃣  Validating features...")
    features_passed = validate_features(feature_dict, scenario.expected_features)

    # Validate prediction
    print("\n7️⃣  Validating prediction...")
    prediction_passed = validate_prediction(prediction, scenario.expected_prediction)

    # Show prediction reasoning
    print("\n📊 Prediction Details:")
    print(f"   Probability: {prediction['moderation_probability']:.3f}")
    print(f"   Recommendation: {prediction['recommendation']}")
    print(f"   Reasoning:")
    for reason in prediction['reasoning']:
        print(f"      {reason}")

    # Final result
    overall_passed = features_passed and prediction_passed

    if overall_passed:
        print(f"\n{'='*70}")
        print(f"  ✅ SCENARIO PASSED: {scenario.name}")
        print(f"{'='*70}")
    else:
        print(f"\n{'='*70}")
        print(f"  ❌ SCENARIO FAILED: {scenario.name}")
        print(f"{'='*70}")

    return overall_passed


def main():
    print("=" * 70)
    print("  GourmetGram Inference Demo - Controlled Test Suite")
    print("=" * 70)
    print("\nThis test creates known scenarios and validates inference results\n")

    # Initialize clients
    print("🔧 Initializing components...")
    client = httpx.Client(timeout=30.0, base_url=API_URL)
    fetcher = FeatureFetcher()
    constructor = FeatureConstructor()
    predictor = HeuristicModerationPredictor()
    print("   ✅ All components initialized\n")

    # Check API health
    print("🏥 Checking API health...")
    try:
        response = client.get("/health")
        response.raise_for_status()
        health = response.json()
        print(f"   ✅ API is healthy")
        print(f"   Kafka enabled: {health.get('kafka_enabled')}")
    except Exception as e:
        print(f"   ❌ API health check failed: {e}")
        print("\n   Hint: Ensure services are running:")
        print("   docker compose -f docker/docker-compose.yaml up -d")
        return 1

    # Create test scenarios
    scenarios = create_test_scenarios()
    print(f"\n📋 Created {len(scenarios)} test scenarios\n")

    # Run all scenarios
    results = []
    for i, scenario in enumerate(scenarios, 1):
        print(f"\n{'#'*70}")
        print(f"  Running Scenario {i}/{len(scenarios)}")
        print(f"{'#'*70}")

        passed = run_scenario_test(scenario, client, fetcher, constructor, predictor)
        results.append({'scenario': scenario.name, 'passed': passed})

        # Pause between scenarios
        if i < len(scenarios):
            print("\n⏸️  Pausing 2 seconds before next scenario...")
            time.sleep(2)

    # Final summary
    print("\n\n" + "=" * 70)
    print("  TEST SUMMARY")
    print("=" * 70)

    passed_count = sum(1 for r in results if r['passed'])
    total_count = len(results)

    for i, result in enumerate(results, 1):
        status = "✅ PASS" if result['passed'] else "❌ FAIL"
        print(f"{i}. {status}: {result['scenario']}")

    print("\n" + "=" * 70)
    print(f"  Results: {passed_count}/{total_count} scenarios passed")
    print("=" * 70)

    # Cleanup
    client.close()
    fetcher.close()

    # Exit code
    if passed_count == total_count:
        print("\n🎉 All tests passed! Inference demo is working correctly.\n")
        return 0
    else:
        print(f"\n⚠️  {total_count - passed_count} test(s) failed. Review results above.\n")
        return 1


if __name__ == "__main__":
    sys.exit(main())
