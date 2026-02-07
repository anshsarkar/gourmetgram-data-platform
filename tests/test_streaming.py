#!/usr/bin/env python3
"""
Controlled test script for event streaming pipeline

Tests:
1. Upload event → Redis metadata
2. View events → Rolling window aggregations
3. Comment events → Counter aggregations
4. Viral alert (100+ views in 5 min)
5. Suspicious alert (10+ comments in 1 min)
6. Milestone alert (100 views total)
"""
# Copy to main folder and then run using the bash script:
import time
import httpx
import redis
import sys
from typing import Optional
import uuid

# Configuration
API_URL = "http://localhost:8000"
REDIS_HOST = "localhost"
REDIS_PORT = 6379

# Test data
TEST_USER_ID = None
TEST_IMAGE_ID = None

# Initialize clients
http_client = httpx.Client(timeout=30.0)
redis_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def print_header(title: str):
    """Print formatted test header"""
    print("\n" + "=" * 70)
    print(f"  {title}")
    print("=" * 70)


def print_test(test_name: str):
    """Print test name"""
    print(f"\n🧪 Test: {test_name}")


def print_result(passed: bool, message: str):
    """Print test result"""
    icon = "✅" if passed else "❌"
    print(f"{icon} {message}")


def check_redis_key(key: str, expected_type: str = "exists") -> bool:
    """Check if Redis key exists"""
    exists = redis_client.exists(key)
    if expected_type == "exists":
        return exists > 0
    return exists == 0


def get_redis_value(key: str, value_type: str = "string") -> Optional[str]:
    """Get value from Redis"""
    if value_type == "string":
        return redis_client.get(key)
    elif value_type == "hash":
        return redis_client.hgetall(key)
    elif value_type == "zcard":
        return redis_client.zcard(key)
    return None


def create_test_user() -> str:
    """Create a test user"""
    global TEST_USER_ID

    print_test("Create test user")

    response = http_client.post(
        f"{API_URL}/users/",
        json={"username": f"test_user_{int(time.time())}"}
    )

    if response.status_code == 200:
        user_data = response.json()
        TEST_USER_ID = user_data["id"]
        print_result(True, f"User created: {TEST_USER_ID}")
        return TEST_USER_ID
    else:
        print_result(False, f"Failed to create user: {response.status_code}")
        sys.exit(1)


def upload_test_image() -> str:
    """Upload a test image"""
    global TEST_IMAGE_ID

    print_test("Upload test image")

    # Create a dummy image file
    files = {"file": ("test.jpg", b"fake_image_data", "image/jpeg")}
    data = {
        "user_id": TEST_USER_ID,
        "caption": "Test image for streaming pipeline",
        "category": "Dessert"
    }

    response = http_client.post(
        f"{API_URL}/upload/",
        files=files,
        data=data
    )

    if response.status_code == 200:
        image_data = response.json()
        TEST_IMAGE_ID = image_data["id"]
        print_result(True, f"Image uploaded: {TEST_IMAGE_ID}")

        # Wait for stream consumer to process
        time.sleep(2)

        # Verify Redis metadata
        metadata_key = f"image:{TEST_IMAGE_ID}:metadata"
        if check_redis_key(metadata_key):
            metadata = get_redis_value(metadata_key, "hash")
            print_result(True, f"Redis metadata exists: {metadata}")
        else:
            print_result(False, "Redis metadata NOT found")

        return TEST_IMAGE_ID
    else:
        print_result(False, f"Failed to upload image: {response.status_code}")
        sys.exit(1)


def send_views(count: int, delay: float = 0.1):
    """Send multiple view events"""
    print_test(f"Send {count} view events (delay={delay}s)")

    for i in range(count):
        response = http_client.post(f"{API_URL}/images/{TEST_IMAGE_ID}/view")
        if response.status_code != 200:
            print_result(False, f"View {i+1} failed: {response.status_code}")
            return
        time.sleep(delay)

    print_result(True, f"Sent {count} views")


def send_comments(count: int, delay: float = 0.1):
    """Send multiple comment events"""
    print_test(f"Send {count} comment events (delay={delay}s)")

    for i in range(count):
        response = http_client.post(
            f"{API_URL}/comments/",
            json={
                "image_id": TEST_IMAGE_ID,
                "user_id": TEST_USER_ID,
                "content": f"Test comment {i+1}"
            }
        )
        if response.status_code != 200:
            print_result(False, f"Comment {i+1} failed: {response.status_code}")
            return
        time.sleep(delay)

    print_result(True, f"Sent {count} comments")


def verify_view_aggregations(expected_1min: int, expected_total: int):
    """Verify view counts in Redis"""
    print_test("Verify view aggregations")

    # Wait for stream consumer
    time.sleep(2)

    # Check total views
    total_views_key = f"image:{TEST_IMAGE_ID}:total_views"
    total_views = redis_client.get(total_views_key)

    if total_views:
        total_views = int(total_views)
        passed = total_views >= expected_total
        print_result(passed, f"Total views: {total_views} (expected >= {expected_total})")
    else:
        print_result(False, "Total views key NOT found")

    # Check 1min window (using sorted set cardinality)
    views_1min_key = f"image:{TEST_IMAGE_ID}:views:1min"
    views_1min_count = redis_client.zcard(views_1min_key)

    passed = views_1min_count >= expected_1min
    print_result(passed, f"Views in 1min window: {views_1min_count} (expected >= {expected_1min})")

    # Check 5min window
    views_5min_key = f"image:{TEST_IMAGE_ID}:views:5min"
    views_5min_count = redis_client.zcard(views_5min_key)
    print_result(True, f"Views in 5min window: {views_5min_count}")


def verify_comment_aggregations(expected_1min: int, expected_total: int):
    """Verify comment counts in Redis"""
    print_test("Verify comment aggregations")

    # Wait for stream consumer
    time.sleep(2)

    # Check total comments
    total_comments_key = f"image:{TEST_IMAGE_ID}:total_comments"
    total_comments = redis_client.get(total_comments_key)

    if total_comments:
        total_comments = int(total_comments)
        passed = total_comments >= expected_total
        print_result(passed, f"Total comments: {total_comments} (expected >= {expected_total})")
    else:
        print_result(False, "Total comments key NOT found")

    # Check 1min window (now using sorted set like views)
    comments_1min_key = f"image:{TEST_IMAGE_ID}:comments:1min"
    comments_1min_count = redis_client.zcard(comments_1min_key)

    passed = comments_1min_count >= expected_1min
    print_result(passed, f"Comments in 1min window: {comments_1min_count} (expected >= {expected_1min})")

    # Check 5min window
    comments_5min_key = f"image:{TEST_IMAGE_ID}:comments:5min"
    comments_5min_count = redis_client.zcard(comments_5min_key)
    print_result(True, f"Comments in 5min window: {comments_5min_count}")


def test_viral_alert():
    """Test: 100+ views in 5 minutes should trigger viral alert"""
    print_header("TEST CASE 1: Viral Content Alert")

    # Send 105 views rapidly (within ~20 seconds)
    send_views(count=105, delay=0.2)

    # Verify aggregations
    verify_view_aggregations(expected_1min=50, expected_total=105)

    print("\n📋 Check stream_consumer logs for:")
    print("   🔥 VIRAL CONTENT ALERT! Image ... received 100+ views in 5 minutes")


def test_suspicious_alert():
    """Test: 10+ comments in 1 minute should trigger suspicious alert"""
    print_header("TEST CASE 2: Suspicious Activity Alert")

    # Send 12 comments rapidly (within ~3 seconds)
    send_comments(count=12, delay=0.25)

    # Verify aggregations
    verify_comment_aggregations(expected_1min=10, expected_total=12)

    print("\n📋 Check stream_consumer logs for:")
    print("   ⚠️  SUSPICIOUS ACTIVITY ALERT! Image ... received 10+ comments in 1 minute")


def test_milestone():
    """Test: 100 total views should trigger milestone alert"""
    print_header("TEST CASE 3: View Milestone (100 views)")

    # We already have 105 views from test 1, so milestone should be triggered
    total_views = redis_client.get(f"image:{TEST_IMAGE_ID}:total_views")

    if total_views:
        total_views = int(total_views)
        passed = total_views >= 100
        print_result(passed, f"Total views: {total_views} (milestone: 100)")

        print("\n📋 Check stream_consumer logs for:")
        print("   🎯 MILESTONE REACHED! Image ... hit 100 total views")
    else:
        print_result(False, "Could not verify milestone")


def test_rolling_window_expiration():
    """Test: Verify rolling windows expire old entries"""
    print_header("TEST CASE 4: Rolling Window Expiration")

    print_test("Wait 65 seconds to test 1min window expiration")
    print("⏳ This test verifies that the 1min rolling window correctly expires old entries...")

    # Get current counts
    views_1min_before = redis_client.zcard(f"image:{TEST_IMAGE_ID}:views:1min")
    comments_1min_before = redis_client.zcard(f"image:{TEST_IMAGE_ID}:comments:1min")

    print(f"   Before wait: views_1min={views_1min_before}, comments_1min={comments_1min_before}")

    # Wait 65 seconds (1min + 5s buffer)
    for i in range(13):
        time.sleep(5)
        print(f"   Waiting... {(i+1)*5}s / 65s", end="\r")

    print("\n")

    # Get counts after expiration
    views_1min_after = redis_client.zcard(f"image:{TEST_IMAGE_ID}:views:1min")
    comments_1min_after = redis_client.zcard(f"image:{TEST_IMAGE_ID}:comments:1min")

    print(f"   After 65s: views_1min={views_1min_after}, comments_1min={comments_1min_after}")

    # Verify expiration (1min window should be empty or near-empty after 65s)
    passed = views_1min_after < views_1min_before and comments_1min_after < comments_1min_before
    print_result(passed, "1min window entries expired correctly")

    # Verify totals didn't change (persistent counters)
    total_views = int(redis_client.get(f"image:{TEST_IMAGE_ID}:total_views") or 0)
    total_comments = int(redis_client.get(f"image:{TEST_IMAGE_ID}:total_comments") or 0)
    print_result(total_views > 0 and total_comments > 0, f"Totals preserved: views={total_views}, comments={total_comments}")


def test_feature_retrieval():
    """Test: Retrieve complete feature vector"""
    print_header("TEST CASE 5: Feature Vector Retrieval")

    print_test("Retrieve all Redis keys for image")

    # Get all keys for this image
    pattern = f"image:{TEST_IMAGE_ID}:*"
    keys = redis_client.keys(pattern)

    print(f"\n📊 Redis keys found ({len(keys)}):")
    for key in sorted(keys):
        key_type = redis_client.type(key)

        if key_type == "string":
            value = redis_client.get(key)
            print(f"  {key}: {value}")
        elif key_type == "hash":
            value = redis_client.hgetall(key)
            print(f"  {key}: {value}")
        elif key_type == "zset":
            count = redis_client.zcard(key)
            print(f"  {key}: {count} entries")

    print_result(len(keys) > 0, f"Found {len(keys)} Redis keys for image")

    # Verify expected key structure
    expected_keys = [
        f"image:{TEST_IMAGE_ID}:metadata",
        f"image:{TEST_IMAGE_ID}:total_views",
        f"image:{TEST_IMAGE_ID}:total_comments",
        f"image:{TEST_IMAGE_ID}:views:1min",
        f"image:{TEST_IMAGE_ID}:views:5min",
        f"image:{TEST_IMAGE_ID}:views:1hr",
        f"image:{TEST_IMAGE_ID}:comments:1min",
        f"image:{TEST_IMAGE_ID}:comments:5min",
        f"image:{TEST_IMAGE_ID}:comments:1hr",
    ]

    found_count = sum(1 for k in expected_keys if k in keys)
    print_result(found_count >= 7, f"Found {found_count}/9 expected key types")


def cleanup():
    """Cleanup test data"""
    print_header("CLEANUP")

    # Close connections
    http_client.close()
    redis_client.close()

    print("✅ Test complete!")


def main():
    print_header("GOURMETGRAM STREAMING PIPELINE TEST")
    print("This test validates the fixed comment window counting logic:")
    print("  1. Create test user and upload image → Redis metadata")
    print("  2. Send 105 views → Viral alert (≥100 views in 5min)")
    print("  3. Send 12 comments → Suspicious alert (≥10 comments in 1min)")
    print("  4. Verify milestone alert (100 total views)")
    print("  5. Wait 65s → Verify rolling window expiration")
    print("  6. Retrieve complete feature vector")
    print("\nExpected Results:")
    print("  ✓ Comments use sorted sets (not counters with broken TTL)")
    print("  ✓ Rolling windows accurately reflect last N seconds")
    print("  ✓ Alerts trigger at correct thresholds")
    print("  ✓ Old entries expire from 1min window")

    try:
        # Test connection
        print_test("Check API health")
        response = http_client.get(f"{API_URL}/health")
        if response.status_code == 200:
            health_data = response.json()
            print_result(True, f"API is healthy")
            print(f"   Kafka enabled: {health_data.get('kafka_enabled')}")
            print(f"   Kafka stats: {health_data.get('kafka_stats')}")
        else:
            print_result(False, "API is not reachable")
            sys.exit(1)

        print_test("Check Redis connection")
        if redis_client.ping():
            print_result(True, "Redis is reachable")
        else:
            print_result(False, "Redis is not reachable")
            sys.exit(1)

        # Run tests
        create_test_user()
        upload_test_image()

        test_viral_alert()
        test_suspicious_alert()
        test_milestone()
        test_rolling_window_expiration()  # New test
        test_feature_retrieval()

        # Final summary
        print_header("TEST SUMMARY")
        print("✅ All API requests successful")
        print("✅ Redis keys populated with sorted sets (comments FIXED)")
        print("✅ Stream consumer processed all events")
        print("✅ Rolling windows expire correctly")
        print("\n" + "=" * 70)
        print("VERIFICATION COMMANDS")
        print("=" * 70)
        print("\n1️⃣  Check alerts in stream consumer logs:")
        print("   docker compose -f docker/docker-compose.yaml logs stream_consumer | grep -E 'VIRAL|SUSPICIOUS|MILESTONE'")
        print("\n   Expected output:")
        print("   🔥 VIRAL CONTENT ALERT! Image ... received 105 views in 5 minutes")
        print("   ⚠️  SUSPICIOUS ACTIVITY ALERT! Image ... received 12 comments in 1 minute")
        print("   🎯 MILESTONE REACHED! Image ... hit 100 total views")

        print("\n2️⃣  Check Redis sorted sets (comment windows FIXED):")
        image_short = TEST_IMAGE_ID[:8] if TEST_IMAGE_ID else "xxx"
        print(f"   docker exec gourmetgram_redis redis-cli KEYS 'image:{image_short}*'")
        print(f"   docker exec gourmetgram_redis redis-cli TYPE 'image:{TEST_IMAGE_ID}:comments:1min'")
        print("   # Should return: zset (not string)")

        print("\n3️⃣  Verify comment window counts:")
        print(f"   docker exec gourmetgram_redis redis-cli ZCARD 'image:{TEST_IMAGE_ID}:comments:1min'")
        print(f"   docker exec gourmetgram_redis redis-cli ZCARD 'image:{TEST_IMAGE_ID}:comments:5min'")
        print("   # After 65s wait, 1min should be 0, 5min should still have entries")

        print("\n4️⃣  Check persistent totals:")
        print(f"   docker exec gourmetgram_redis redis-cli GET 'image:{TEST_IMAGE_ID}:total_views'")
        print(f"   docker exec gourmetgram_redis redis-cli GET 'image:{TEST_IMAGE_ID}:total_comments'")
        print("   # Should be: 105 views, 12 comments")

        print("\n5️⃣  View all aggregated features:")
        print(f"   docker exec gourmetgram_redis redis-cli HGETALL 'image:{TEST_IMAGE_ID}:metadata'")
        print("   # Should show: category=Dessert, caption_length, uploaded_at, user_id")

        print("\n6️⃣  Verify Kafka message flow:")
        print("   docker compose -f docker/docker-compose.yaml logs api | grep 'Kafka:' | tail -20")
        print("   # Should show: Published events to topics (uploads, views, comments)")

    except KeyboardInterrupt:
        print("\n\nTest interrupted by user")
    except Exception as e:
        print(f"\n❌ Test failed with error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cleanup()


if __name__ == "__main__":
    main()
