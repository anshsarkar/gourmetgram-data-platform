#!/bin/bash

# GourmetGram Streaming Pipeline Test Runner
# This script validates the complete streaming pipeline including milestone persistence

echo "======================================================================"
echo "  GourmetGram Streaming Pipeline Test - Complete Validation"
echo "======================================================================"
echo ""
echo "What this test validates:"
echo "  ✓ Comments use sorted sets (not broken incr_with_ttl)"
echo "  ✓ Rolling windows accurately count events in last N seconds"
echo "  ✓ Alert thresholds trigger correctly"
echo "  ✓ Window entries expire properly after TTL"
echo "  ✓ Milestones persisted to PostgreSQL database"
echo ""

# Navigate to docker directory
cd docker || exit 1

# Check if services are running
echo "🔍 Checking services..."
if ! docker compose ps | grep -q "gourmetgram_api.*Up"; then
    echo "⚠️  Services not running. Starting infrastructure..."
    docker compose up -d postgres minio redpanda redis api stream_consumer

    echo "⏳ Waiting 35s for all services to be healthy..."
    sleep 35
else
    echo "✅ Services already running"
fi

# Verify services are healthy
echo ""
echo "🏥 Service Health Check:"
docker compose ps | grep -E "gourmetgram_(api|stream_consumer|redis|redpanda)"

echo ""
echo "======================================================================"
echo "  Running Test Script (includes 65s rolling window test)"
echo "======================================================================"
echo ""

# Run the test from project root
cd ..
python3 tests/test_streaming.py

# Store exit code
TEST_EXIT_CODE=$?

echo ""
echo "======================================================================"
echo "  Post-Test Verification"
echo "======================================================================"
echo ""

# Extract image ID from test output (if test completed)
# Full UUID format: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx (36 chars with dashes)
IMAGE_ID=$(docker compose -f docker/docker-compose.yaml logs stream_consumer --tail=200 | grep -oE "[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}" | head -1)

if [ -n "$IMAGE_ID" ]; then
    echo "📊 Test Image ID: $IMAGE_ID..."
    echo ""

    echo "1️⃣  Alerts Triggered:"
    docker compose -f docker/docker-compose.yaml logs stream_consumer --tail=200 | grep -E "🔥|⚠️|🎯" | tail -5

    echo ""
    echo "2️⃣  Comment Window Structure (FIXED):"
    echo "   Checking Redis key types..."
    # IMAGE_ID="d45c1c99-f5e5-45fb-911c-ff4eb54617fa"
    COMMENT_KEY_TYPE=$(docker exec gourmetgram_redis redis-cli TYPE image:${IMAGE_ID}:comments:1min 2>/dev/null | xargs)
    if [ "$COMMENT_KEY_TYPE" = "zset" ]; then
        echo "   ✅ comments:1min is zset (CORRECT - uses sorted sets now)"
    else
        echo "   ❌ comments:1min is $COMMENT_KEY_TYPE (WRONG - should be zset)"
    fi

    COMMENT_KEY_TYPE=$(docker exec gourmetgram_redis redis-cli TYPE image:${IMAGE_ID}:comments:5min 2>/dev/null | xargs)
    if [ "$COMMENT_KEY_TYPE" = "zset" ]; then
        echo "   ✅ comments:5min is zset (CORRECT - uses sorted sets now)"
    else
        echo "   ❌ comments:5min is $COMMENT_KEY_TYPE (WRONG - should be zset)"
    fi

    echo ""
    echo "3️⃣  Redis Keys Created:"
    docker exec gourmetgram_redis redis-cli KEYS "image:${IMAGE_ID}*" | head -10

    echo ""
    echo "4️⃣  Kafka Event Flow:"
    docker compose -f docker/docker-compose.yaml logs api --tail=50 | grep "Kafka:" | tail -10

    echo ""
    echo "5️⃣  Milestone Persistence to Database:"
    MILESTONE_COUNT=$(docker exec gourmetgram_db psql -U user -d gourmetgram -t -c "SELECT COUNT(*) FROM image_milestones;" 2>/dev/null | xargs)
    if [ -n "$MILESTONE_COUNT" ] && [ "$MILESTONE_COUNT" -gt 0 ]; then
        echo "   ✅ Found $MILESTONE_COUNT milestone(s) in database"
        docker exec gourmetgram_db psql -U user -d gourmetgram -c "SELECT milestone_value, COUNT(*) FROM image_milestones GROUP BY milestone_value ORDER BY milestone_value;" 2>/dev/null
    else
        echo "   ⚠️  No milestones found in database yet"
        echo "   Check stream_consumer logs: docker compose -f docker/docker-compose.yaml logs stream_consumer | grep 'Milestone persisted'"
    fi
else
    echo "⚠️  Could not extract test image ID from logs"
    echo "   Check if stream_consumer is running and processing events"
fi

echo ""
echo "======================================================================"
echo "  Manual Verification Commands"
echo "======================================================================"
echo ""
echo "To check alerts:"
echo "  docker compose -f docker/docker-compose.yaml logs stream_consumer | grep -E 'VIRAL|SUSPICIOUS|MILESTONE'"
echo ""
echo "To verify comment windows use sorted sets:"
echo "  docker exec gourmetgram_redis redis-cli KEYS 'image:*:comments:*'"
echo "  docker exec gourmetgram_redis redis-cli TYPE 'image:<ID>:comments:1min'"
echo ""
echo "To check window counts:"
echo "  docker exec gourmetgram_redis redis-cli ZCARD 'image:<ID>:comments:1min'"
echo "  docker exec gourmetgram_redis redis-cli ZCARD 'image:<ID>:views:1min'"
echo ""
echo "To verify milestone persistence to database:"
echo "  docker exec gourmetgram_db psql -U user -d gourmetgram -c 'SELECT COUNT(*) FROM image_milestones;'"
echo "  docker exec gourmetgram_db psql -U user -d gourmetgram -c 'SELECT milestone_value, COUNT(*) FROM image_milestones GROUP BY milestone_value;'"
echo "  docker exec gourmetgram_db psql -U user -d gourmetgram -c \"SELECT SUBSTRING(image_id::text, 1, 16), milestone_value, reached_at FROM image_milestones ORDER BY reached_at DESC LIMIT 10;\""
echo ""
echo "To check milestone persistence logs:"
echo "  docker compose -f docker/docker-compose.yaml logs stream_consumer | grep 'Milestone persisted'"
echo ""
echo "To see full stream consumer logs:"
echo "  docker compose -f docker/docker-compose.yaml logs -f stream_consumer"
echo ""

exit $TEST_EXIT_CODE
