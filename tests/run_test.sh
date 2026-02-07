#!/bin/bash

echo "=========================================="
echo "GourmetGram Streaming Pipeline Test"
echo "=========================================="
echo ""

# Check if services are running
echo "🔍 Checking services..."
cd docker

if ! docker-compose ps | grep -q "gourmetgram_api.*Up"; then
    echo "⚠️  API service not running. Starting services..."
    docker-compose up -d postgres minio redpanda redis api stream_consumer

    echo "⏳ Waiting 30s for services to be ready..."
    sleep 30
fi

echo "✅ Services are running"
echo ""

# Run the test
echo "🧪 Running test script..."
cd ..
python3 test_streaming.py

echo ""
echo "=========================================="
echo "Check Logs"
echo "=========================================="
echo ""
echo "📋 Stream Consumer Logs (last 50 lines):"
docker-compose -f docker/docker-compose.yaml logs --tail=50 stream_consumer | grep -E "(Upload|View|Comment|Flag|ALERT|MILESTONE|Processed)"

echo ""
echo "📋 API Kafka Events (last 20 lines):"
docker-compose -f docker/docker-compose.yaml logs --tail=20 api | grep "Kafka:"

echo ""
echo "✅ Test complete!"
echo ""
echo "To see full logs:"
echo "  docker-compose -f docker/docker-compose.yaml logs -f stream_consumer"
echo "  docker-compose -f docker/docker-compose.yaml logs -f api"
