#!/usr/bin/env python3
"""
Inference Integration Test

Sends predefined requests through the API (without the data generator running)
so you can verify each stage in the UI dashboards.

Two scenarios:
  1. SAFE IMAGE  — few views, no flags → heuristic should say SAFE
  2. VIRAL IMAGE — 110 views in quick succession → triggers moderation request
                   → inference service produces a decision in moderation_decisions

Prerequisites:
  - All services running EXCEPT data_generator (stop it first):
      docker compose -f docker/docker-compose.yaml stop data_generator
  - Postgres volume must include the moderation_decisions table.
    If not, recreate:  docker compose down -v && docker compose up -d

Usage:
    pip install requests redis psycopg2-binary
    python tests/test_inference_integration.py

After running, inspect these UIs:
  - Adminer     (localhost:5050)  → PostgreSQL tables
  - RedPanda    (localhost:8090)  → Kafka topics and messages
  - RedisInsight (localhost:8081) → Redis keys and values
  - MinIO       (localhost:9001)  → Uploaded images
"""

import base64
import json
import os
import sys
import time
import uuid

import psycopg2
import redis
import requests

# ─── Configuration ────────────────────────────────────────────────────────────

API_BASE     = os.getenv("API_BASE", "http://localhost:8000")
REDIS_HOST   = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT   = int(os.getenv("REDIS_PORT", "6379"))
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/gourmetgram"
)

# Scenario parameters
SAFE_VIEWS       = 5
SAFE_COMMENTS    = 2
VIRAL_VIEWS      = 110   # above the 100 threshold
VIRAL_COMMENTS   = 3
INFERENCE_WAIT   = 10    # seconds to wait for inference service to process

# ─── Helpers ──────────────────────────────────────────────────────────────────

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"

_errors = []


def check(name: str, condition: bool, detail: str = ""):
    if condition:
        print(f"  {PASS} {name}")
    else:
        msg = f"  {FAIL} {name}" + (f" — {detail}" if detail else "")
        print(msg)
        _errors.append(name)


def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


# 1×1 red pixel PNG for uploads
TINY_PNG = base64.b64decode(
    "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
)


# ─── Setup: Create Users + Images ────────────────────────────────────────────

def setup():
    section("Setup: Health Check + Create Users and Images")

    # Health check
    r = requests.get(f"{API_BASE}/health", timeout=5)
    check("API is healthy", r.status_code == 200 and r.json().get("status") == "healthy")

    # Create user
    username = f"test_inf_{uuid.uuid4().hex[:6]}"
    r = requests.post(f"{API_BASE}/users/", json={"username": username})
    check("User created", r.status_code == 200)
    user_id = r.json()["id"]
    print(f"  {INFO} user_id  = {user_id}")
    print(f"  {INFO} username = {username}")

    # Upload SAFE image (Bread category)
    r = requests.post(
        f"{API_BASE}/upload/",
        data={"user_id": user_id, "caption": "Fresh sourdough bread", "category": "Bread"},
        files={"file": ("bread.png", TINY_PNG, "image/png")},
    )
    check("Safe image uploaded", r.status_code == 200)
    safe_image_id = r.json()["id"]
    print(f"  {INFO} safe_image_id  = {safe_image_id}")

    # Upload VIRAL image (Dessert category)
    r = requests.post(
        f"{API_BASE}/upload/",
        data={"user_id": user_id, "caption": "Amazing chocolate cake", "category": "Dessert"},
        files={"file": ("cake.png", TINY_PNG, "image/png")},
    )
    check("Viral image uploaded", r.status_code == 200)
    viral_image_id = r.json()["id"]
    print(f"  {INFO} viral_image_id = {viral_image_id}")

    return user_id, safe_image_id, viral_image_id


# ─── Scenario 1: Safe Image ──────────────────────────────────────────────────

def scenario_safe(user_id: str, image_id: str):
    section(f"Scenario 1: SAFE Image ({SAFE_VIEWS} views, {SAFE_COMMENTS} comments)")

    # Send views
    for i in range(SAFE_VIEWS):
        r = requests.post(f"{API_BASE}/images/{image_id}/view")
        check(f"View {i+1}/{SAFE_VIEWS}", r.status_code == 200)
        time.sleep(0.05)

    # Send comments
    for i in range(SAFE_COMMENTS):
        r = requests.post(f"{API_BASE}/comments/", json={
            "image_id": image_id,
            "user_id": user_id,
            "content": f"Nice bread! Comment {i+1}"
        })
        check(f"Comment {i+1}/{SAFE_COMMENTS}", r.status_code == 200)
        time.sleep(0.05)

    print(f"\n  {INFO} Waiting 3s for stream consumer to process...")
    time.sleep(3)


# ─── Scenario 2: Viral Image ─────────────────────────────────────────────────

def scenario_viral(user_id: str, image_id: str):
    section(f"Scenario 2: VIRAL Image ({VIRAL_VIEWS} views, {VIRAL_COMMENTS} comments)")

    # Send views rapidly to cross the 100 threshold
    for i in range(VIRAL_VIEWS):
        r = requests.post(f"{API_BASE}/images/{image_id}/view")
        if (i + 1) % 25 == 0:
            print(f"  {INFO} Sent {i+1}/{VIRAL_VIEWS} views...")
        time.sleep(0.02)

    # Send comments
    for i in range(VIRAL_COMMENTS):
        r = requests.post(f"{API_BASE}/comments/", json={
            "image_id": image_id,
            "user_id": user_id,
            "content": f"This cake is incredible! Comment {i+1}"
        })
        time.sleep(0.05)

    print(f"\n  {INFO} Waiting {INFERENCE_WAIT}s for stream consumer + inference service...")
    time.sleep(INFERENCE_WAIT)


# ─── Verify Redis ────────────────────────────────────────────────────────────

def verify_redis(safe_image_id: str, viral_image_id: str):
    section("Verify: Redis State")

    rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
    ts = time.time()

    for label, image_id, expected_views, expected_comments in [
        ("SAFE",  safe_image_id,  SAFE_VIEWS,  SAFE_COMMENTS),
        ("VIRAL", viral_image_id, VIRAL_VIEWS, VIRAL_COMMENTS),
    ]:
        print(f"\n  {BOLD}[{label} image: {image_id[:8]}...]{RESET}")

        total_views = int(rc.get(f"image:{image_id}:total_views") or 0)
        total_comments = int(rc.get(f"image:{image_id}:total_comments") or 0)

        check(f"  total_views == {expected_views}", total_views == expected_views,
              f"got {total_views}")
        check(f"  total_comments == {expected_comments}", total_comments == expected_comments,
              f"got {total_comments}")

        views_5min = rc.zcount(f"image:{image_id}:views:5min", ts - 300, ts)
        print(f"  {INFO} views in 5min window: {views_5min}")

        metadata = rc.hgetall(f"image:{image_id}:metadata")
        print(f"  {INFO} metadata: {metadata}")

    print(f"\n  {BOLD}What to verify in RedisInsight (localhost:8081):{RESET}")
    print(f"  - Search for key: image:{viral_image_id[:8]}*")
    print(f"  - Check the sorted set image:{viral_image_id}:views:5min")
    print(f"    → should have {VIRAL_VIEWS} entries")
    print(f"  - Check the hash image:{viral_image_id}:metadata")
    print(f"    → category=Dessert, caption_length=21")


# ─── Verify PostgreSQL ───────────────────────────────────────────────────────

def verify_postgres(user_id: str, safe_image_id: str, viral_image_id: str):
    section("Verify: PostgreSQL")

    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    # Check images table
    for label, image_id, expected_views in [
        ("SAFE", safe_image_id, SAFE_VIEWS),
        ("VIRAL", viral_image_id, VIRAL_VIEWS),
    ]:
        cur.execute("SELECT category, caption, views FROM images WHERE id = %s", (image_id,))
        row = cur.fetchone()
        check(f"{label} image exists in DB", row is not None)
        if row:
            check(f"  {label} views == {expected_views}", row[2] == expected_views,
                  f"got {row[2]}")
            print(f"  {INFO} {label}: category={row[0]}, caption={row[1]}, views={row[2]}")

    # Check comments
    cur.execute("SELECT COUNT(*) FROM comments WHERE image_id = %s", (safe_image_id,))
    safe_comments = cur.fetchone()[0]
    check(f"SAFE image comments == {SAFE_COMMENTS}", safe_comments == SAFE_COMMENTS,
          f"got {safe_comments}")

    cur.execute("SELECT COUNT(*) FROM comments WHERE image_id = %s", (viral_image_id,))
    viral_comments = cur.fetchone()[0]
    check(f"VIRAL image comments == {VIRAL_COMMENTS}", viral_comments == VIRAL_COMMENTS,
          f"got {viral_comments}")

    # Check moderation_decisions (only viral image should have a decision)
    cur.execute(
        "SELECT inference_mode, moderation_probability, recommendation, trigger_type "
        "FROM moderation_decisions WHERE image_id = %s ORDER BY created_at DESC",
        (viral_image_id,)
    )
    decisions = cur.fetchall()

    check("Viral image has moderation decision(s)", len(decisions) > 0,
          "No decisions found — is the inference service running?")

    if decisions:
        mode, prob, rec, trigger = decisions[0]
        check("Decision inference_mode == 'heuristic'", mode == "heuristic",
              f"got '{mode}'")
        check("Decision trigger_type == 'viral'", trigger == "viral",
              f"got '{trigger}'")
        check("Decision recommendation is SAFE/REVIEW/FLAG",
              rec in ("SAFE", "REVIEW", "FLAG"), f"got '{rec}'")

        print(f"\n  {INFO} Latest moderation decision for viral image:")
        print(f"       mode={mode}, probability={prob:.3f}, recommendation={rec}, trigger={trigger}")

    # SAFE image should NOT have a moderation decision
    cur.execute(
        "SELECT COUNT(*) FROM moderation_decisions WHERE image_id = %s",
        (safe_image_id,)
    )
    safe_decisions = cur.fetchone()[0]
    check("Safe image has NO moderation decision", safe_decisions == 0,
          f"got {safe_decisions} decisions (expected 0)")

    cur.close()
    conn.close()

    print(f"\n  {BOLD}What to verify in Adminer (localhost:5050):{RESET}")
    print(f"  Login: PostgreSQL, server=localhost, user=user, password=password, db=gourmetgram")
    print(f"  - SELECT * FROM images ORDER BY uploaded_at DESC LIMIT 5;")
    print(f"    → two test images with {SAFE_VIEWS} and {VIRAL_VIEWS} views")
    print(f"  - SELECT * FROM moderation_decisions ORDER BY created_at DESC;")
    print(f"    → viral image has a decision with trigger_type='viral'")
    print(f"    → safe image has NO decision")


# ─── Verify RedPanda Topics ──────────────────────────────────────────────────

def verify_redpanda(safe_image_id: str, viral_image_id: str):
    section("Verify: RedPanda Topics")

    try:
        from kafka import KafkaConsumer
    except ImportError:
        print(f"  {INFO} kafka-python not installed — skipping Redpanda check")
        return

    brokers = os.getenv("REDPANDA_BROKERS", "localhost:19092")

    # Check moderation_requests topic specifically
    try:
        consumer = KafkaConsumer(
            "gourmetgram.moderation_requests",
            bootstrap_servers=brokers,
            auto_offset_reset='earliest',
            enable_auto_commit=False,
            consumer_timeout_ms=5000,
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            group_id=f"test_inf_{uuid.uuid4().hex[:6]}"
        )

        viral_requests = []
        safe_requests = []
        for msg in consumer:
            if msg.value.get("image_id") == viral_image_id:
                viral_requests.append(msg.value)
            elif msg.value.get("image_id") == safe_image_id:
                safe_requests.append(msg.value)
        consumer.close()

        check("Viral image has moderation request in Kafka", len(viral_requests) > 0,
              f"found {len(viral_requests)}")
        check("Safe image has NO moderation request", len(safe_requests) == 0,
              f"found {len(safe_requests)} (expected 0)")

        if viral_requests:
            req = viral_requests[0]
            check("Request trigger == 'viral'", req.get("trigger") == "viral",
                  f"got '{req.get('trigger')}'")
            print(f"  {INFO} Moderation request: {json.dumps(req, indent=2)}")

    except Exception as e:
        check("Redpanda moderation_requests readable", False, str(e))

    print(f"\n  {BOLD}What to verify in RedPanda Console (localhost:8090):{RESET}")
    print(f"  - Topics → gourmetgram.views → Messages")
    print(f"    → filter by image_id to see {VIRAL_VIEWS} view events for the viral image")
    print(f"  - Topics → gourmetgram.moderation_requests → Messages")
    print(f"    → should see a message with trigger='viral' for {viral_image_id[:8]}...")
    print(f"    → should NOT see any message for {safe_image_id[:8]}...")


# ─── Print UI Guide ──────────────────────────────────────────────────────────

def print_ui_guide(user_id, safe_image_id, viral_image_id):
    section("UI Verification Guide")

    print(f"""
  {BOLD}Test Data Summary:{RESET}
    user_id        = {user_id}
    safe_image_id  = {safe_image_id}  (Bread, {SAFE_VIEWS} views)
    viral_image_id = {viral_image_id}  (Dessert, {VIRAL_VIEWS} views)

  {BOLD}1. Adminer (localhost:5050){RESET}
     Login: PostgreSQL | server=localhost | user=user | pass=password | db=gourmetgram
     Queries to run:
       SELECT * FROM images WHERE id IN ('{safe_image_id}', '{viral_image_id}');
       SELECT * FROM comments WHERE image_id IN ('{safe_image_id}', '{viral_image_id}');
       SELECT * FROM moderation_decisions ORDER BY created_at DESC;

  {BOLD}2. RedPanda Console (localhost:8090){RESET}
     - Topics → gourmetgram.views → should have {SAFE_VIEWS + VIRAL_VIEWS} total test events
     - Topics → gourmetgram.moderation_requests → 1 message for viral image

  {BOLD}3. RedisInsight (localhost:8081){RESET}
     - Browser → search "image:{viral_image_id[:12]}*"
     - Keys to look for:
       image:{viral_image_id}:total_views     = {VIRAL_VIEWS}
       image:{viral_image_id}:views:5min      = sorted set with {VIRAL_VIEWS} entries
       image:{viral_image_id}:metadata         = hash with category=Dessert

  {BOLD}4. MinIO Console (localhost:9001){RESET}
     Login: admin / password
     - Bucket: gourmetgram-images → should have bread.png and cake.png uploads

  {BOLD}5. Inference Service Logs{RESET}
     docker logs inference-service --tail 20
     → Should show: "Moderation: image={viral_image_id[:8]}... mode=heuristic prob=X.XXX rec=SAFE/REVIEW/FLAG"
""")


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "=" * 60)
    print("  GourmetGram Inference Integration Test")
    print("  (stop data_generator first: docker compose stop data_generator)")
    print("=" * 60)

    user_id, safe_image_id, viral_image_id = setup()

    scenario_safe(user_id, safe_image_id)
    scenario_viral(user_id, viral_image_id)

    verify_redis(safe_image_id, viral_image_id)
    verify_postgres(user_id, safe_image_id, viral_image_id)
    verify_redpanda(safe_image_id, viral_image_id)

    print_ui_guide(user_id, safe_image_id, viral_image_id)

    # Final summary
    print("=" * 60)
    if _errors:
        print(f"  \033[91mFAILED\033[0m — {len(_errors)} check(s) failed:")
        for e in _errors:
            print(f"    - {e}")
        sys.exit(1)
    else:
        print(f"  \033[92mALL CHECKS PASSED\033[0m")
    print("=" * 60 + "\n")


if __name__ == "__main__":
    main()
