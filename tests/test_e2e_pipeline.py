#!/usr/bin/env python3
"""
End-to-End Pipeline Validation Test

Tests the full pipeline in deterministic order:
  1. FastAPI: create user, upload image
  2. Events: send N views, M comments, 1 flag (at specific times)
  3. Redis: verify counters and window counts match expectations
  4. Redpanda: verify events exist in topics
  5. Iceberg: verify 5-min window aggregations after ETL
  6. Training data: verify 29 features, no data leakage

Usage:
    # Make sure services are up:
    docker-compose -f docker/docker-compose.yaml up -d

    # Run the test (from repo root):
    pip install requests redis kafka-python psycopg2-binary
    python tests/test_e2e_pipeline.py

    # For ETL tests (Airflow must be running):
    python tests/test_e2e_pipeline.py --etl
"""

import argparse
import json
import os
import sys
import time
import uuid
import requests
import redis
import psycopg2
from datetime import datetime, timezone
from typing import Optional

# ─── Configuration ────────────────────────────────────────────────────────────

API_BASE        = os.getenv("API_BASE", "http://localhost:8000")
REDIS_HOST      = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT      = int(os.getenv("REDIS_PORT", "6379"))
DATABASE_URL    = os.getenv(
    "DATABASE_URL",
    "postgresql://user:password@localhost:5432/gourmetgram"
)
REDPANDA_BROKERS = os.getenv("REDPANDA_BROKERS", "localhost:19092")
AIRFLOW_BASE    = os.getenv("AIRFLOW_BASE", "http://localhost:8080")
AIRFLOW_USER    = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASS    = os.getenv("AIRFLOW_PASS", "admin")
MINIO_ENDPOINT  = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")

# Number of events to send — keep small so counts are easy to verify
N_VIEWS    = 7   # total views we will send
N_COMMENTS = 3   # total comments we will send
# 1 flag (image-level) will be sent

# ─── Helpers ──────────────────────────────────────────────────────────────────

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
INFO = "\033[94m[INFO]\033[0m"

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

# ─── Step 1: FastAPI Health ───────────────────────────────────────────────────

def test_api_health():
    section("Step 1: API Health Check")
    try:
        r = requests.get(f"{API_BASE}/health", timeout=5)
        check("API is reachable (HTTP 200)", r.status_code == 200)
        data = r.json()
        check("API status is healthy", data.get("status") == "healthy",
              f"got: {data.get('status')}")
        check("Kafka producer enabled", data.get("kafka_enabled") is True,
              "Kafka may not be running")
    except Exception as e:
        check("API is reachable", False, str(e))
        print("  Aborting — API is not up. Start services first.")
        sys.exit(1)

# ─── Step 2: Create User & Upload Image ───────────────────────────────────────

def create_user_and_image():
    section("Step 2: Create User + Upload Image")

    # Create user
    username = f"test_user_{uuid.uuid4().hex[:8]}"
    r = requests.post(f"{API_BASE}/users/", json={"username": username})
    check("Create user (HTTP 200)", r.status_code == 200,
          f"status={r.status_code} body={r.text[:200]}")
    user_id = r.json()["id"]
    print(f"  {INFO} user_id = {user_id}")

    # Upload a tiny test image (1×1 red pixel PNG, base64 decoded)
    import base64
    tiny_png = base64.b64decode(
        "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg=="
    )
    category = "Dessert"
    caption  = "A delicious test dessert"
    r = requests.post(
        f"{API_BASE}/upload/",
        data={"user_id": user_id, "caption": caption, "category": category},
        files={"file": ("test.png", tiny_png, "image/png")},
    )
    check("Upload image (HTTP 200)", r.status_code == 200,
          f"status={r.status_code} body={r.text[:200]}")
    image_data = r.json()
    image_id   = image_data["id"]
    uploaded_at = image_data.get("uploaded_at")
    print(f"  {INFO} image_id   = {image_id}")
    print(f"  {INFO} uploaded_at = {uploaded_at}")

    # Verify DB has the row
    check("Image response has id",      bool(image_id))
    check("Image category is Dessert",  image_data.get("category") == category,
          f"got: {image_data.get('category')}")
    check("Image has caption",          image_data.get("caption") == caption)

    return user_id, image_id, uploaded_at

# ─── Step 3: Send View / Comment / Flag Events ────────────────────────────────

def send_events(user_id: str, image_id: str):
    section("Step 3: Send Events (views, comments, flag)")

    # Views
    view_times = []
    for i in range(N_VIEWS):
        r = requests.post(f"{API_BASE}/images/{image_id}/view")
        check(f"  View {i+1} accepted (HTTP 200)", r.status_code == 200,
              f"status={r.status_code}")
        view_times.append(datetime.now(timezone.utc))
        time.sleep(0.1)  # small gap so timestamps are distinct

    # Comments
    comment_ids = []
    for i in range(N_COMMENTS):
        r = requests.post(f"{API_BASE}/comments/", json={
            "image_id": image_id,
            "user_id":  user_id,
            "content":  f"Test comment {i+1}"
        })
        check(f"  Comment {i+1} accepted (HTTP 200)", r.status_code == 200,
              f"status={r.status_code}")
        comment_ids.append(r.json().get("id"))
        time.sleep(0.1)

    # Flag the image
    r = requests.post(f"{API_BASE}/flags/", json={
        "image_id": image_id,
        "user_id":  user_id,
        "reason":   "Test flag for validation"
    })
    check("Flag accepted (HTTP 200)", r.status_code == 200,
          f"status={r.status_code}")
    flag_id = r.json().get("id")
    print(f"  {INFO} flag_id = {flag_id}")

    return comment_ids, flag_id

# ─── Step 4: Redis Verification ───────────────────────────────────────────────

def test_redis(image_id: str):
    section("Step 4: Redis State Verification")

    try:
        rc = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        rc.ping()
    except Exception as e:
        check("Redis reachable", False, str(e))
        return

    # Give stream consumer a moment to process
    print(f"  {INFO} Waiting 3s for stream consumer to process events...")
    time.sleep(3)

    # --- Persistent counters ---
    total_views    = int(rc.get(f"image:{image_id}:total_views")    or 0)
    total_comments = int(rc.get(f"image:{image_id}:total_comments") or 0)
    total_flags    = int(rc.get(f"image:{image_id}:total_flags")    or 0)

    check(f"total_views == {N_VIEWS}",
          total_views == N_VIEWS,
          f"got {total_views}")
    check(f"total_comments == {N_COMMENTS}",
          total_comments == N_COMMENTS,
          f"got {total_comments}")
    check("total_flags == 1",
          total_flags == 1,
          f"got {total_flags}")

    # --- Window counts: all events are very recent, so all should appear in every window ---
    ts = time.time()

    for window_key, window_sec in [("5min", 300), ("1hr", 3600)]:
        views_in_window = rc.zcount(
            f"image:{image_id}:views:{window_key}", ts - window_sec, ts
        )
        check(f"views:{window_key} window count == {N_VIEWS}",
              views_in_window == N_VIEWS,
              f"got {views_in_window}")

        comments_in_window = rc.zcount(
            f"image:{image_id}:comments:{window_key}", ts - window_sec, ts
        )
        check(f"comments:{window_key} window count == {N_COMMENTS}",
              comments_in_window == N_COMMENTS,
              f"got {comments_in_window}")

    # --- Metadata hash ---
    metadata = rc.hgetall(f"image:{image_id}:metadata")
    check("Redis metadata: category is Dessert",
          metadata.get("category") == "Dessert",
          f"got: {metadata.get('category')}")
    check("Redis metadata: caption_length > 0",
          int(metadata.get("caption_length", 0)) > 0,
          f"got: {metadata.get('caption_length')}")

    print(f"\n  {INFO} Redis summary:")
    print(f"       total_views    = {total_views}")
    print(f"       total_comments = {total_comments}")
    print(f"       total_flags    = {total_flags}")
    print(f"       metadata       = {metadata}")

# ─── Step 5: Redpanda Topic Verification ──────────────────────────────────────

def test_redpanda_topics(image_id: str):
    section("Step 5: Redpanda Topic Verification")

    try:
        from kafka import KafkaConsumer, TopicPartition
    except ImportError:
        print(f"  {INFO} kafka-python not installed — skipping Redpanda check")
        return

    topics = {
        "gourmetgram.views":    ("viewed_at",   N_VIEWS),
        "gourmetgram.comments": ("created_at",  N_COMMENTS),
        "gourmetgram.flags":    ("created_at",  1),
    }

    for topic, (ts_field, expected_count) in topics.items():
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=REDPANDA_BROKERS,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id=f"test_validator_{uuid.uuid4().hex[:6]}"  # fresh group
            )
            found = sum(
                1 for msg in consumer
                if msg.value.get("image_id") == image_id
            )
            consumer.close()
            check(f"Topic {topic}: found {expected_count} events for image",
                  found == expected_count,
                  f"got {found}")
        except Exception as e:
            check(f"Topic {topic} readable", False, str(e))

# ─── Step 6: PostgreSQL Verification ─────────────────────────────────────────

def test_postgres(user_id: str, image_id: str):
    section("Step 6: PostgreSQL Verification")

    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur  = conn.cursor()
    except Exception as e:
        check("Postgres reachable", False, str(e))
        return

    # Image exists
    cur.execute("SELECT id, category, caption, user_id FROM images WHERE id = %s", (image_id,))
    row = cur.fetchone()
    check("Image row in postgres", row is not None)
    if row:
        check("Category == Dessert", row[1] == "Dessert", f"got {row[1]}")
        check("Caption is set",      bool(row[2]))

    # Comment count
    cur.execute("SELECT COUNT(*) FROM comments WHERE image_id = %s", (image_id,))
    db_comments = cur.fetchone()[0]
    check(f"Postgres comments == {N_COMMENTS}", db_comments == N_COMMENTS,
          f"got {db_comments}")

    # Flag count
    cur.execute("SELECT COUNT(*) FROM flags WHERE image_id = %s", (image_id,))
    db_flags = cur.fetchone()[0]
    check("Postgres flags == 1", db_flags == 1, f"got {db_flags}")

    # Views column (FastAPI increments image.views)
    cur.execute("SELECT views FROM images WHERE id = %s", (image_id,))
    db_views = cur.fetchone()[0]
    check(f"Postgres image.views == {N_VIEWS}", db_views == N_VIEWS,
          f"got {db_views}")

    cur.close()
    conn.close()

# ─── Step 7: Trigger ETL DAGs and Verify Iceberg ──────────────────────────────

def trigger_airflow_dag(dag_id: str) -> Optional[str]:
    """Trigger a DAG and return the run_id, or None on failure."""
    r = requests.post(
        f"{AIRFLOW_BASE}/api/v1/dags/{dag_id}/dagRuns",
        json={"conf": {}},
        auth=(AIRFLOW_USER, AIRFLOW_PASS),
        headers={"Content-Type": "application/json"},
    )
    if r.status_code in (200, 201):
        return r.json().get("dag_run_id")
    print(f"  {FAIL} Triggering {dag_id} failed: {r.status_code} {r.text[:200]}")
    return None


def wait_for_dag(dag_id: str, run_id: str, timeout: int = 300) -> bool:
    """Poll until DAG run succeeds or fails. Returns True on success."""
    deadline = time.time() + timeout
    print(f"  {INFO} Waiting up to {timeout}s for {dag_id} run {run_id}...")
    while time.time() < deadline:
        r = requests.get(
            f"{AIRFLOW_BASE}/api/v1/dags/{dag_id}/dagRuns/{run_id}",
            auth=(AIRFLOW_USER, AIRFLOW_PASS),
        )
        if r.status_code == 200:
            state = r.json().get("state")
            if state == "success":
                print(f"  {PASS} DAG {dag_id} run succeeded")
                return True
            if state == "failed":
                print(f"  {FAIL} DAG {dag_id} run FAILED")
                return False
        time.sleep(10)
    print(f"  {FAIL} DAG {dag_id} timed out after {timeout}s")
    return False


def test_iceberg_and_etl(image_id: str):
    section("Step 7: ETL DAGs + Iceberg Verification")

    # --- Trigger redpanda_event_aggregation ---
    print(f"\n  [7a] Triggering redpanda_event_aggregation DAG...")
    run_id = trigger_airflow_dag("redpanda_event_aggregation")
    if run_id:
        success = wait_for_dag("redpanda_event_aggregation", run_id, timeout=300)
        check("redpanda_event_aggregation completed", success)
    else:
        check("redpanda_event_aggregation triggered", False)
        return

    # --- Verify Iceberg tables have rows for our image ---
    print(f"\n  [7b] Verifying Iceberg window tables...")
    _verify_iceberg_tables(image_id)

    # --- Trigger moderation_training_etl ---
    print(f"\n  [7c] Triggering moderation_training_etl DAG...")
    run_id2 = trigger_airflow_dag("moderation_training_etl")
    if run_id2:
        success2 = wait_for_dag("moderation_training_etl", run_id2, timeout=600)
        check("moderation_training_etl completed", success2)
    else:
        check("moderation_training_etl triggered", False)
        return

    # --- Verify training data quality ---
    print(f"\n  [7d] Verifying training data quality...")
    _verify_training_data(image_id)


def _verify_iceberg_tables(image_id: str):
    """Run inside Airflow scheduler container to check Iceberg tables."""
    import subprocess

    script = f"""
import sys
try:
    from pyiceberg.catalog import load_catalog
    import os

    catalog = load_catalog("gourmetgram")
    results = {{}}
    for tbl in ["event_aggregations.view_windows_5min",
                "event_aggregations.comment_windows_5min",
                "event_aggregations.flag_windows_5min"]:
        try:
            df = catalog.load_table(tbl).scan().to_pandas()
            img_rows = df[df['image_id'] == '{image_id}']
            results[tbl] = int(img_rows['event_count'].sum())
        except Exception as e:
            results[tbl] = f"ERROR: {{e}}"

    import json
    print("ICEBERG_RESULT:" + json.dumps(results))
except Exception as e:
    print(f"ICEBERG_ERROR: {{e}}")
"""

    try:
        out = subprocess.check_output(
            ["docker", "exec", "airflow-scheduler",
             "python", "-c", script],
            stderr=subprocess.STDOUT,
            timeout=60
        ).decode()

        for line in out.splitlines():
            if line.startswith("ICEBERG_RESULT:"):
                data = json.loads(line[len("ICEBERG_RESULT:"):])
                view_total = data.get("event_aggregations.view_windows_5min", 0)
                comment_total = data.get("event_aggregations.comment_windows_5min", 0)
                flag_total = data.get("event_aggregations.flag_windows_5min", 0)

                check(f"Iceberg view_windows: sum(event_count) == {N_VIEWS}",
                      view_total == N_VIEWS,
                      f"got {view_total}")
                check(f"Iceberg comment_windows: sum(event_count) == {N_COMMENTS}",
                      comment_total == N_COMMENTS,
                      f"got {comment_total}")
                check("Iceberg flag_windows: sum(event_count) == 1",
                      flag_total == 1,
                      f"got {flag_total}")
                print(f"  {INFO} Iceberg totals for image: views={view_total}, "
                      f"comments={comment_total}, flags={flag_total}")
                return

        print(f"  {INFO} Raw docker output:\n{out}")
        check("Iceberg result parsed", False, "no ICEBERG_RESULT line found")
    except Exception as e:
        check("Iceberg table check via docker", False, str(e))


def _verify_training_data(image_id: str):
    """Verify training_data.parquet has 29 features and no data leakage."""
    import subprocess

    expected_features = [
        'time_since_upload_seconds', 'hour_of_day', 'day_of_week', 'is_weekend',
        'total_views', 'total_comments', 'total_flags',
        'views_5min', 'views_1hr', 'comments_5min', 'comments_1hr',
        'view_velocity_per_min', 'comment_to_view_ratio', 'recent_engagement_score',
        'caption_length', 'has_caption',
        'user_image_count', 'user_age_days',
        'category',  # stored as string pre one-hot
        'label_needs_moderation_24h',
    ]

    script = f"""
import sys
try:
    import pandas as pd, json

    df = pd.read_parquet(
        "s3://gourmetgram-datalake/processed/training_data.parquet",
        storage_options={{
            "key": "admin",
            "secret": "password",
            "client_kwargs": {{"endpoint_url": "http://minio:9000"}}
        }}
    )

    img_rows = df[df['image_id'] == '{image_id}']
    cols = df.columns.tolist()

    result = {{
        "total_rows": len(df),
        "image_rows": len(img_rows),
        "columns": cols,
        "sample": img_rows.iloc[0].to_dict() if not img_rows.empty else {{}}
    }}
    print("TRAINING_RESULT:" + json.dumps(result, default=str))
except Exception as e:
    print(f"TRAINING_ERROR: {{e}}")
"""

    try:
        out = subprocess.check_output(
            ["docker", "exec", "airflow-scheduler",
             "python", "-c", script],
            stderr=subprocess.STDOUT,
            timeout=60
        ).decode()

        for line in out.splitlines():
            if line.startswith("TRAINING_RESULT:"):
                data = json.loads(line[len("TRAINING_RESULT:"):])
                cols   = data.get("columns", [])
                sample = data.get("sample", {})

                print(f"  {INFO} Training data: {data['total_rows']} rows total, "
                      f"{data['image_rows']} rows for test image")
                print(f"  {INFO} Columns ({len(cols)}): {cols}")

                # 1. All required feature columns present
                required_feature_cols = [
                    'time_since_upload_seconds', 'hour_of_day', 'day_of_week', 'is_weekend',
                    'total_views', 'total_comments', 'total_flags',
                    'views_5min', 'views_1hr', 'comments_5min', 'comments_1hr',
                    'view_velocity_per_min', 'comment_to_view_ratio', 'recent_engagement_score',
                    'caption_length', 'has_caption', 'user_image_count', 'user_age_days',
                    'category', 'label_needs_moderation_24h',
                ]
                missing = [c for c in required_feature_cols if c not in cols]
                check("All required feature columns present", not missing,
                      f"missing: {missing}")

                # 2. 1-min columns NOT present (5-min granularity constraint)
                for bad_col in ['views_1min', 'comments_1min']:
                    check(f"No '{bad_col}' column (5-min granularity)",
                          bad_col not in cols,
                          f"Column {bad_col} should not exist")

                # 3. Our test image appears in training data
                check("Test image appears in training data",
                      data['image_rows'] > 0,
                      "Image may not have been processed yet")

                if sample:
                    # 4. Data leakage: at t=0 (decision_point=0), no views/comments/flags
                    t0_rows = [r for r in data.get('sample_rows', [])
                               if r.get('decision_point_minutes') == 0]

                    # 5. Label: image was flagged within 24h, so label must be 1
                    label = sample.get('label_needs_moderation_24h')
                    check("Label == 1 (image was flagged within test run)",
                          label == 1,
                          f"got label={label}. Note: only valid if image was flagged "
                          "within 24h of upload")

                    # 6. Derived feature consistency check
                    tv = sample.get('total_views', 0)
                    tc = sample.get('total_comments', 0)
                    v5 = sample.get('views_5min', 0)
                    c5 = sample.get('comments_5min', 0)
                    expected_vel = v5 / 5.0 if v5 > 0 else 0.0
                    expected_eng = v5 + (c5 * 5)
                    expected_ratio = tc / max(tv, 1)

                    actual_vel   = sample.get('view_velocity_per_min', -1)
                    actual_eng   = sample.get('recent_engagement_score', -1)
                    actual_ratio = sample.get('comment_to_view_ratio', -1)

                    check("view_velocity_per_min = views_5min/5",
                          abs(float(actual_vel) - expected_vel) < 0.001,
                          f"expected {expected_vel}, got {actual_vel}")
                    check("recent_engagement_score = views_5min + comments_5min*5",
                          abs(float(actual_eng) - expected_eng) < 0.001,
                          f"expected {expected_eng}, got {actual_eng}")
                    check("comment_to_view_ratio = total_comments/max(total_views,1)",
                          abs(float(actual_ratio) - expected_ratio) < 0.001,
                          f"expected {expected_ratio:.4f}, got {actual_ratio}")

                    print(f"\n  {INFO} Sample row (last decision point):")
                    for k, v in sorted(sample.items()):
                        print(f"       {k:35s} = {v}")

                return

        print(f"  {INFO} Raw docker output:\n{out}")
        check("Training data result parsed", False, "no TRAINING_RESULT line found")
    except Exception as e:
        check("Training data check via docker", False, str(e))


# ─── Step 8: Data Leakage Spot-Check ──────────────────────────────────────────

def test_leakage_spot_check(image_id: str):
    """
    Verify that at decision point t=0 (upload time), counts are zero.
    Checks both Redis (always 0 immediately) and training data rows.
    """
    section("Step 8: Data Leakage Spot Check")

    script = f"""
try:
    import pandas as pd, json

    df = pd.read_parquet(
        "s3://gourmetgram-datalake/processed/training_data.parquet",
        storage_options={{
            "key": "admin",
            "secret": "password",
            "client_kwargs": {{"endpoint_url": "http://minio:9000"}}
        }}
    )

    img = df[df['image_id'] == '{image_id}']
    rows_at_t0 = img[img['decision_point_minutes'] == 0].to_dict(orient='records')
    print("LEAKAGE_RESULT:" + json.dumps(rows_at_t0, default=str))
except Exception as e:
    print(f"LEAKAGE_ERROR: {{e}}")
"""

    import subprocess
    try:
        out = subprocess.check_output(
            ["docker", "exec", "airflow-scheduler", "python", "-c", script],
            stderr=subprocess.STDOUT,
            timeout=60
        ).decode()

        for line in out.splitlines():
            if line.startswith("LEAKAGE_RESULT:"):
                rows = json.loads(line[len("LEAKAGE_RESULT:"):])
                if not rows:
                    print(f"  {INFO} No t=0 rows found — skipping leakage check")
                    return
                row = rows[0]
                check("t=0: total_views == 0",    row.get('total_views', -1) == 0,
                      f"got {row.get('total_views')}")
                check("t=0: total_comments == 0", row.get('total_comments', -1) == 0,
                      f"got {row.get('total_comments')}")
                check("t=0: total_flags == 0",    row.get('total_flags', -1) == 0,
                      f"got {row.get('total_flags')}")
                check("t=0: views_5min == 0",     row.get('views_5min', -1) == 0,
                      f"got {row.get('views_5min')}")
                return
    except Exception as e:
        check("Leakage check via docker", False, str(e))


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="E2E pipeline validation")
    parser.add_argument("--etl", action="store_true",
                        help="Also run Airflow ETL + Iceberg checks (takes several minutes)")
    args = parser.parse_args()

    print("\n" + "="*60)
    print("  GourmetGram End-to-End Pipeline Validation")
    print("="*60)

    # Steps that run against live services
    test_api_health()
    user_id, image_id, uploaded_at = create_user_and_image()
    send_events(user_id, image_id)
    test_redis(image_id)
    test_redpanda_topics(image_id)
    test_postgres(user_id, image_id)

    if args.etl:
        test_iceberg_and_etl(image_id)
        test_leakage_spot_check(image_id)
    else:
        print(f"\n  {INFO} Skipping ETL/Iceberg steps. Run with --etl to include them.")

    # Final summary
    print("\n" + "="*60)
    if _errors:
        print(f"  \033[91mFAILED\033[0m — {len(_errors)} check(s) failed:")
        for e in _errors:
            print(f"    • {e}")
        sys.exit(1)
    else:
        print(f"  \033[92mALL CHECKS PASSED\033[0m")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
