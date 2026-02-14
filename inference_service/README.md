# Inference Service

Listens for moderation requests on Kafka, runs inference on the flagged image, and stores the decision in PostgreSQL. Supports two modes: **heuristic** (rule-based, works out of the box) and **model** (trained SGDClassifier loaded from MinIO).

## What It Does

1. **Consumes** moderation requests from `gourmetgram.moderation_requests` (published by the stream consumer when viral/suspicious thresholds are crossed)
2. **Fetches features** for the image from Redis (rolling window counts) and PostgreSQL (metadata)
3. **Builds a 26-dimensional feature vector** from those features
4. **Runs inference** — either heuristic or model-based depending on the `USE_MODEL` flag
5. **Stores the decision** in the `moderation_decisions` table in PostgreSQL

## Inference Modes

### Heuristic Mode (default — `USE_MODEL = False`)

Rule-based scoring that works without any trained model. Looks at risk signals:
- Flags reported on the image → +0.4 score
- Comment spam (10+ comments/min) → +0.3 score
- High comment-to-view ratio (>0.5) → +0.2 score
- Bot-like pattern (many comments, few views) → +0.3 score
- Viral content (100+ views/5min) → small positive contribution

### Model Mode (`USE_MODEL = True`)

Uses a trained `SGDClassifier` loaded from MinIO. The model is checked every 30 seconds — if a new version is detected via `metadata.json`, it is downloaded and hot-swapped. Falls back to heuristic if no model is found.

**To switch to model mode:**
1. Train the model and upload artifacts to MinIO (see `model_training/`)
2. Edit `moderator.py` line 35: change `USE_MODEL = False` → `USE_MODEL = True`
3. Rebuild and restart the container

## Input

- **Kafka** → `gourmetgram.moderation_requests` topic (message contains `image_id` and `trigger`)
- **Redis** → rolling window counts (views/comments per 5min and 1hr) for the image
- **PostgreSQL** → image metadata (category, caption, upload time, user info)
- **MinIO** → model artifacts (`model.joblib`, `scaler.joblib`, `encoder.joblib`) — model mode only

## Output / What Gets Stored

| Data | Storage |
|------|---------|
| Moderation decisions | PostgreSQL → `moderation_decisions` table |

**`moderation_decisions` table columns:**

| Column | Description |
|--------|-------------|
| `id` | UUID primary key |
| `image_id` | The image that was evaluated |
| `inference_mode` | `heuristic`, `model`, or `heuristic_fallback` |
| `moderation_probability` | Float 0–1, likelihood the image needs moderation |
| `recommendation` | `SAFE` (<0.3), `REVIEW` (0.3–0.7), or `FLAG` (>0.7) |
| `trigger_type` | What triggered the request: `viral` or `suspicious` |
| `created_at` | Timestamp |

## Feature Vector (26 dimensions)

| Index | Feature | Type |
|-------|---------|------|
| 0–2 | `time_since_upload_seconds`, `hour_of_day`, `day_of_week` | Numeric |
| 3 | `is_weekend` | Binary |
| 4–10 | `views_5min`, `views_1hr`, `comments_5min`, `comments_1hr`, velocity, ratio, engagement | Numeric |
| 11 | `caption_length` | Numeric |
| 12 | `has_caption` | Binary |
| 13–14 | `user_image_count`, `user_age_days` | Numeric |
| 15–25 | Category one-hot (11 Food-11 classes) | Binary |

## Dependencies

```
kafka-python       # Kafka consumer
redis              # feature fetching from Redis
psycopg2-binary    # PostgreSQL for decisions + features
boto3              # MinIO model artifact downloads
scikit-learn       # SGDClassifier inference
joblib             # model deserialization
numpy              # feature vector operations
```

## Configuration

| Env Var | Default | Description |
|---------|---------|-------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `MODERATION_TOPIC` | `gourmetgram.moderation_requests` | Topic to consume |
| `REDIS_HOST` / `REDIS_PORT` | `localhost:6379` | Redis for features |
| `DATABASE_URL` | `postgresql://...` | PostgreSQL |
| `S3_ENDPOINT_URL` | `http://minio:9000` | MinIO endpoint |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | `admin/password` | MinIO credentials |
| `MODEL_CHECK_INTERVAL` | `30` | Seconds between MinIO model checks |
| `MODERATION_THRESHOLD` | `0.7` | Probability above which → FLAG |

## Files

- `main.py` — entry point, Kafka consumer loop, stores decisions to Postgres
- `moderator.py` — `USE_MODEL` toggle, orchestrates the pipeline
- `feature_fetcher.py` — fetches raw features from Redis + PostgreSQL
- `feature_constructor.py` — builds the 26-dim feature vector
- `heuristic_predictor.py` — rule-based scoring (no model needed)
- `model_manager.py` — polls MinIO for new models, loads artifacts, runs `predict_proba()`
- `config.py` — all configuration via env vars, feature names, food categories
