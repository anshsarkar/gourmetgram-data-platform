#!/usr/bin/env python3
import logging
import time
from typing import Optional, List, Dict, Any
import redis
from redis.exceptions import RedisError, ConnectionError as RedisConnectionError

from config import config

logger = logging.getLogger(__name__)


class RedisClient:
    def __init__(self):
        self.client: Optional[redis.Redis] = None
        self.connection_pool: Optional[redis.ConnectionPool] = None

    def connect(self, max_retries: int = 5, retry_delay: int = 2):
        for attempt in range(1, max_retries + 1):
            try:
                # Create connection pool
                self.connection_pool = redis.ConnectionPool(
                    host=config.redis_host,
                    port=config.redis_port,
                    db=config.redis_db,
                    decode_responses=True,  # Automatically decode responses to strings
                    max_connections=10
                )

                # Create Redis client
                self.client = redis.Redis(connection_pool=self.connection_pool)

                # Test connection
                self.client.ping()

                logger.info(f"Connected to Redis at {config.redis_host}:{config.redis_port}")
                return True

            except RedisConnectionError as e:
                logger.warning(f"Redis connection attempt {attempt}/{max_retries} failed: {e}")
                if attempt < max_retries:
                    time.sleep(retry_delay)
                else:
                    logger.error("Failed to connect to Redis after all retries")
                    raise

    def close(self):
        if self.connection_pool:
            self.connection_pool.disconnect()
            logger.info("Redis connection closed")

    # Add to sorted set and set TTL on the key
    def zadd_with_ttl(self, key: str, score: float, value: str, ttl: int):
        try:
            pipeline = self.client.pipeline()
            pipeline.zadd(key, {value: score})
            pipeline.expire(key, ttl)
            pipeline.execute()
            return True
        except RedisError as e:
            logger.error(f"Error in zadd_with_ttl for key {key}: {e}")
            return False

    # Remove elements from sorted set by score range
    def zremrangebyscore(self, key: str, min_score: float, max_score: float):
        try:
            return self.client.zremrangebyscore(key, min_score, max_score)
        except RedisError as e:
            logger.error(f"Error in zremrangebyscore for key {key}: {e}")
            return 0

    # Count elements in sorted set within score range
    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        try:
            return self.client.zcount(key, min_score, max_score)
        except RedisError as e:
            logger.error(f"Error in zcount for key {key}: {e}")
            return 0

    # Increment counter and set TTL
    def incr_with_ttl(self, key: str, ttl: int) -> int:
        try:
            pipeline = self.client.pipeline()
            pipeline.incr(key)
            pipeline.expire(key, ttl)
            result = pipeline.execute()
            return result[0]
        except RedisError as e:
            logger.error(f"Error in incr_with_ttl for key {key}: {e}")
            return 0

    # Increment counter (no TTL)
    def incr(self, key: str) -> int:
        
        try:
            return self.client.incr(key)
        except RedisError as e:
            logger.error(f"Error in incr for key {key}: {e}")
            return 0
    
    # Get value by key
    def get(self, key: str) -> Optional[str]:
        
        try:
            return self.client.get(key)
        except RedisError as e:
            logger.error(f"Error in get for key {key}: {e}")
            return None

    # Get multiple values by keys
    def mget(self, keys: List[str]) -> List[Optional[str]]:
        try:
            return self.client.mget(keys)
        except RedisError as e:
            logger.error(f"Error in mget for keys {keys}: {e}")
            return [None] * len(keys)

    # Get all fields and values in a hash
    def hgetall(self, key: str) -> Dict[str, Any]:
        try:
            return self.client.hgetall(key)
        except RedisError as e:
            logger.error(f"Error in hgetall for key {key}: {e}")
            return {}

    # Set multiple fields in a hash
    def hset(self, key: str, mapping: Dict[str, Any]):
        try:
            return self.client.hset(key, mapping=mapping)
        except RedisError as e:
            logger.error(f"Error in hset for key {key}: {e}")
            return 0

    # Find keys matching pattern (use sparingly in production)
    def keys(self, pattern: str) -> List[str]:
        try:
            return self.client.keys(pattern)
        except RedisError as e:
            logger.error(f"Error in keys for pattern {pattern}: {e}")
            return []


# Global Redis client instance
redis_client = RedisClient()
