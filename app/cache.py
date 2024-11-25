from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
from redis import asyncio as aioredis
import os
import redis
import logging

def init_cache():
    redis = aioredis.from_url(os.getenv("REDIS_URL"))
    FastAPICache.init(RedisBackend(redis), prefix="application-cache", expire=86400)

def connect_to_redis():
    logging.info("Establishing connection to Redis")
    return redis.from_url(os.getenv("REDIS_URL") + "?decode_responses=True&health_check_interval=2")


def delete_cache_keys(keys):
    redis_connection = connect_to_redis()
    logging.info("Redis: " + str(len(redis_connection.keys(keys))) + " community keys found")
    count = 0
    pipe = redis_connection.pipeline()
    for key in redis_connection.scan_iter(keys):
        pipe.delete(key)
        count += 1
    pipe.execute()
    logging.info("Deleted cache for keys " + keys + ". Close Redis Connection")
    return redis_connection.close()
