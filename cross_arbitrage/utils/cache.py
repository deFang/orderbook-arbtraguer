import functools
import logging
import time


class ExpireCache:
    def __init__(self, expire_seconds: float):
        self.expire_time = int(expire_seconds * 1e9)
        self.cache = {}

    def get(self, key):
        if key in self.cache:
            logging.debug(f"hit cache: {key}")
            if time.monotonic_ns() < self.cache[key][1]:
                return self.cache[key][0]
            else:
                logging.debug(f"cache expired: {key}")
                del self.cache[key]
        return None

    def set(self, key, value):
        self.cache[key] = (value, time.monotonic_ns() + self.expire_time)

    def delete(self, key):
        if key in self.cache:
            del self.cache[key]


def expire_cache(expire_seconds: float):
    def decorator(func):
        cache = ExpireCache(expire_seconds)

        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = (args, tuple(kwargs.items()))
            value = cache.get(key)
            if value is None:
                value = func(*args, **kwargs)
                cache.set(key, value)
            return value

        return wrapper

    return decorator


def test_expire_cache():
    @expire_cache(1)
    def test(a, b):
        return a + b

    assert test(1, 2) == 3
    assert test(1, 2) == 3
    time.sleep(2)
    assert test(1, 2) == 3
