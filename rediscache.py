import json
import zlib

from datetime import datetime, timedelta
from redis import StrictRedis

# https://github.com/kjam/wswp/blob/master/code/chp3/rediscache.py
class RedisCache:
    """ Initialization components:
            client: a Redis client connected to the key-value database for
                the webcrawling cache (if not set, a localhost:6379
                default connection is used).
            encoding (str): character encoding for serialization
            compress (bool): boolean indicating whether compression with zlib should be used
    """
    def __init__(self, client=None, encoding='utf-8', compress=False):
        self.client = (
            StrictRedis(host='localhost', 
                        port=6379, 
                        db=0)
            if client is None else client
        )

        self.encoding = encoding
        self.compress = compress

    def __getitem__(self, key):
        """Load data from Redis for given URL"""
        record = self.client.get(key)
        if record:
            if self.compress:
                record = zlib.decompress(record)
            return record.decode(self.encoding)
        else:
            # URL has not yet been cached
            raise KeyError(key + ' does not exist')

    def __setitem__(self, key, val):
        """Save value to Redis for given key"""
        if not key or not val:
            return

        data = val.encode(self.encoding)
        if self.compress:
            data = zlib.compress(data)
        #self.client.setex(key, timedelta(days=15), data) # Expires in 2 days
        self.client.set(key, data)
