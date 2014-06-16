
import time

key_space = {}


class RedisDatabase:

    def __init__(self, idnum=0):
        self._idnum = idnum
        self.key_space = {}

    @property
    def idnum(self):
        return self._idnum

    def flush(self):
        self.key_space.clear()
