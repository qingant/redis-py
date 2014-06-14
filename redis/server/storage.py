
import time

key_space = {}


class StorageNode(object):

    def __init__(self, value, expire_time=None):
        self._value = value
        self.expire_time = expire_time

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value

    def expired(self):
        if self.expire_time is not None:
            return True if time.time() > self.expire_time else False
        return False
