
import time
from decimal import Decimal
import types


class RedisObject:

    def __init__(self, value, expire_time=None):
        self._expire_time = expire_time
        self._value = value

    def __str__(self):
        return str(self.value)

    def expired(self):
        return True if self.expire_time is not None and time.time() > self.expire_time else False

    @property
    def expire_time(self):
        return self._expire_time

    @expire_time.setter
    def expire_time(self, value):
        self._expire_time = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value


class RedisStringObject(RedisObject):

    def __init__(self, value=b'', expire_time=None):
        if isinstance(value, str):
            value = value.encode()
        elif not isinstance(value, bytes):
            value = str(value).encode()
        super(RedisStringObject, self).__init__(value, expire_time)

    def __str__(self):
        if isinstance(self.value, str):
            return self.value
        elif isinstance(self.value, bytes):
            return self.value.decode()
        else:
            return str(self.value)

    def __len__(self):
        return len(self.get_bytes())

    def get_bytes(self):
        if not isinstance(self.value, bytes):
            return str(self.value).encode()
        return self.value

    def get_integer(self):
        if isinstance(self.value, Decimal):
            raise ValueError('Value is a Decimal')
        return int(self.value)

    def get_decimal(self):
        if isinstance(self.value, bytes):
            return Decimal(self.value.decode())
        return Decimal(str(self.value))

    def get_float(self):
        return float(self.value)


class RedisListObject(RedisObject):

    def __init__(self, value=[], expire_time=None):
        if isinstance(value, types.GeneratorType):
            value = list(value)
        super(RedisStringObject, self).__init__(value, expire_time)

    def push(self, value):
        self.value.append(value)

    def __len__(self):
        return len(self.value)
