
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

    def serialize(self):
        '''
        Convert to a REdis Serialization Protocol.


        '''
        raise NotImplementedError()


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

    def get_range(self, start=None, stop=None):
        val = self.get_bytes()
        return RedisStringObject(val[start:stop + 1])

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

    def serialize(self):
        from .proto import RedisBulkStringSerializationObject
        return RedisBulkStringSerializationObject(self)


class RedisListObject(RedisObject):

    def __init__(self, value=[], expire_time=None):
        if isinstance(value, types.GeneratorType):
            value = list(value)
        elif isinstance(value, RedisListObject):
            value = value.value
        else:
            raise ValueError('Value should be a list or RedisListObject')
        super(RedisListObject, self).__init__(value, expire_time)

    def push(self, *value):
        for val in value:
            self.value.insert(0, val)

    def pop(self, index=0):
        return self.value.pop(index)

    def insert(self, index, value):
        if index >= len(self.value) or index < 0:
            raise IndexError('Out of range')
        return self.value.insert(index, value)

    def append(self, value):
        self.value.append(value)

    def __len__(self):
        return len(self.value)

    def __getitem__(self, index):
        return self.value[index]

    def __setitem__(self, index, value):
        self.value[index] = value

    def splice(self, begin=None, end=None, step=None):
        return RedisListObject(self.value[begin:end:step])

    def __iter__(self):
        return self.value.__iter__()

    def reverse(self):
        return self.value.reverse()

    def remove(self, value):
        self.value.remove(value)

    def index(self, value):
        return self.value.index(value)

    def serialize(self):
        from .proto import RedisListSerializationObject
        return RedisListSerializationObject(self)
