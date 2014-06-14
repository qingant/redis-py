
from .server import current_server as server
from .storage import key_space
from redis.common.objects import RedisStringObject
from redis.common.utils import abort
import time


@server.command('set')
def set_handler(argv):
    '''
    Set the string value of a key

    .. code::
        SET key value [EX seconds] [PX milliseconds] [NX|XX]

    :param EX: Set the specified expire time, in seconds.
    :param PX: Set the specified expire time, in milliseconds.
    :param NX: Only set the key if it does not already exist.
    :param XX: Only set the key if it already exist.

    '''
    if len(argv) < 3:
        abort(message="wrong number of arguments for 'set' command")

    key, value = argv[1], argv[2]
    expire_time = None
    nx = False
    xx = False

    cur_index = 3
    while cur_index < len(argv):
        argname = argv[cur_index].lower()
        if argname == b'ex':
            if cur_index == len(argv) - 1:
                abort(message='syntax error')
            if expire_time is None:
                expire_time = time.time()
            cur_index += 1
            try:
                expire_time += int(argv[cur_index])
            except:
                abort(message='syntax error')
        elif argname == b'px':
            if cur_index == len(argv) - 1:
                abort(message='syntax error')
            if expire_time is None:
                expire_time = time.time()
            cur_index += 1
            try:
                expire_time += int(argv[cur_index]) / 1000.0
            except:
                abort(message='syntax error')
        elif argname == b'nx':
            nx = True
        elif argname == b'xx':
            xx = True
        else:
            abort(message='syntax error')
        cur_index += 1

    if nx and xx:
        abort(message='syntax error')

    if nx and key in key_space:
        return None
    if xx and key not in key_space:
        return None

    key_space[key] = RedisStringObject(value, expire_time=expire_time)
    return True


@server.command('setbit')
def setbit_handler(argv):
    pass


@server.command('get')
def get_handler(argv):
    '''
    Get the value of key. If the key does not exist the special value nil is returned.
    An error is returned if the value stored at key is not a string, because GET only handles string values.

    .. code::
        GET key

    '''
    if len(argv) != 2:
        abort(message="wrong number of arguments for 'get' command")

    key = argv[1]
    if key not in key_space:
        return None

    if key_space[key].expired():
        del key_space[key]
        return None

    if not isinstance(key_space[key], RedisStringObject):
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')
    return key_space[key].value


import bitarray


@server.command('getbit')
def getbit_handler(argv):
    '''
    Returns the bit value at offset in the string value stored at key.

    When offset is beyond the string length, the string is assumed to be a contiguous space with 0 bits.
    When key does not exist it is assumed to be an empty string, so offset is always out of range and the
    value is also assumed to be a contiguous space with 0 bits.

    .. code::
        GETBIT key offset

    '''

    if len(argv) != 3:
        abort(message="wrong number of arguments for 'getbit' command")

    key, offset = argv[1], argv[2]

    try:
        offset = int(offset)
    except ValueError:
        abort(message='bit offset is not an integer or out of range')

    if key not in key_space:
        return 0

    if not isinstance(key_space[key], RedisStringObject):
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    ba = bitarray.bitarray()
    ba.frombytes(key_space[key].value)

    try:
        return int(ba[offset])
    except IndexError:
        return 0


@server.command('getrange')
def getrange_handler(argv):
    '''
    Returns the substring of the string value stored at key, determined by the offsets start and end
    (both are inclusive). Negative offsets can be used in order to provide an offset starting from the
    end of the string. So -1 means the last character, -2 the penultimate and so forth.

    The function handles out of range requests by limiting the resulting range to the actual length of
    the string.

    .. code::
        GETRANGE key start end

    '''
    if len(argv) != 4:
        abort(message="wrong number of arguments for 'getrange' command")

    key, start, end = argv[1], argv[2], argv[3]

    try:
        start = int(start)
        end = int(end)
    except ValueError:
        abort(message='value is not an integer or out of range')

    if key not in key_space:
        return ""

    if not isinstance(key_space[key], RedisStringObject):
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if end == -1:
        end = len(key_space[key].value)
    return key_space[key].value[start:end + 1]


@server.command('getset')
def getset_handler(argv):
    '''
    Atomically sets key to value and returns the old value stored at key. Returns an error when key
    exists but does not hold a string value.

    .. code::
        GETSET key value

    '''
    if len(argv) != 3:
        abort(message="wrong number of arguments for 'getrange' command")

    key, value = argv[1], argv[2]

    if key in key_space and not isinstance(key_space[key], RedisStringObject):
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if key in key_space:
        orig_value = key_space[key].value
        key_space[key].value = value
    else:
        orig_value = None
        key_space[key] = RedisStringObject(value)

    return orig_value
