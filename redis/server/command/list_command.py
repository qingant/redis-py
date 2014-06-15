from .server import current_server as server
from .storage import key_space
from redis.common.objects import RedisListObject
from redis.common.utils import abort, close_connection
from redis.common.utils import nargs_greater_equal


@server.command('lindex', nargs=2)
def lindex_handler(argv):
    '''
    Returns the element at index index in the list stored at key. The index is zero-based, so 0
    means the first element, 1 the second element and so on. Negative indices can be used to designate
    elements starting at the tail of the list. Here, -1 means the last element, -2 means the penultimate
    and so forth.

    When the value at key is not a list, an error is returned.

    .. code::
        LINDEX key index

    :return: the requested element, or nil when index is out of range.
    :rtype: str

    '''

    key, index = argv[1], argv[2]
    try:
        index = int(index)
    except ValueError:
        abort(message='value is not an integer or out of range')

    if key not in key_space:
        return None

    if not isinstance(key_space[key], RedisListObject):
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    obj = key_space[key]

    try:
        return obj.value[index]
    except IndexError:
        return None


@server.command('lpush', nargs=nargs_greater_equal(2))
def lpush_handler(argv):
    '''
    Insert all the specified values at the head of the list stored at key. If key does not exist,
    it is created as empty list before performing the push operations. When key holds a value that
    is not a list, an error is returned.

    It is possible to push multiple elements using a single command call just specifying multiple
    arguments at the end of the command. Elements are inserted one after the other to the head of
    the list, from the leftmost element to the rightmost element. So for instance the command LPUSH
    mylist a b c will result into a list containing c as first element, b as second element and a
    as third element.

    .. code::
        LPUSH key value [value ...]

    :return: the length of the list after the push operations.
    :rtype: int

    '''

    key, values = argv[1], argv[2:]

    if key not in key_space:
        obj = RedisListObject()
    else:
        obj = key_space[key]
        if not isinstance(obj, RedisListObject):
            abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    for value in values:
        obj.push(value)

    return len(obj)
