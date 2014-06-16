from redis.server import current_server as server
from redis.common.objects import RedisListObject
from redis.common.utils import abort, close_connection
from redis.common.utils import nargs_greater_equal
from redis.common.utils import get_object


@server.command('lindex', nargs=2)
def lindex_handler(client, argv):
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

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return None
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    try:
        return obj[index]
    except IndexError:
        return None


@server.command('lpush', nargs=nargs_greater_equal(2))
def lpush_handler(client, argv):
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

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        obj = RedisListObject()
        client.db.key_space[key] = obj
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    # for value in values:
    obj.push(*values)

    return len(obj)


@server.command('lpushx', nargs=2)
def lpushx_handler(client, argv):
    '''
    Inserts value at the head of the list stored at key, only if key already exists and holds a list.
    In contrary to LPUSH, no operation will be performed when key does not yet exist.

    .. code::
        LPUSHX key value

    :return: the length of the list after the push operation.
    :rtype: int

    '''

    key, value = argv[1], argv[2]

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return 0
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    obj.push(value)
    return len(obj)


@server.command('lrange', nargs=3)
def lrange_handler(client, argv):
    '''
    Returns the specified elements of the list stored at key. The offsets start and stop are zero-based
    indexes, with 0 being the first element of the list (the head of the list), 1 being the next element
    and so on.

    These offsets can also be negative numbers indicating offsets starting at the end of the list. For
    example, -1 is the last element of the list, -2 the penultimate, and so on.

    .. code::
        LRANGE key start stop

    :return: list of elements in the specified range.
    :rtype: list

    '''

    key, start, stop = argv[1], argv[2], argv[3]
    try:
        start = int(start)
        stop = int(stop)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return []
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if start < 0:
        start = len(obj) + start
    if stop < 0:
        stop = len(obj) + stop

    return obj[start:stop + 1]


@server.command('lrem', nargs=3)
def lrem_handler(client, argv):
    '''
    Removes the first count occurrences of elements equal to value from the list stored at key. The
    count argument influences the operation in the following ways:

    * count > 0: Remove elements equal to value moving from head to tail.
    * count < 0: Remove elements equal to value moving from tail to head.
    * count = 0: Remove all elements equal to value.

    For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list
    stored at list.

    Note that non-existing keys are treated like empty lists, so when key does not exist, the command
    will always return 0.

    .. code::
        LREM key count value

    :return: the number of removed elements.
    :rtype: int

    '''

    key, count, value = argv[1], argv[2], argv[3]
    try:
        count = int(count)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return 0
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if count < 0:
        count *= -1
        objlst = obj[::-1]
        revd = True
    else:
        objlst = obj[:]
        revd = False

    if count == 0:
        count = len(obj)

    counter = 0
    while count:
        try:
            objlst.remove(value)
        except ValueError:
            break
        count -= 1
        counter += 1

    if revd:
        objlst.reverse()
    obj.value = objlst
    return counter


@server.command('llen', nargs=1)
def llen_handler(client, argv):
    '''
    Returns the length of the list stored at key. If key does not exist, it is interpreted as an empty
    list and 0 is returned. An error is returned when the value stored at key is not a list.

    .. code::
        LLEN key

    :return: the length of the list at key.
    :rtype: int
    '''

    key = argv[1]
    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return 0
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    return len(obj)


@server.command('lpop', nargs=1)
def lpop_handler(client, argv):
    '''
    Removes and returns the first element of the list stored at key.

    .. code::
        LPOP key

    :return: the value of the first element, or nil when key does not exist.
    :rtype: str

    '''

    key = argv[1]
    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return 0
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    try:
        return obj.pop()
    except IndexError:
        return None


@server.command('lset', nargs=3)
def lset_handler(client, argv):
    '''

    Sets the list element at index to value. For more information on the index argument, see LINDEX.

    An error is returned for out of range indexes.

    .. code::
        LSET key index value

    '''

    key, index, value = argv[1], argv[2], argv[3]

    try:
        index = int(index)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        abort(message='index out of range')
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if index < 0:
        index = len(obj) + index

    try:
        obj[index] = value
    except IndexError:
        abort(message='index out of range')
    return True


@server.command('ltrim', nargs=3)
def ltrim_handler(client, argv):
    '''
    Trim an existing list so that it will contain only the specified range of elements specified.
    Both start and stop are zero-based indexes, where 0 is the first element of the list (the head),
    1 the next element and so on.

    For example: LTRIM foobar 0 2 will modify the list stored at foobar so that only the first three
    elements of the list will remain.

    start and end can also be negative numbers indicating offsets from the end of the list, where -1 is
    the last element of the list, -2 the penultimate element and so on.

    Out of range indexes will not produce an error: if start is larger than the end of the list, or
    start > end, the result will be an empty list (which causes key to be removed). If end is larger
    than the end of the list, Redis will treat it like the last element of the list.

    .. code::
        LTRIM key start stop

    '''

    key, start, stop = argv[1], argv[2], argv[3]

    try:
        start = int(start)
        stop = int(stop)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return True
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    if start < 0:
        start = len(obj) + start
    if stop < 0:
        stop = len(obj) + stop

    obj.value = obj[start:stop]
    return True


@server.command('linsert', nargs=4)
def linsert(client, argv):
    '''
    Inserts value in the list stored at key either before or after the reference value pivot.

    When key does not exist, it is considered an empty list and no operation is performed.

    An error is returned when key exists but does not hold a list value.

    .. code::
        LINSERT key BEFORE|AFTER pivot value

    :return: the length of the list after the insert operation, or -1 when the value pivot was not found.
    :rtype: int

    '''

    key, op, pivot, value = argv[1], argv[2].upper(), argv[3], argv[4]

    if op not in (b'BEFORE', b'AFTER'):
        abort(message='syntax error')

    if op == b'BEFORE':
        op_func = lambda index: index
    else:
        op_func = lambda index: index + 1

    try:
        obj = get_object(client.db, key, RedisListObject)
    except KeyError:
        return None
    except TypeError:
        abort(errtype='WRONGTYPE', message='Operation against a key holding the wrong kind of value')

    try:
        index = obj.index(pivot)
        obj.insert(op_func(index), value)
    except ValueError:
        return None

    return len(obj)
