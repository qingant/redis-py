import pickle
import time

from redis.server import current_server as server
from redis.server.server import RedisClientBase
from redis.common.objects import RedisStringObject
from redis.common.utils import abort, close_connection
from redis.common.utils import nargs_greater_equal
from redis.common.utils import get_object


@server.command('del', nargs=nargs_greater_equal(1))
def del_handler(client, argv):
    '''
    Removes the specified keys. A key is ignored if it does not exist.

    :return: The number of keys that were removed.
    :rtype: int

    '''

    deleted = 0
    for key in argv:
        try:
            del client.db.key_space[key]
            deleted += 1
        except KeyError:
            pass

    return deleted


@server.command('dump', nargs=1)
def dump_handler(client, argv):
    '''
    Serialize the value stored at key in a Redis-specific format and return it to the user. The returned
    value can be synthesized back into a Redis key using the RESTORE command.

    The serialization format is opaque and non-standard, however it has a few semantical characteristics:

    * It contains a 64-bit checksum that is used to make sure errors will be detected. The RESTORE command
    makes sure to check the checksum before synthesizing a key using the serialized value. *(NOT IMPLEMENTED)*

    * Values are encoded in the same format used by RDB.

    * An RDB version is encoded inside the serialized value, so that different Redis versions with
    incompatible RDB formats will refuse to process the serialized value. *(NOT IMPLEMENTED)*

    <del>The serialized value does NOT contain expire information. In order to capture the time to live of the
    current value the PTTL command should be used.</del>

    If key does not exist a nil bulk reply is returned.

    .. code::
        DUMP key
    '''

    key = argv[1]

    if key not in client.db.key_space:
        return None

    return pickle.dumps(client.db.key_space[key], pickle.HIGHEST_PROTOCOL)


@server.command('echo', nargs=1)
def echo_handler(client, argv):
    '''
    Returns message.

    .. code::
        ECHO message
    '''

    return argv[1]


@server.command('expire', nargs=2)
def expire_handler(client, argv):
    '''
    Set a timeout on key. After the timeout has expired, the key will automatically be deleted. A key
    with an associated timeout is often said to be volatile in Redis terminology.

    The timeout is cleared only when the key is removed using the DEL command or overwritten using the
    SET or GETSET commands. This means that all the operations that conceptually alter the value stored
    at the key without replacing it with a new one will leave the timeout untouched. For instance,
    incrementing the value of a key with INCR, pushing a new value into a list with LPUSH, or altering the
    field value of a hash with HSET are all operations that will leave the timeout untouched.

    The timeout can also be cleared, turning the key back into a persistent key, using the PERSIST command.

    If a key is renamed with RENAME, the associated time to live is transferred to the new key name.

    If a key is overwritten by RENAME, like in the case of an existing key Key_A that is overwritten by a call
    like RENAME Key_B Key_A, it does not matter if the original Key_A had a timeout associated or not, the new
    key Key_A will inherit all the characteristics of Key_B.

    .. code::
        EXPIRE key time
    '''

    key, exptime = argv[1], argv[2]
    try:
        exptime = int(exptime)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key)
    except KeyError:
        return 0

    obj.expire_time = time.time() + exptime
    return 1


@server.command('expireat', nargs=2)
def expireat_handler(client, argv):
    '''
    EXPIREAT has the same effect and semantic as EXPIRE, but instead of specifying the number of seconds
    representing the TTL (time to live), it takes an absolute Unix timestamp (seconds since January 1, 1970).

    Please for the specific semantics of the command refer to the documentation of EXPIRE.

    .. code::
        EXPIREAT key timestamp

    :return: 1 if the timeout was set. 0 if key does not exist or the timeout could not be set (see: EXPIRE).
    :rtype: int

    '''

    key, exptime = argv[1], argv[2]
    try:
        exptime = int(exptime)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key)
    except KeyError:
        return 0

    obj.expire_time = exptime
    return 1


@server.command('flushall', nargs=0)
def flushall_handler(client, argv):
    '''
    Delete all the keys of all the existing databases, not just the currently selected one. This command never fails.

    '''

    dbs = client.server.all_databases()

    for db in dbs:
        db.flush()

    return True


@server.command('flushdb', nargs=0)
def flushdb_handler(client, argv):
    '''
    Delete all the keys of the currently selected DB. This command never fails.

    '''

    client.db.flush()

    return True


@server.command('persist', nargs=1)
def persist_handler(client, argv):
    '''
    Remove the existing timeout on key, turning the key from volatile (a key with an expire set) to
    persistent (a key that will never expire as no timeout is associated).

    .. code::
        PERSIST key

    '''

    key = argv[1]
    try:
        obj = get_object(client.db, key)
    except KeyError:
        return 0

    obj.expire_time = None
    return 1


@server.command('pexpire', nargs=2)
def pexpire_handler(client, argv):
    '''
    This command works exactly like EXPIRE but the time to live of the key is specified in milliseconds
    instead of seconds.

    .. code::
        PEXPIRE key milliseconds

    '''

    key, milliseconds = argv[1], argv[2]

    try:
        milliseconds = int(milliseconds)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key)
    except KeyError:
        return 0

    obj.expire_time = time.time() + milliseconds / 1000.0
    return 1


@server.command('pexpireat', nargs=2)
def pexpireat_handler(client, argv):
    '''
    PEXPIREAT has the same effect and semantic as EXPIREAT, but the Unix time at which the key will
    expire is specified in milliseconds instead of seconds.

    .. code::
        PEXPIREAT key milliseconds-timestamp

    '''

    key, milliseconds = argv[1], argv[2]

    try:
        milliseconds = int(milliseconds)
    except ValueError:
        abort(message='value is not an integer or out of range')

    try:
        obj = get_object(client.db, key)
    except KeyError:
        return 0

    obj.expire_time = milliseconds / 1000.0
    return 1


@server.command('multi', nargs=0)
def multi_handler(client, argv):
    '''

    '''

    if client.stat == RedisClientBase.STAT_MULTI:
        abort(message='MULTI calls can not be nested')

    client.stat = RedisClientBase.STAT_MULTI
    return True


@server.command('exec', nargs=0)
def exec_handler(client, argv):
    '''

    '''

    if client.stat != RedisClientBase.STAT_MULTI:
        abort(message='EXEC without MULTI')

    ret = []
    for cmd in client.multi_command_list:
        ret.append(client.exec_command(cmd))

    client.multi_command_list = []
    client.stat = RedisClientBase.STAT_NORMAL

    return ret
