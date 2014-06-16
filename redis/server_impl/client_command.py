from redis.server import current_server as server
from redis.common.objects import RedisStringObject
from redis.common.utils import abort, close_connection
from redis.common.utils import nargs_greater_equal


@server.command('client', nargs=nargs_greater_equal(1))
def client_handler(client, argv):
    '''

    Client command dispatcher

    .. code::
        CLIENT op args

    '''

    op = argv[1].upper()

    if op == b'KILL':
        return client_kill_handler(client, argv)
    elif op == b'LIST':
        return client_list_handler(client, argv)
    elif op == b'GETNAME':
        return client_getname_handler(client, argv)
    elif op == b'SETNAME':
        return client_setname_handler(client, argv)
    else:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')


def client_kill_handler(client, argv):
    '''
    The CLIENT KILL command closes a given client connection identified by ip:port.

    The ip:port should match a line returned by the CLIENT LIST command.

    .. code::
        CLIENT KILL ip:port

    '''

    if len(argv) != 3:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')

    try:
        client.server.kill_client(argv[2].decode())
    except KeyError:
        abort(message='No such client')

    return True


def client_list_handler(client, argv):
    '''
    The CLIENT LIST command returns information and statistics about the client connections server in a
    mostly human readable format.

    .. code::
        CLIENT LIST

    :return:  a unique string, formatted as follows:
              * One client connection per line (separated by LF)
              * Each line is composed of a succession of property=value fields separated by a space character.

    '''

    if len(argv) != 2:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')

    return client.server.get_clients_info_str()


def client_getname_handler(client, argv):
    '''
    The CLIENT GETNAME returns the name of the current connection as set by CLIENT SETNAME. Since every
    new connection starts without an associated name, if no name was assigned a null bulk reply is returned.

    .. code::
        CLIENT GETNAME

    '''

    if len(argv) != 2:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')

    return client.name


def client_setname_handler(client, argv):
    '''
    The CLIENT SETNAME command assigns a name to the current connection.

    The assigned name is displayed in the output of CLIENT LIST so that it is possible to identify the
    client that performed a given connection.

    For instance when Redis is used in order to implement a queue, producers and consumers of messages
    may want to set the name of the connection according to their role.

    There is no limit to the length of the name that can be assigned if not the usual limits of the Redis
    string type (512 MB). However it is not possible to use spaces in the connection name as this would
    violate the format of the CLIENT LIST reply.

    It is possible to entirely remove the connection name setting it to the empty string, that is not a valid
    connection name since it serves to this specific purpose.

    The connection name can be inspected using CLIENT GETNAME.

    Every new connection starts without an assigned name.

    Tip: setting names to connections is a good way to debug connection leaks due to bugs in the application
    using Redis.

    .. code::
        CLIENT SETNAME name

    '''

    if len(argv) != 3:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')

    client.name = argv[2].decode()

    return True


def client_pause_handler(client, argv):
    '''
    CLIENT PAUSE is a connections control command able to suspend all the Redis clients for the specified
    amount of time (in milliseconds).

    The command performs the following actions:

    * It stops processing all the pending commands from normal and pub/sub clients. However interactions with
      slaves will continue normally.

    * However it returns OK to the caller ASAP, so the CLIENT PAUSE command execution is not paused by itself.

    * When the specified amount of time has elapsed, all the clients are unblocked: this will trigger the
      processing of all the commands accumulated in the query buffer of every client during the pause.

    This command is useful as it makes able to switch clients from a Redis instance to another one in a
    controlled way. For example during an instance upgrade the system administrator could do the following:

    * Pause the clients using CLIENT PAUSE

    * Wait a few seconds to make sure the slaves processed the latest replication stream from the master.

    * Turn one of the slaves into a master.

    * Reconfigure clients to connect with the new master.

    It is possible to send CLIENT PAUSE in a MULTI/EXEC block together with the INFO replication command in
    order to get the current master offset at the time the clients are blocked. This way it is possible to wait
    for a specific offset in the slave side in order to make sure all the replication stream was processed.

    .. code::
        CLIENT PAUSE

    '''

    if len(argv) != 2:
        abort(message='Syntax error, try CLIENT (LIST | KILL ip:port | GETNAME | SETNAME connection-name)')

    raise NotImplementedError()
