import asyncio
import functools
import types
import time

from redis.common.proto import RedisProtocol, ProtocolError
from redis.common.exceptions import CommandNotFoundError, CommandError, ClientQuitError
from redis.common.utils import close_connection, abort
from .storage import RedisDatabase

from redis.common.proto import RedisSerializationObject, \
    RedisSimpleStringSerializationObject, RedisErrorStringSerializationObject, \
    RedisIntegerSerializationObject, RedisListSerializationObject, RedisBulkStringSerializationObject

from redis.common.objects import RedisObject

from redis.common.proto import resp_loads, InlineProtocolParser

import logging
logger = logging.getLogger(__name__)


class RedisServerMixin(object):

    def command(self, cmd, nargs=None):
        if not hasattr(self, 'handlers'):
            self.handlers = dict()
        if not isinstance(cmd, bytes):
            cmd = cmd.encode()

        def wrapper(func):
            @functools.wraps(func)
            def __wrapper(client, argv):

                if nargs is not None:
                    if isinstance(nargs, int):
                        if len(argv) - 1 != nargs:
                            abort(message="wrong number of arguments for '%s' command" % cmd.decode())
                    elif isinstance(nargs, types.FunctionType):
                        if not nargs(len(argv) - 1):
                            abort(message="wrong number of arguments for '%s' command" % cmd.decode())

                try:
                    ret = func(client, argv)

                    if isinstance(ret, RedisObject):
                        return ret.serialize()
                    elif isinstance(ret, RedisSerializationObject):
                        return ret
                    elif ret is True:
                        return RedisSimpleStringSerializationObject('OK')
                    elif isinstance(ret, int):
                        # This line shouldn't put before the ``ret is True``
                        # **Cuz True is an integer**
                        return RedisIntegerSerializationObject(ret)
                    elif isinstance(ret, (list, types.GeneratorType)):
                        return RedisListSerializationObject(ret)
                    elif ret is None:
                        return RedisBulkStringSerializationObject(None)
                    else:
                        raise ValueError('Invalid reply %s' % ret)
                except CommandNotFoundError as e:
                    return RedisErrorStringSerializationObject(errtype='ERR', message=str(e))
                except CommandError as e:
                    errtype, message = e.args
                    return RedisErrorStringSerializationObject(errtype=errtype, message=message)

            self.handlers[cmd.lower()] = __wrapper
            return __wrapper
        return wrapper

    def exec_command(self, argv, client_instance):
        cmd = argv[0].lower()
        if not hasattr(self, 'handlers') or cmd not in self.handlers:
            raise CommandNotFoundError("unknown command '%s'" % cmd.decode())

        return self.handlers[cmd](client_instance, argv)


class RedisServerTestClientMixin:

    def get_test_client(self):
        return RedisTestClient(self)


class RedisServer(RedisServerMixin, RedisServerTestClientMixin):

    def __init__(self, *args, **kwargs):
        super(RedisServer, self).__init__(*args, **kwargs)
        import redis.server
        redis.server.current_server = self

        self.clients = dict()
        self.dbs = {
            0: RedisDatabase(0),
        }
        self.pause_seconds = None

    def all_databases(self):
        return self.dbs.values()

    def default_database(self):
        return self.dbs[0]

    def get_database(self, dbnum):
        if dbnum not in self.dbs:
            self.dbs[dbnum] = RedisDatabase(dbnum)
        return self.dbs[dbnum]

    def kill_client(self, ipaddr):
        client = self.clients[ipaddr]
        client.transport.close()

    def pause_all_clients(self, seconds):
        self.pause_seconds = seconds
        raise NotImplementedError()

    def get_clients_info_str(self):
        repr_strs = [client.get_info_str() for ipaddr, client in self.clients.items()]
        return '\r'.join(repr_strs)

    @asyncio.coroutine
    def client_connected_cb(self, stream_reader, stream_writer):
        client = RedisClient(self, stream_reader, stream_writer)
        self.clients[client.ipaddr] = client
        yield from client.run()
        del self.clients[client.ipaddr]

    def run(self, host=None, port=8888):
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.client_connected_cb, host=host, port=port, loop=loop)
        server = loop.run_until_complete(coro)
        logger.info('serving on {}'.format(server.sockets[0].getsockname()))

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            logger.info('exiting')
        finally:
            server.close()
            loop.close()


class RedisClientBase:

    STAT_NORMAL = 0
    STAT_MULTI = 1
    STAT_EXEC = 2

    def __init__(self, server):
        self.server = server

        self.name = None
        self.parse_until = None

        self._db = self.server.default_database()

        self.last_cmd = None
        self.conn_time = time.time()
        self.idle_time = 0
        self.last_active_time = self.conn_time

        self.stat = RedisClient.STAT_NORMAL
        self.multi_command_list = []

    def get_info_str(self):
        return 'addr={addr} fd= name={name} age={age} idle={idle} flags= db={db} sub= psub= multi= qbuf= ' \
            'qbuf-free= obl= oll= omem= events= cmd={last_cmd}'.format(
                addr='',
                age=int(time.time() - self.conn_time),
                idle=int(self.idle_time),
                db=self.db.idnum,
                last_cmd=self.last_cmd,
                name=self.name if self.name is not None else ''
            )

    @property
    def db(self):
        return self._db

    def change_db(self, dbnum):
        self._db = self.server.get_database(dbnum)

    def exec_command(self, argv):
        '''
        Execute the command and serialize the return value as the REdis Serializaion Protocol representation.

        :return: RESP value
        :rtype: RedisSerializationObject

        '''

        return self.server.exec_command(argv, self)

    def run(self):
        raise NotImplementedError()


class RedisTestClient(RedisClientBase):

    def __init__(self, server):
        super(RedisTestClient, self).__init__(server)

    def execute(self, command_str):
        if isinstance(command_str, str):
            command_str = command_str.encode()
        elif isinstance(command_str, bytes):
            command_str = command_str
        else:
            raise ValueError('Command string should be a str or bytes')

        if command_str.startswith(b'*'):
            lst = resp_loads(command_str)
            if not isinstance(lst, RedisListSerializationObject):
                raise ValueError('Command is not a RESP list')
            argv = []
            for item in lst:
                if not isinstance(item, RedisBulkStringSerializationObject):
                    raise ValueError('Command should not contain a %s' % item)
                argv.append(item._value)
        else:
            argv = InlineProtocolParser.parse_line(command_str)

        if self.stat == RedisClientBase.STAT_MULTI \
                and argv[0].upper() != b'EXEC':
            self.multi_command_list.append(argv)
            return RedisSimpleStringSerializationObject('QUEUED')

        return self.server.exec_command(argv, self).to_resp()


class RedisClient(RedisClientBase):

    def __init__(self, server, stream_reader, stream_writer):
        super(RedisClientBase, self).__init__(server)
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer

    def get_info_str(self):
        return 'addr={addr} fd= name={name} age={age} idle={idle} flags= db={db} sub= psub= multi= qbuf= ' \
            'qbuf-free= obl= oll= omem= events= cmd={last_cmd}'.format(
                addr=self.ipaddr,
                age=int(time.time() - self.conn_time),
                idle=int(self.idle_time),
                db=self.db.idnum,
                last_cmd=self.last_cmd,
                name=self.name if self.name is not None else ''
            )

    @property
    def transport(self):
        return self.stream_writer.transport

    @property
    def peername(self):
        return self.transport.get_extra_info('peername')

    @property
    def ipaddr(self):
        ipaddr, ipport, *others = self.peername
        return '%s:%s' % (ipaddr, ipport)

    def run(self):
        logger.info('client {} connected'.format(self.ipaddr))
        while True:
            try:
                argv = yield from self.proto.get_command()
            except ProtocolError as e:
                self.write_object(RedisErrorStringSerializationObject(errtype='ERR', message='Protocol error: %s' % e))
                self.close()
                break
            else:
                cur_time = time.time()
                self.idle_time += cur_time - self.last_active_time
                self.last_active_time = cur_time

            if argv is None:
                break
            if len(argv) == 0:
                continue

            if argv[0].upper() == b'QUIT':
                self.write_object(RedisSimpleStringSerializationObject('OK'))
                self.close()
                break

            if self.stat == RedisClientBase.STAT_MULTI \
                    and argv[0].upper() != b'EXEC':
                self.multi_command_list.append(argv)
                self.write_object(RedisSimpleStringSerializationObject('QUEUED'))
                continue

            ret = self.exec_command(argv)
            self.write_object(ret)
            self.last_cmd = argv[0].decode()

        logger.info('client {} exiting'.format(self.ipaddr))

    def close(self):
        yield from self.stream_writer.drain()
        self.stream_writer.close()

    def write_object(self, obj):
        if not isinstance(obj, RedisSerializationObject):
            raise ValueError('Object should be a RedisSerializationObject')
        self.stream_writer.write(obj.to_resp())
