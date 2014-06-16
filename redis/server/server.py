import asyncio
import functools
import types
import time

from redis.common.proto import RedisProtocol, ProtocolError
from redis.common.exceptions import CommandNotFoundError, CommandError, ClientQuitError
from redis.common.utils import close_connection, abort
from .storage import RedisDatabase

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

                return func(client, argv)
            self.handlers[cmd.lower()] = __wrapper
            return __wrapper
        return wrapper

    def exec_command(self, argv, client_instance):
        cmd = argv[0].lower()
        if not hasattr(self, 'handlers') or cmd not in self.handlers:
            raise CommandNotFoundError("unknown command '%s'" % cmd.decode())

        return self.handlers[cmd](client_instance, argv)


class RedisServer(RedisServerMixin):

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


class RedisClient(object):

    def __init__(self, server, stream_reader, stream_writer):
        self.server = server
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.proto = RedisProtocol(self.stream_reader)

        self.name = None
        self.parse_until = None

        self._db = self.server.default_database()

        self.last_cmd = None
        self.conn_time = time.time()
        self.idle_time = 0
        self.last_active_time = self.conn_time

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
    def db(self):
        return self._db

    def change_db(self, dbnum):
        self._db = self.server.get_database(dbnum)

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
                self.write_error(errtype='ERR', message='Protocol error: %s' % e)
                self.stream_writer.transport.close()
                break
            else:
                cur_time = time.time()
                self.idle_time += cur_time - self.last_active_time
                self.last_active_time = cur_time

            if argv is None:
                break
            if len(argv) == 0:
                continue
            try:
                ret = self.server.exec_command(argv, self)
                self.write_object(ret)
            except CommandNotFoundError as e:
                self.write_error(errtype='ERR', message=e)
            except CommandError as e:
                errtype, message = e.args
                self.write_error(errtype=errtype, message=message)
            except ClientQuitError as e:
                self.write_simple_string('OK')
                self.stream_writer.transport.close()
                break
            self.last_cmd = argv[0].decode()

        logger.info('client {} exiting'.format(self.ipaddr))

    def write_error(self, errtype, message):
        resp = '-{errtype} {message}\r\n'.format(errtype=errtype, message=message)
        self.stream_writer.write(resp.encode())

    def write_simple_string(self, string):
        resp = '+{string}\r\n'.format(string=string)
        self.stream_writer.write(resp.encode())

    def write_object(self, obj):
        if obj is True:
            self.write_simple_string('OK')
        else:
            resp = self.serialize_to_resp(obj)
            self.stream_writer.write(resp)

    def serialize_to_resp(self, obj):
        if isinstance(obj, list):
            resp = [b'*' + str(len(obj)).encode() + b'\r\n']
            for o in obj:
                resp.append(self.serialize_to_resp(o))
            return b''.join(resp)
        elif isinstance(obj, str):
            obj = obj.encode()
            return b''.join((b'$', str(len(obj)).encode(), b'\r\n', obj, b'\r\n'))
        elif isinstance(obj, bytes):
            return b''.join((b'$', str(len(obj)).encode(), b'\r\n', obj, b'\r\n'))
        elif isinstance(obj, int):
            resp = ':%d\r\n' % obj
            return resp.encode()
        elif obj is None:
            return b'$-1\r\n'
