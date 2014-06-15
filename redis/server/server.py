import asyncio
import functools
import types

from redis.common.proto import RedisProtocol, ProtocolError
from redis.common.exceptions import CommandNotFoundError, CommandError, ClientQuitError
from redis.common.utils import close_connection, abort


class RedisServerMixin(object):

    def command(self, cmd, nargs=None):
        if not hasattr(self, 'handlers'):
            self.handlers = dict()
        if not isinstance(cmd, bytes):
            cmd = cmd.encode()

        def wrapper(func):
            @functools.wraps(func)
            def __wrapper(raw_argv):

                if nargs is not None:
                    if isinstance(nargs, int):
                        if len(raw_argv) - 1 != nargs:
                            abort(message="wrong number of arguments for '%s' command" % cmd.decode())
                    elif isinstance(nargs, types.FunctionType):
                        if not nargs(len(raw_argv) - 1):
                            abort(message="wrong number of arguments for '%s' command" % cmd.decode())

                return func(raw_argv)
            self.handlers[cmd.lower()] = __wrapper
            return __wrapper
        return wrapper

    def exec_command(self, argv):
        cmd = argv[0].lower()
        if not hasattr(self, 'handlers') or cmd not in self.handlers:
            raise CommandNotFoundError("unknown command '%s'" % cmd.decode())

        return self.handlers[cmd](argv)


class RedisServer(RedisServerMixin):

    def __init__(self, *args, **kwargs):
        super(RedisServer, self).__init__(*args, **kwargs)
        import redis.server
        redis.server.current_server = self

    @asyncio.coroutine
    def client_connected_cb(self, stream_reader, stream_writer):
        client = RedisClient(self, stream_reader, stream_writer)
        yield from client.run()

    def run(self, host=None, port=8888):
        loop = asyncio.get_event_loop()
        coro = asyncio.start_server(self.client_connected_cb, host=host, port=port, loop=loop)
        server = loop.run_until_complete(coro)
        print('serving on {}'.format(server.sockets[0].getsockname()))

        try:
            loop.run_forever()
        except KeyboardInterrupt:
            print("exit")
        finally:
            server.close()
            loop.close()


class RedisClient(object):

    def __init__(self, server, stream_reader, stream_writer):
        self.server = server
        self.stream_reader = stream_reader
        self.stream_writer = stream_writer
        self.proto = RedisProtocol(self.stream_reader)

    def run(self):
        while True:
            try:
                argv = yield from self.proto.get_command()
            except ProtocolError as e:
                self.write_error(errtype='ERR', message='Protocol error: %s' % e)
                self.stream_writer.transport.close()
                break

            if argv is None:
                break
            if len(argv) == 0:
                continue
            try:
                ret = self.server.exec_command(argv)
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

        print('Client closed')

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
