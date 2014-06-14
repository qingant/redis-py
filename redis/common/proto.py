
import io
import asyncio
import types


class ProtocolError(Exception):

    def __init__(self, *args, **kwargs):
        super(ProtocolError, self).__init__(*args, **kwargs)


class RedisProtocol(object):

    def __init__(self, stream_reader):
        self.stream_reader = stream_reader
        self.parser = None

    def get_command(self):
        line = yield from self.stream_reader.readline()
        if not line:
            return None

        if self.parser is None:
            if line.startswith(b'*'):
                self.parser = BulkProtocolParser()
            else:
                self.parser = InlineProtocolParser()

        result = yield from self.parser.get_argvalue(line, self.stream_reader)
        self.parser = None
        return result


class RedisProtocolParser(object):

    def __init__(self):
        pass

    @asyncio.coroutine
    def get_argvalue(self, beginline, stream_reader):
        raise NotImplementedError()


class BulkProtocolParser(RedisProtocolParser):

    def __init__(self):
        super(BulkProtocolParser, self).__init__()

    @asyncio.coroutine
    def get_argvalue(self, beginline, stream_reader):
        '''
        For parsing input string to argv list.

        '''
        line = beginline.rstrip(b'\r\n')
        if line.startswith(b'*'):
            array_length = int(line[1:])
            argv = []
            for i in range(array_length):
                arg_length_str = yield from stream_reader.readline()
                if not arg_length_str.startswith(b'$'):
                    raise ProtocolError("expected '$', got '\\x%x'" % arg_length_str[0])
                arg = yield from stream_reader.readline()
                arg = arg.rstrip(b'\r\n')
                if len(arg) != int(arg_length_str[1:]):
                    raise ProtocolError('Length not match')
                argv.append(arg)
        return argv

    @asyncio.coroutine
    def parse_resp(self, beginline, stream_reader):
        '''
        For parsing an RedisObject from a RESP string.

        '''
        if beginline.startswith(b'*'):
            return (yield from __parse_array(beginline, stream_reader))
        elif beginline.startswith(b'+'):
            return (yield from __parse_simple_string(beginline, stream_reader))
        elif beginline.startswith(b'-'):
            return (yield from __parse_error_string(beginline, stream_reader))
        elif beginline.startswith(b'$'):
            return (yield from __parse_string(beginline, stream_reader))
        else:
            raise ProtocolError('Invalid syntax')

    @asyncio.coroutine
    def __parse_array(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')
        array_length = int(line[1:])
        argv = []
        for i in range(array_length):
            arg_length_str = yield from stream_reader.readline()
            if not arg_length_str.startswith(b'$'):
                raise ProtocolError("expected '$', got '\\x%x'" % arg_length_str[0])
            arg = yield from stream_reader.readline()
            arg = arg.rstrip(b'\r\n')
            if len(arg) != int(arg_length_str[1:]):
                raise ProtocolError('Length not match')
            argv.append(arg)
        return argv

    @asyncio.coroutine
    def __parse_simple_string(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')
        return line[1:]

    @asyncio.coroutine
    def __parse_string(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')
        return line[1:]

    @asyncio.coroutine
    def __parse_error_string(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')
        errstr = line.split(' ', 1)
        return errstr[0], errstr[1] if len(errstr) > 1 else ''

    @asyncio.coroutine
    def __parse_integer(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')


class InlineProtocolParser(RedisProtocolParser):

    def __init__(self):
        super(InlineProtocolParser, self).__init__()

    @asyncio.coroutine
    def get_argvalue(self, beginline, stream_reader):
        line = beginline.rstrip(b'\r\n')
        return line.split(b' ')
