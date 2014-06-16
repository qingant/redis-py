
import io
import asyncio
import types

from .objects import RedisObject, RedisListObject, RedisStringObject


class RedisSerializationObject:

    def to_resp(self):
        '''
        To REdis Serialization Protocol representation bytes.

        This method should return bytes.
        '''
        raise NotImplementedError()


class RedisSimpleStringSerializationObject(RedisSerializationObject):

    '''
    Format::

        +{string_content}\\r\\n

    Example::

        +OK\\r\\n
    '''

    def __init__(self, value):
        if not isinstance(value, bytes):
            self._value = str(value).encode()
        else:
            self._value = value

    def to_resp(self):
        '''
        Format::

            +{string_content}\\r\\n

        Example::

            +OK\\r\\n
        '''

        return b'+' + self._value + b'\r\n'


class RedisErrorStringSerializationObject(RedisSerializationObject):

    '''
    Error string should be a simple string, but begins with a ``-``

    Format::

        -{ErrType} {ErrMessage}\\r\\n

    Example::

        -ERR value is not a valid float

    '''

    def __init__(self, errtype='ERR', message=''):
        self._errtype = errtype
        self._message = message

    def to_resp(self):
        '''
        Format::

            -{ErrType} {ErrMessage}\\r\\n

        Example::

            -ERR value is not a valid float
        '''

        return b'-' + '{} {}'.format(self._errtype, self._message).encode() + b'\r\n'


class RedisIntegerSerializationObject(RedisSerializationObject):

    '''
    Format::

        :{integer_value}\\r\\n

    Example::

        :10\\r\\n

    '''

    def __init__(self, value):
        if isinstance(value, int):
            self._value = value
        elif isinstance(value, RedisIntegerSerializationObject):
            self._value = value._value
        else:
            raise ValueError('Value should be an integer')

    def to_resp(self):
        return b':' + str(self._value).encode() + b'\r\n'


class RedisBulkStringSerializationObject(RedisSerializationObject):

    '''
    Format::

        ${string_length}\\r\\n
        {string_content}\\r\\n

    Specially, if string is None, then length should be -1.

    Example::

        $5\\r\\n
        Hello\\r\\n
    '''

    def __init__(self, value):
        if isinstance(value, bytes):
            self._value = value
        elif isinstance(value, RedisStringObject):
            self._value = value.get_bytes()
        elif isinstance(value, str):
            self._value = value.encode()
        elif value is None:
            self._value = None
        else:
            self._value = str(value).encode()

    def to_resp(self):
        '''
        Format::

            ${string_length}\\r\\n
            {string_content}\\r\\n

        Specially, if string is None, then length should be -1.

        Example::

            $5\\r\\n
            Hello\\r\\n
        '''

        if self._value is None:
            return b'$-1\r\n'

        return b''.join((
            b'$',
            str(len(self._value)).encode(),
            b'\r\n',
            self._value,
            b'\r\n',
        ))


class RedisListSerializationObject(RedisSerializationObject):

    '''
    Format::

        *{list_lenth}\\r\\n
        {list_content}\\r\\n
        {list_content}\\r\\n

    Example::

        *2\\r\\n
        +OK\\r\\n
        $11\\r\\n
        Hello World\\r\\n
    '''

    def __init__(self, value):
        self._value = []

        for val in value:
            if not isinstance(val, RedisSerializationObject):
                if isinstance(val, RedisObject):
                    self._value.append(val.serialize())
                elif val is None:
                    self._value.append(RedisBulkStringSerializationObject(val))
                else:
                    raise ValueError('Value should be a RedisObject or RedisSerializationObject')
            else:
                self._value.append(val)

    def to_resp(self):
        '''
        Format::

            *{list_lenth}\\r\\n
            {list_content}\\r\\n
            {list_content}\\r\\n

        Example::

            *2\\r\\n
            +OK\\r\\n
            $11\\r\\n
            Hello World\\r\\n
        '''

        parts = [b'*', str(len(self._value)).encode(), b'\r\n']
        for val in self._value:
            parts.append(val.to_resp())
        return b''.join(parts)


class ProtocolError(Exception):

    def __init__(self, *args, **kwargs):
        super(ProtocolError, self).__init__(*args, **kwargs)


def resp_dumps(respobj):
    if isinstance(respobj, RedisSerializationObject):
        return respobj.to_resp()
    elif isinstance(respobj, (bytes, str, RedisStringObject)):
        return RedisBulkStringSerializationObject(respobj).to_resp()
    elif isinstance(respobj, (list, RedisListObject)):
        return RedisListSerializationObject(respobj).to_resp()
    elif respobj is None:
        return RedisBulkStringSerializationObject(respobj).to_resp()
    elif respobj is True:
        return RedisSimpleStringSerializationObject('OK').to_resp()
    elif isinstance(respobj, int):
        return RedisIntegerSerializationObject(respobj).to_resp()
    else:
        raise ValueError('%s is not RESP serializable' % respobj)


def resp_loads(raw_respstr):
    if not isinstance(raw_respstr, bytes):
        raise ValueError('Value should be bytes')

    if not raw_respstr.endswith(b'\r\n'):
        raise ValueError('%s is not a valid RESP string' % raw_respstr)

    sio = io.BytesIO(raw_respstr)

    loaded_objs = []
    while True:
        respstr = sio.readline()
        if respstr == b'':
            break
        loaded_objs.append(__resp_loads(respstr, sio))

    if len(loaded_objs) == 1:
        return loaded_objs[0]
    else:
        return loaded_objs


def __resp_loads(begin_part, buf):
    if begin_part.startswith(b'+'):
        # Simple String
        return RedisSimpleStringSerializationObject(begin_part[1:-2])
    elif begin_part.startswith(b'-'):
        # Simple Error String
        sps = begin_part.split(b' ')
        if len(sps) != 2:
            raise ValueError('%s is not a valid RESP string' % begin_part)
        return RedisErrorStringSerializationObject(errtype=sps[0].decode(), message=sps[1].decode())
    elif begin_part.startswith(b'*'):
        # List
        obj_list = []
        length = int(begin_part[1:-2].decode())
        while length != 0:
            begin_part = buf.readline()
            if begin_part == b'':
                raise ValueError('List length not match')
            obj_list.append(__resp_loads(begin_part, buf))
            length -= 1
        return RedisListSerializationObject(obj_list)
    elif begin_part.startswith(b'$'):
        # Bulk string
        length = int(begin_part[1:-2].decode())
        strvalue = buf.read(length)
        lineend = buf.readline()
        if len(strvalue) != length or len(lineend) != 2:
            raise ValueError('String length not match or EOF')
        return RedisBulkStringSerializationObject(strvalue)
    else:
        raise ValueError('%s is not a valid RESP string' % begin_part)


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
                try:
                    arg_length = int(arg_length_str[1:])
                except ValueError:
                    raise ProtocolError('Invalid length')

                arg = yield from stream_reader.read(arg_length)
                if len(arg) != length:
                    raise ProtocolError('Length not match')
                argv.append(arg)

                rest_of_line = yield from stream_reader.readline()
                if rest_of_line != b'\r\n':
                    raise ProtocolError('Length not match')
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


class InlineProtocolParser(RedisProtocolParser):

    def __init__(self):
        super(InlineProtocolParser, self).__init__()

    @asyncio.coroutine
    def get_argvalue(self, beginline, stream_reader):
        return self.parse_line(beginline)

    @classmethod
    def parse_line(self, raw_line):
        line = raw_line.rstrip(b'\r\n').rstrip(b' ').decode('unicode_escape')
        quote_ = None
        argv = []
        tmp = []
        for c in line:
            if (c == ' ' or c == '\t') and quote_ is None:
                if len(tmp) != 0:
                    argv.append(''.join(tmp).encode())
                    tmp = []
            elif c == '"' or c == "'":
                if quote_ is not None and quote_ == c:
                    quote_ = None
                    argv.append(''.join(tmp).encode())
                    tmp = []
                elif quote_ is None:
                    quote_ = c
                else:
                    tmp.append(c)
            else:
                tmp.append(c)
        if len(tmp) != 0:
            argv.append(''.join(tmp).encode())

        if quote_ is not None:
            raise ProtocolError('unbalanced quotes in request')
        return argv
