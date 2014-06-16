
from redis.server_impl import server

test_client = server.get_test_client()


def test_set():
    assert test_client.execute(b'SET hello world\r\n') == b'+OK\r\n'


def test_get():
    assert test_client.execute(b'GET hello\r\n') == b'$5\r\nworld\r\n'
