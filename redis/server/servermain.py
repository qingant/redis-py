
from .server import RedisServer

server = RedisServer()

from .command import *


def server_main():
    server.run()

if __name__ == '__main__':
    server_main()
