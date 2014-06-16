
from redis.server import RedisServer

server = RedisServer()

from .string_command import *
from .list_command import *
from .client_command import *
from .misc_command import *


def server_main():
    server.run()

if __name__ == '__main__':
    server_main()
