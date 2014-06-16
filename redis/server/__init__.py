
from .server import RedisServer

current_server = None

import logging
logging.basicConfig(format='%(levelname)s - %(asctime)s %(message)s', level=logging.INFO)
