"""Module with logging setting"""

import logging
from random import choices
from string import ascii_uppercase, digits
from time import time
from typing import Callable

from fastapi import Request, Response

logging.getLogger("uvicorn").handlers.clear()

logger = logging.getLogger()
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
fh = logging.FileHandler(filename="server.log")
formatter = logging.Formatter("%(levelname)-9s %(asctime)s - %(message)s")

ch.setFormatter(formatter)
fh.setFormatter(formatter)

logger.addHandler(ch)  # Exporting logs to the screen
logger.addHandler(fh)  # Exporting logs to a file


async def log_requests(request: Request, call_next: Callable) -> Response:
    rid = ''.join(choices(ascii_uppercase + digits, k=6))
    logger.info(f"Request {rid} started")
    start_time = time()

    response = await call_next(request)

    duration = (time() - start_time) * 1000
    logger.info(f"Request {rid} finished in {duration:.2f}ms")

    return response
