from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware

from src.api.endpoints import router
from src.api.logs import log_requests
from src.core import config


def get_application() -> FastAPI:
    app = FastAPI(
        title=config.APP_NAME,
        version=config.APP_VERSION,
    )

    app.add_middleware(
        BaseHTTPMiddleware,
        dispatch=log_requests,
    )
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # TODO uncomment when DB is necessary
    # app.add_event_handler("startup", create_tables)
    app.include_router(router)

    return app


app = get_application()
