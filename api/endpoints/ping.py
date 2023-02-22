import logging

from fastapi import APIRouter

from src.schemas.ping import Ping

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    path="/",
    description="API ping route.",
    response_model=Ping,
)
async def ping() -> Ping:
    return Ping()
