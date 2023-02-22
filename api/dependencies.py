import logging
from typing import Callable, Type

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession

from src.dals.base import BaseDAL
from src.database.db import get_session

logger = logging.getLogger(__name__)


def get_dal(dal: Type[BaseDAL]) -> Callable:
    async def _get_dal(session: AsyncSession = Depends(get_session)) -> BaseDAL:
        yield dal(session)

    return _get_dal
