from sqlalchemy.ext.asyncio import AsyncSession


class BaseDAL:
    def __init__(self, db_session: AsyncSession):
        self.db_session = db_session

    async def commit(self):
        await self.db_session.commit()
