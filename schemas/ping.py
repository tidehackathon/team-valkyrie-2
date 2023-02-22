from src.schemas.base import Base


class Ping(Base):
    message: str = 'ok'
