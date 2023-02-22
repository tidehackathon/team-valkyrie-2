from src.schemas.base import Base
from typing import Optional

class Features(Base):
    input: str
    entities: list
    lemmatized: list
    words: dict


class LinguisticFeatures(Base):
    features: list[Features]
