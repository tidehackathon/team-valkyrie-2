from src.schemas.base import Base


class TextSimilarity(Base):
    first_test: str
    second_test: str
    similarity_score: float


class TopSimilarPairs(Base):
    pairs: list[TextSimilarity]
