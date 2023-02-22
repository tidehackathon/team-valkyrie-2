import logging

from fastapi import APIRouter, Body

from src.schemas.similarity import TopSimilarPairs

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/similarity",
)


@router.post(
    path="/most_similar_texts/",
    description="...",
    response_model=TopSimilarPairs,
)
async def get_most_similar_text(text_list: list[str] = Body()) -> TopSimilarPairs:
    return TopSimilarPairs(pairs=[])
