import logging

from fastapi import APIRouter, Body

from src.schemas.linguistic_features import Features, LinguisticFeatures
from src.utils.linguistic_features import text_to_json

logger = logging.getLogger(__name__)
router = APIRouter(
    prefix="/features",
)


@router.post(
    path="/",
    description="...",
    response_model=LinguisticFeatures,
)
async def get_features(text: str = Body()) -> LinguisticFeatures:
    return LinguisticFeatures(features=[Features(**item) for item in text_to_json(text)])
