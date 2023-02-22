from fastapi import APIRouter

from .features import router as features_router
from .ping import router as ping_router
from .similarity import router as similarity_router

router = APIRouter()
router.include_router(ping_router)
router.include_router(features_router)
router.include_router(similarity_router)
