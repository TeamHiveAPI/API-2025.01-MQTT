from fastapi import APIRouter
from services.external_services import ExternalAPIService

router = APIRouter(
    prefix="/external",
    tags=["external"]
)

@router.get("/data")
async def get_external_data():
    external_service = ExternalAPIService()
    return await external_service.get_external_data()