
from fastapi import APIRouter
from services.data_sync_service import DataSyncService
import logging

router = APIRouter(
    prefix="/sync",
    tags=["sync"]
)

@router.post("/process")
async def sync_data():
    try:
        sync_service = DataSyncService()
        await sync_service.sync_data()
        return {"message": "Sincronização iniciada com sucesso"}
    except Exception as e:
        logging.error(f"Erro na sincronização: {str(e)}")
        return {"error": str(e)}