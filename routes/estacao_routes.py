from fastapi import APIRouter, HTTPException
from models.estacao import DadosEstacao
from services.mqtt_service import MQTTService
import logging

router = APIRouter(
    prefix="/dados",
    tags=["dados"]
)

@router.post("/")
async def receber_dados(dados: DadosEstacao):
    try:
        mqtt_service = MQTTService()
        logging.info(f"Recebendo dados da estação: {dados.estacao.uid}")
        
        resultado = await mqtt_service.process_message(dados)
        if resultado:
            return {"mensagem": "Dados processados com sucesso!"}
        else:
            raise HTTPException(status_code=404, 
                              detail="Estação não encontrada na API externa")
    except Exception as e:
        logging.error(f"Erro ao processar requisição: {str(e)}")
        raise HTTPException(status_code=500, 
                          detail=f"Erro ao processar dados: {str(e)}")