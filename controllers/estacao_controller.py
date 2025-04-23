from fastapi import HTTPException
from models.estacao import DadosEstacao
from services.mqtt_service import MQTTService
from services.mongodb_service import MongoDBService
import logging

class EstacaoController:
    def __init__(self):
        self.mqtt_service = MQTTService()
        self.mongodb_service = MongoDBService()

    async def receber_dados(self, dados: DadosEstacao):
        try:
            self.mqtt_service.publish(dados.dict())
            logging.info("Dados enviados para processamento via MQTT")
            return {"message": "Dados enviados para processamento!"}
        except Exception as e:
            logging.error(f"Erro ao processar dados: {str(e)}")
            raise HTTPException(status_code=500, detail=f"Erro ao processar dados: {str(e)}")

    async def get_failed_messages(self):
        failed = self.mongodb_service.get_failed_messages()
        return {"failed_messages": failed}