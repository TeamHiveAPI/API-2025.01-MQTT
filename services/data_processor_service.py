from .parametro_service import ParametroService
from .mongodb_service import MongoDBService
import httpx
import logging
from datetime import datetime
import time

class DataProcessorService:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.mongodb_service = MongoDBService()
        self.parametro_service = ParametroService()

    async def get_estacao_info(self, uid: str):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/estacoes/uid/{uid}")
                if response.status_code == 200:
                    return response.json()
                return None
        except Exception as e:
            logging.error(f"Erro ao buscar estação: {str(e)}")
            return None
    async def process_and_send_data(self, dados_mongodb: dict):
        try:
            # Busca informações da estação
            estacao_info = await self.get_estacao_info(dados_mongodb["uid"])
            if not estacao_info:
                logging.error(f"Estação não encontrada: {dados_mongodb['uid']}")
                return False

            async with httpx.AsyncClient(follow_redirects=True) as client:
                success_count = 0
                
                 # Para cada sensor da estação
                for sensor in estacao_info["sensores"]:
                    # Busca informações do parâmetro
                    param_response = await client.get(f"{self.base_url}/parametros/{sensor['id']}")
                    if param_response.status_code != 200:
                        continue

                    param_info = param_response.json()
                    json_field = param_info["json"]  # Campo que corresponde ao nome no MongoDB

                    # Verifica se o valor existe no MongoDB
                    if json_field in dados_mongodb:
                        timestamp = dados_mongodb.get("unixtime", dados_mongodb.get("unix_time", int(time.time())))
                        data_hora = datetime.fromtimestamp(timestamp).isoformat()
                        
                        medida = {
                            "estacao_id": estacao_info["id"],
                            "parametro_id": sensor["id"],
                            "valor": dados_mongodb[json_field],
                            "data_hora": data_hora
                        }

                        try:
                            response = await client.post(f"{self.base_url}/medidas", json=medida)
                            if response.status_code in [200, 201]:
                                success_count += 1
                                logging.info(f"Medida para {param_info['nome']} enviada com sucesso")
                            else:
                                logging.error(f"Erro ao enviar medida para {param_info['nome']}: {response.status_code}")
                        except Exception as e:
                            logging.error(f"Erro ao enviar medida para {param_info['nome']}: {str(e)}")

                if success_count > 0:
                    # Atualiza o status no MongoDB
                    self.mongodb_service.collection.update_one(
                        {"_id": dados_mongodb["_id"]},
                        {"$set": {"processado": True}}
                    )
                    return True

                return False

        except Exception as e:
            logging.error(f"Erro ao processar e enviar dados: {str(e)}")
            return False