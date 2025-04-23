from .parametro_service import ParametroService
from .mongodb_service import MongoDBService
import httpx
import logging
from datetime import datetime

class DataSyncService:
    def __init__(self):
        self.mongodb_service = MongoDBService()
        self.parametro_service = ParametroService()
        self.api_url = "http://localhost:8000/medidas"

    async def sync_data(self):
        try:
            # Busca dados não processados do MongoDB
            dados_para_processar = self.mongodb_service.collection.find({"processado": {"$ne": True}})
            
            async with httpx.AsyncClient() as client:
                for dados in dados_para_processar:
                    # Valida os parâmetros usando o ParametroService
                    parametros_validos = await self.parametro_service.validar_parametros_mongodb(dados)
                    if not parametros_validos:
                        logging.error(f"Nenhum parâmetro válido encontrado para: {dados['uid']}")
                        continue

                    # Busca informações da estação
                    response = await client.get(f"{self.parametro_service.base_url}/estacoes/uid/{dados['uid']}")
                    if response.status_code != 200:
                        logging.error(f"Estação não encontrada: {dados['uid']}")
                        continue

                    estacao_info = response.json()
                    
                    # Para cada sensor da estação
                    for sensor in estacao_info["sensores"]:
                        param_response = await client.get(f"{self.parametro_service.base_url}/parametros/{sensor['id']}")
                        if param_response.status_code != 200:
                            continue

                        param_info = param_response.json()
                        param_nome = param_info["nome"]

                        if param_nome in parametros_validos:
                            medida = {
                                "estacao_id": estacao_info["id"],
                                "parametro_id": sensor["id"],
                                "valor": parametros_validos[param_nome],
                                "data_hora": datetime.fromtimestamp(dados["unix_time"]).isoformat()
                            }

                            response = await client.post(
                                self.api_url,
                                json=medida
                            )

                            if response.status_code in [200, 201]:
                                self.mongodb_service.retry_mongodb_insert({
                                    "_id": dados["_id"],
                                    "processado": True
                                })
                                logging.info(f"Dados processados para estação {dados['uid']}")
                            else:
                                logging.error(f"Erro ao enviar dados: {response.status_code}")

        except Exception as e:
            logging.error(f"Erro na sincronização: {str(e)}")