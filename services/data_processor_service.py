# from .parametro_service import ParametroService
# from .mongodb_service import MongoDBService
# import httpx
# import logging
# from datetime import datetime
# import time

# class DataProcessorService:
#     def __init__(self):
#         self.base_url = "http://localhost:8000"
#         self.mongodb_service = MongoDBService()
#         self.parametro_service = ParametroService()
#         self.running = False

#     async def process_loop(self):
#         while self.running:
#             # Busca dados não processados
#             dados_nao_processados = self.mongodb_service.collection.find({"processado": {"$ne": True}})
            
#             for dados in dados_nao_processados:
#                 await self.process_and_send_data(dados)
            
#             await asyncio.sleep(31)  # Processa a cada 31 segundos
    
#     def start(self):
#         self.running = True
#         asyncio.create_task(self.process_loop())
#         logging.info("Processador de dados iniciado")
    
#     def stop(self):
#         self.running = False
#         logging.info("Processador de dados parado")

#     async def get_estacao_info(self, uid: str):
#         try:
#             async with httpx.AsyncClient() as client:
#                 response = await client.get(f"{self.base_url}/estacoes/uid/{uid}")
#                 if response.status_code == 200:
#                     return response.json()
#                 return None
#         except Exception as e:
#             logging.error(f"Erro ao buscar estação: {str(e)}")
#             return None
#     async def process_and_send_data(self, dados_mongodb: dict):
#         try:
#             # Busca informações da estação
#             estacao_info = await self.get_estacao_info(dados_mongodb["uid"])
#             if not estacao_info:
#                 logging.error(f"Estação não encontrada: {dados_mongodb['uid']}")
#                 return False

#             async with httpx.AsyncClient(follow_redirects=True) as client:
#                 success_count = 0
                
#                  # Para cada sensor da estação
#                 for sensor in estacao_info["sensores"]:
#                     # Busca informações do parâmetro
#                     param_response = await client.get(f"{self.base_url}/parametros/{sensor['id']}")
#                     if param_response.status_code != 200:
#                         continue

#                     param_info = param_response.json()
#                     json_field = param_info["json"]  # Campo que corresponde ao nome no MongoDB

#                     # Verifica se o valor existe no MongoDB
#                     if json_field in dados_mongodb:
#                         timestamp = dados_mongodb.get("unixtime", dados_mongodb.get("unix_time", int(time.time())))
#                         data_hora = datetime.fromtimestamp(timestamp).isoformat()
                        
#                         medida = {
#                             "estacao_id": estacao_info["id"],
#                             "parametro_id": sensor["id"],
#                             "valor": dados_mongodb[json_field],
#                             "data_hora": data_hora
#                         }

#                         try:
#                             response = await client.post(f"{self.base_url}/medidas", json=medida)
#                             if response.status_code in [200, 201]:
#                                 success_count += 1
#                                 logging.info(f"Medida para {param_info['nome']} enviada com sucesso")
#                             else:
#                                 logging.error(f"Erro ao enviar medida para {param_info['nome']}: {response.status_code}")
#                         except Exception as e:
#                             logging.error(f"Erro ao enviar medida para {param_info['nome']}: {str(e)}")

#                 if success_count > 0:
#                     # Atualiza o status no MongoDB
#                     self.mongodb_service.collection.update_one(
#                         {"_id": dados_mongodb["_id"]},
#                         {"$set": {"processado": True}}
#                     )
#                     return True

#                 return False

#         except Exception as e:
#             logging.error(f"Erro ao processar e enviar dados: {str(e)}")
#             return False




from .parametro_service import ParametroService
from .mongodb_service import MongoDBService
from .external_services import ExternalAPIService
import httpx
import logging
from datetime import datetime
import time
import asyncio

class DataProcessorService:
    def __init__(self):
        self.base_url = "http://localhost:8000"
        self.mongodb_service = MongoDBService()
        self.parametro_service = ParametroService()
        self.external_service = ExternalAPIService()
        self.running = False

    async def process_loop(self):
        while self.running:
            try:
                # Busca dados não processados
                dados_nao_processados = self.mongodb_service.collection.find({"processado": {"$ne": True}})
                
                for dados in dados_nao_processados:
                    success = await self.process_and_send_data(dados)
                    if success:
                        logging.info(f"Processamento bem sucedido para dados: {dados['uid']}")
                    else:
                        logging.warning(f"Falha no processamento para dados: {dados['uid']}")
                
                await asyncio.sleep(31)  # Processa a cada 31 segundos
            except Exception as e:
                logging.error(f"Erro no loop de processamento: {str(e)}")
                await asyncio.sleep(31)  # Continua tentando mesmo após erro
    
    def start(self):
        self.running = True
        asyncio.create_task(self.process_loop())
        logging.info("Processador de dados iniciado")
    
    def stop(self):
        self.running = False
        logging.info("Processador de dados parado")

    async def get_estacao_info(self, uid: str):
        # Usa o ExternalAPIService para verificar a estação
        exists = await self.external_service.verify_station_exists(uid)
        if not exists:
            logging.error(f"Estação {uid} não encontrada ou inválida")
            return None
            
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/estacoes/uid/{uid}")
                if response.status_code == 200:
                    return response.json()
                logging.error(f"Erro ao buscar detalhes da estação {uid}: Status {response.status_code}")
                return None
        except Exception as e:
            logging.error(f"Erro ao buscar detalhes da estação {uid}: {str(e)}")
            return None

    async def process_and_send_data(self, dados_mongodb: dict):
        try:
            # Valida os parâmetros usando o ParametroService
            parametros_validos = await self.parametro_service.validar_parametros_mongodb(dados_mongodb)
            if not parametros_validos:
                logging.error(f"Nenhum parâmetro válido encontrado para: {dados_mongodb['uid']}")
                return False

            # Busca informações da estação
            estacao_info = await self.get_estacao_info(dados_mongodb["uid"])
            if not estacao_info:
                logging.error(f"Estação não encontrada: {dados_mongodb['uid']}")
                return False

            async with httpx.AsyncClient() as client:
                success_count = 0
                total_count = 0
                
                # Para cada sensor da estação
                for sensor in estacao_info["sensores"]:
                    param_response = await client.get(f"{self.base_url}/parametros/{sensor['id']}")
                    if param_response.status_code != 200:
                        logging.warning(f"Erro ao buscar parâmetro {sensor['id']}: Status {param_response.status_code}")
                        continue

                    param_info = param_response.json()
                    param_nome = param_info["nome"]

                    if param_nome in parametros_validos:
                        total_count += 1
                        try:
                            medida = {
                                "estacao_id": estacao_info["id"],
                                "parametro_id": sensor["id"],
                                "valor": parametros_validos[param_nome],
                                "data_hora": datetime.fromtimestamp(dados_mongodb.get("unixtime", dados_mongodb.get("unix_time", int(time.time())))).isoformat()
                            }

                            response = await client.post(f"{self.base_url}/medidas", json=medida)
                            if response.status_code in [200, 201]:
                                success_count += 1
                                logging.info(f"Medida para {param_nome} enviada com sucesso")
                            else:
                                logging.error(f"Erro ao enviar medida para {param_nome}: Status {response.status_code}")
                        except Exception as e:
                            logging.error(f"Erro ao processar medida para {param_nome}: {str(e)}")

                # Atualiza status no MongoDB apenas se houver algum sucesso
                if success_count > 0:
                    try:
                        self.mongodb_service.collection.update_one(
                            {"_id": dados_mongodb["_id"]},
                            {"$set": {
                                "processado": True,
                                "sucesso_total": success_count,
                                "total_parametros": total_count,
                                "data_processamento": datetime.now().isoformat()
                            }}
                        )
                        logging.info(f"Processamento finalizado - Sucesso: {success_count}/{total_count} parâmetros")
                        return True
                    except Exception as e:
                        logging.error(f"Erro ao atualizar status no MongoDB: {str(e)}")
                        return False

                logging.warning(f"Nenhuma medida processada com sucesso de {total_count} tentativas")
                return False

        except Exception as e:
            logging.error(f"Erro ao processar e enviar dados: {str(e)}")
            return False