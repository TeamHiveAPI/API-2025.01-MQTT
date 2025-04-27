import asyncio
import logging
import time
import httpx
from datetime import datetime
from .mongodb_service import MongoDBService
 
class DataProcessorService:
    def __init__(self):
        self.mongodb_service = MongoDBService()
        self.running = False
        self.base_url = "http://localhost:8000"
        self.parametros = {}
        self.estacoes = {}
 
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
   
 
    async def get_parametros(self):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/parametros/")
                if response.status_code == 200:
                    # Cria um mapeamento de json para id do parâmetro
                    parametros = response.json()
                    self.parametros = {
                        param.get("json", self.generate_json_key(param["nome"])): param["id"]
                        for param in parametros
                    }
                    logging.info(f"Mapeamento de parâmetros atualizado: {self.parametros}")
        except Exception as e:
            logging.error(f"Erro ao buscar parâmetros: {str(e)}")
 
    def generate_json_key(self, param_name: str):
        # Remove espaços e caracteres especiais, converte para minúsculas
        key = param_name.lower()
        key = ''.join(c for c in key if c.isalnum() or c == '_')
        return key
 
    async def process_and_send_data(self, dados):
        try:
            await self.get_parametros()
           
            # Busca informações da estação para obter o ID numérico
            estacao_info = await self.get_estacao_info(dados["uid"])
            if not estacao_info:
                print(f"Estação não encontrada: {dados['uid']}")
                return False
 
            medidas = []
            timestamp = dados["unixtime"]
            estacao_id = estacao_info["id"]  # Usando o ID numérico da estação
 
            # Para cada campo nos dados (exceto uid, unixtime e processado)
            for campo, valor in dados.items():
                if campo not in ["uid", "unixtime", "processado", "_id"]:
                    if campo in self.parametros:
                        medida = {
                            "estacao_id": estacao_id,  # ID numérico ao invés do UUID
                            "parametro_id": self.parametros[campo],
                            "valor": valor,
                            "data_hora": datetime.fromtimestamp(timestamp).isoformat()
                        }
                        medidas.append(medida)
                        print(f"Medida preparada: {medida}")
                    else:
                        print(f"Campo {campo} não encontrado no mapeamento: {self.parametros}")
 
            # Envia as medidas para a API
            if medidas:
                success_count = 0
                async with httpx.AsyncClient() as client:
                    for medida in medidas:
                        try:
                            print(f"Tentando enviar medida: {medida}")
                            response = await client.post(f"{self.base_url}/medidas/", json=medida)
                            if response.status_code == 201:
                                success_count += 1
                                print(f"Medida enviada com sucesso: {medida}")
                            else:
                                print(f"Erro ao enviar medida: {response.status_code} - {response.text}")
                        except Exception as e:
                            print(f"Exceção ao enviar medida: {str(e)}")
 
                if success_count > 0:
                    self.mongodb_service.collection.update_one(
                        {"_id": dados["_id"]},
                        {"$set": {"processado": True}}
                    )
                    return True
                return False
 
        except Exception as e:
            print(f"Erro geral no processamento: {str(e)}")
            return False
 
 
 
    async def process_loop(self):
        while self.running:
            try:
                # Busca dados não processados
                dados_nao_processados = list(self.mongodb_service.collection.find({"processado": {"$ne": True}}))
                logging.info(f"Encontrados {len(dados_nao_processados)} registros não processados")
               
                for dados in dados_nao_processados:
                    success = await self.process_and_send_data(dados)
                    if success:
                        print(f"Sucesso: {dados['uid']}")
                        logging.info(f"Dados processados com sucesso: {dados['uid']}")
                    else:
                        print(f"Falha: {dados['uid']}")
                        logging.warning(f"Falha ao processar dados: {dados['uid']}")
               
                await asyncio.sleep(31)
            except Exception as e:
                logging.error(f"Erro no loop de processamento: {str(e)}")
                await asyncio.sleep(31)
 
    def start(self):
        self.running = True
        asyncio.create_task(self.process_loop())
        logging.info("Processador de dados iniciado")
 
    def stop(self):
        self.running = False
        logging.info("Processador de dados parado")
 