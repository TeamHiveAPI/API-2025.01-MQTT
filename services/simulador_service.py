import asyncio
import random
from datetime import datetime
import time
import httpx
import logging
from .mongodb_service import MongoDBService
from .data_processor_service import DataProcessorService
 
class SimulatorService:
    def __init__(self):
        self.mongodb_service = MongoDBService()
        self.data_processor = DataProcessorService()
        self.running = False
        self.base_url = "http://localhost:8000"
        self.estacoes = []
        self.parametros = []
        # Mapeamento de tipos de parâmetros com seus ranges
        self.param_ranges = {
            'temperatura': (-10, 40),
            'umidade': (0, 100),
            'pressao': (900, 1100),
            'vento': (0, 100),
            'chuva': (0, 150),
            'radiacao': (0, 1000),
            'default': (0, 50)  # Range padrão para outros tipos
        }
 
    async def get_estacao_ativa(self):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/estacoes/")
                if response.status_code == 200:
                    self.estacoes = [estacao for estacao in response.json() if estacao["status"] == "ativa"]
                    print(f"Estações ativas encontradas: {len(self.estacoes)}")
                    return True
        except Exception as e:
            print(f"Erro ao buscar estações: {str(e)}")
            return False
 
    async def get_parametros(self):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{self.base_url}/parametros/")
                if response.status_code == 200:
                    self.parametros = response.json()
                    print(f"Parâmetros encontrados: {len(self.parametros)}")
                    return True
        except Exception as e:
            print(f"Erro ao buscar parâmetros: {str(e)}")
            return False
 
    def get_param_range(self, param_name: str):
        # Procura por palavras-chave no nome do parâmetro
        nome_lower = param_name.lower()
        for key in self.param_ranges.keys():
            if key in nome_lower:
                return self.param_ranges[key]
        return self.param_ranges['default']
 
    def generate_json_key(self, param_name: str):
        # Remove espaços e caracteres especiais, converte para minúsculas
        key = param_name.lower()
        key = ''.join(c for c in key if c.isalnum() or c == '_')
        return key
 
    async def gerador_dados_falso(self):
        while self.running:
            try:
                await self.get_estacao_ativa()
                await self.get_parametros()
               
                for estacao in self.estacoes:
                    dados = {
                        "uid": estacao["uid"],
                        "unixtime": int(time.time()),
                        "processado": False
                    }
                   
                    # Gera dados para os parâmetros
                    for param in self.parametros:
                        # Se não tiver json definido, gera automaticamente
                        json_key = param.get("json") or self.generate_json_key(param["nome"])
                        min_val, max_val = self.get_param_range(param["nome"])
                        dados[json_key] = round(random.uniform(min_val, max_val), 2)
                   
                    if len(dados) > 3:  # Mais que uid, unixtime e processado
                        self.mongodb_service.retry_mongodb_insert(dados)
                        logging.info(f"Dados simulados gerados para estação {estacao['nome']}: {dados}")
                    else:
                        logging.warning(f"Nenhum dado gerado para estação {estacao['nome']}")
               
                await asyncio.sleep(30)
            except Exception as e:
                logging.error(f"Erro no gerador de dados: {str(e)}")
                await asyncio.sleep(30)
   
    def start(self):
        self.running = True
        self.data_processor.start()  # Inicia o processador de dados
        asyncio.create_task(self.gerador_dados_falso())
        print("Simulador e processador de dados iniciados")
   
    def stop(self):
        self.running = False
        self.data_processor.stop()  # Para o processador de dados
        print("Simulador e processador de dados parados")
 