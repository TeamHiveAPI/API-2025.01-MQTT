import asyncio
import random
from datetime import datetime
import time
import httpx
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

    async def gerador_dados_falso(self):
        while self.running:
            await self.get_estacao_ativa()
            await self.get_parametros()
            
            for estacao in self.estacoes:
                dados = {
                    "uid": estacao["uid"],
                    "unixtime": int(time.time()),
                    "processado": False
                }
                
                # Gera dados apenas para os parâmetros existentes
                for param in self.parametros:
                    if param["json"]:
                        dados[param["json"]] = round(random.uniform(0, 50), 2)
                
                self.mongodb_service.retry_mongodb_insert(dados)
                print(f"Dados simulados gerados para estação {estacao['nome']}: {dados}")
            
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