import httpx
import logging
from typing import Dict, List, Optional

class ParametroService:
    def __init__(self):
        self.base_url = "http://localhost:8000" 
        
    async def get_estacao_parametros(self, uid: str) -> Dict[str, str]:
        """Retorna um dicionário com o mapeamento entre json e id do parâmetro"""
        try:
            async with httpx.AsyncClient() as client:
                # Busca a estação pelo UID
                response = await client.get(f"{self.base_url}/estacoes/uid/{uid}")
                if response.status_code != 200:
                    logging.error(f"Erro ao buscar estação: {response.status_code}")
                    return {}

                estacao = response.json()
                parametros_map = {}

                # Para cada sensor da estação
                for sensor in estacao.get("sensores", []):
                    # Busca os detalhes do parâmetro
                    param_response = await client.get(f"{self.base_url}/parametros/{sensor['id']}")
                    if param_response.status_code == 200:
                        param_details = param_response.json()
                        # Mapeia o campo 'json' com o nome do parâmetro
                        if param_details.get("json"):
                            parametros_map[param_details["json"]] = param_details["nome"]

                return parametros_map

        except Exception as e:
            logging.error(f"Erro ao processar parâmetros: {str(e)}")
            return {}

    async def validar_parametros_mongodb(self, dados_mongodb: dict) -> Dict[str, float]:
        """Valida e formata os parâmetros do MongoDB"""
        try:
            uid = dados_mongodb.get("uid")
            if not uid:
                return {}

            # Obtém o mapeamento de parâmetros da estação
            parametros_validos = await self.get_estacao_parametros(uid)
            
            # Filtra apenas os campos que são parâmetros válidos
            parametros_formatados = {}
            for campo, valor in dados_mongodb.items():
                if campo in parametros_validos:
                    parametros_formatados[parametros_validos[campo]] = valor

            return parametros_formatados

        except Exception as e:
            logging.error(f"Erro ao validar parâmetros: {str(e)}")
            return {}