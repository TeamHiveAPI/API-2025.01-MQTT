import httpx
from fastapi import HTTPException
import logging

class ExternalAPIService:
    def __init__(self):
        self.api_base_url = "http://localhost:8000"
        self.headers = {
            'Authorization': ''
        }

    async def verify_station_exists(self, uid: str) -> bool:
        try:
            async with httpx.AsyncClient() as client:
                url = f"{self.api_base_url}/estacoes/{uid}"
                response = await client.get(url, headers=self.headers)
                
                if response.status_code == 200:
                    logging.info(f"Estação {uid} verificada com sucesso")
                    return True
                elif response.status_code == 404:
                    logging.info(f"Estação {uid} não encontrada")
                    return False
                else:
                    logging.error(f"Erro na API externa: {response.status_code}")
                    return False
                    
        except Exception as e:
            logging.error(f"Erro de conexão com API externa: {str(e)}")
            return False