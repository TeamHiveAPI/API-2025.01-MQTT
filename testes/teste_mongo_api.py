import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.mongodb_service import MongoDBService
from services.parametro_service import ParametroService
import asyncio
import httpx
import time
from datetime import datetime 

BASE_URL = "http://localhost:8000"

async def test_mongo_to_api():
    print("\n=== Testing MongoDB to API Flow ===")
    
    mongodb_service = MongoDBService()
    parametro_service = ParametroService()
    
    # Get unprocessed data from MongoDB
    unprocessed_data = mongodb_service.collection.find({"processado": {"$ne": True}})
    
    # Parameter mapping
    param_mapping = {
        "wind": "vento",
        "temp": "temperatura"
    }
    
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for data in unprocessed_data:
            print(f"\nProcessing data: {data}")
            
            # Map parameters to API format
            params = {}
            for mongo_param, api_param in param_mapping.items():
                if mongo_param in data:
                    params[api_param] = data[mongo_param]
            
            if not params:
                print("No valid parameters found")
                continue
                
            # Get station info
            response = await client.get(f"{BASE_URL}/estacoes/uid/{data['uid']}")
            if response.status_code != 200:
                print(f"Station not found: {data['uid']}")
                continue
                
            station = response.json()
            print(f"\nInformações da Estação:")
            print(f"ID: {station['id']}")
            print(f"Nome: {station['nome']}")
            print(f"Sensores: {station['sensores']}")
            
            # Process parameters and send to API
            for param_name, value in params.items():
                print(f"\nProcessando parâmetro: {param_name}")
                print(f"Valor: {value}")
                
                param_id = next((s["id"] for s in station["sensores"] if s["nome"].lower() == param_name.lower()), None)
                print(f"ID do parâmetro encontrado: {param_id}")
                
                # Convert timestamp to ISO format
                timestamp = data.get("unixtime", data.get("unix_time", int(time.time())))
                data_hora = datetime.fromtimestamp(timestamp).isoformat()
                
                medida = {
                    "estacao_id": station["id"],
                    "parametro_id": param_id,
                    "valor": value,
                    "data_hora": data_hora
                }
                
                print(f"\nMedida a ser enviada: {medida}")
                
                if medida["parametro_id"]:
                    try:
                        response = await client.post(f"{BASE_URL}/medidas", json=medida)
                        print(f"Status code: {response.status_code}")
                        print(f"Resposta: {await response.text()}")
                    except Exception as e:
                        print(f"Erro ao enviar medida: {str(e)}")
                else:
                    print(f"ERRO: Não encontrou ID para o parâmetro {param_name}")
                    print(f"Tentando encontrar entre: {[s['nome'] for s in station['sensores']]}")

            # Update instead of insert
            mongodb_service.collection.update_one(
                {"_id": data["_id"]},
                {"$set": {"processado": True}}
            )

if __name__ == "__main__":
    asyncio.run(test_mongo_to_api())