from services.external_services import ExternalAPIService
from services.mongodb_service import MongoDBService
import logging
import paho.mqtt.client as mqtt
import json
from models.estacao import DadosEstacao

class MQTTService:
    def __init__(self):
        # Serviços e conexões
        self.external_api = ExternalAPIService()
        self.mongodb_service = MongoDBService()
        
        # Configurações MQTT
        self.mqtt_client = mqtt.Client()
        self.mqtt_client.on_message = self.on_message
        self.mqtt_client.on_connect = self.on_connect
        
    def start(self):
        try:
            # Conecta ao broker MQTT (localhost na porta padrão 1883)
            self.mqtt_client.connect("localhost", 1883, 60)
            # Inicia o loop em segundo plano
            self.mqtt_client.loop_start()
            logging.info("Serviço MQTT iniciado com sucesso")
        except Exception as e:
            logging.error(f"Erro ao conectar ao broker MQTT: {str(e)}")
            raise
        
    def on_connect(self, client, userdata, flags, rc):
        logging.info(f"Conectado ao broker MQTT com código: {rc}")
        client.subscribe("api-fatec/estacao/dados/")
        
    def on_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload.decode('utf-8'))
            logging.info("Mensagem MQTT recebida")
            dados = DadosEstacao(**payload)
            self.process_message(dados)
        except Exception as e:
            logging.error(f"Erro ao processar mensagem MQTT: {str(e)}")
            
    async def process_message(self, dados: DadosEstacao):
        try:
            estacao_existe = await self.external_api.verify_station_exists(dados.uid)
            
            if estacao_existe:
                dados_dict = dados.dict()
                dados_dict["processado"] = False
                # Usa o MongoDBService para inserir com retry
                if self.mongodb_service.retry_mongodb_insert(dados_dict):
                    logging.info(f"Dados da estação {dados.uid} salvos com sucesso")
                    return True
                else:
                    logging.error(f"Falha ao salvar dados da estação {dados.uid}")
                    return False
            else:
                logging.warning(f"Estação {dados.uid} não encontrada")
                return False
                
        except Exception as e:
            logging.error(f"Erro ao processar mensagem: {str(e)}")
            raise Exception(f"Falha ao processar mensagem: {str(e)}")