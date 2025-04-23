from pymongo import MongoClient
from datetime import datetime
import logging
import time

class MongoDBService:
    def __init__(self):
        self.client = MongoClient("mongodb+srv://tmadm:apifatec2025@api-estacao.xikvdt1.mongodb.net/?retryWrites=true&w=majority")
        self.db = self.client["api_estacao"]
        self.collection = self.db["dados_estacao"]
        self.failed_messages = self.db["failed_messages"]

    def retry_mongodb_insert(self, data, max_retries=3, delay=1):
        for attempt in range(max_retries):
            try:
                self.collection.insert_one(data)
                logging.info(f"Dados salvos no MongoDB com sucesso após {attempt + 1} tentativa(s)")
                return True
            except Exception as e:
                logging.error(f"Tentativa {attempt + 1} falhou: {str(e)}")
                time.sleep(delay)
        
        self.failed_messages.insert_one({
            "data": data,
            "timestamp": datetime.utcnow(),
            "error_count": max_retries
        })
        logging.error("Todas as tentativas falharam. Mensagem salva em failed_messages")
        return False

    def get_failed_messages(self):
        return list(self.failed_messages.find({}, {'_id': 0}))