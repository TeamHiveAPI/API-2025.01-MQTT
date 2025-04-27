from fastapi import FastAPI
import logging
import threading
from routes.estacao_routes import router as estacao_router
from routes.external_routes import router as external_router
# from routes.sync_routes import router as sync_router
from services.mqtt_service import MQTTService
from services.data_processor_service import DataProcessorService
from services.simulador_service import SimulatorService
 
 
 
 
 
# Configuração do logging
logging.basicConfig(
    filename='api_logs.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    force=True
)
 
app = FastAPI(title="API Estação", version="1.0.0")
 
# Registra as rotas
app.include_router(estacao_router)
app.include_router(external_router)
# app.include_router(sync_router)
 
@app.on_event("startup")
async def startup_event():
    try:
        global simulator, data_processor, mqtt_service
       
        # Inicia o simulador
        simulator = SimulatorService()
        simulator.start()
        logging.info("Simulador iniciado com sucesso")
       
        # Inicia o processador de dados
        data_processor = DataProcessorService()
        data_processor.start()  
        logging.info("Processador de dados iniciado com sucesso")
       
       
        # Inicia o MQTT
        mqtt_service = MQTTService()
        mqtt_thread = threading.Thread(target=mqtt_service.start)
        mqtt_thread.daemon = True
        mqtt_thread.start()
        logging.info("Serviço MQTT iniciado com sucesso")
       
        logging.info("API iniciada com sucesso")
    except Exception as e:
        logging.error(f"Erro ao iniciar serviços: {str(e)}")
 