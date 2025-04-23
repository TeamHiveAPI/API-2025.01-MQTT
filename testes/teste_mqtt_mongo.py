import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import time
import random
from services.mqtt_service import MQTTService
from services.mongodb_service import MongoDBService
import asyncio

def generate_test_data():
    return {
        "uid": "92e88871-c5bf-4d2c-bf0c-a3d6d3eaa525",
        "unixtime": int(time.time()),  # Changed to match existing data
        "wind": round(random.uniform(0, 100), 2),
        "temp": round(random.uniform(15, 35), 2)
    }

async def test_mqtt_to_mongo():
    print("\n=== Testing MQTT to MongoDB Flow ===")
    
    mongodb_service = MongoDBService()
    mqtt_service = MQTTService()
    
    # Generate and send test data
    test_data = generate_test_data()
    print(f"Test data: {test_data}")
    
    # Save to MongoDB
    if mongodb_service.retry_mongodb_insert(test_data):
        print("Data successfully saved to MongoDB")
        
        # Verify data in MongoDB
        saved_data = mongodb_service.collection.find_one({"uid": test_data["uid"]})
        if saved_data:
            print("Data verified in MongoDB:")
            print(saved_data)
            return True
    
    return False

if __name__ == "__main__":
    asyncio.run(test_mqtt_to_mongo())