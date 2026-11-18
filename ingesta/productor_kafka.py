import json
import pandas as pd
from kafka import KafkaProducer
from dotenv import load_dotenv
import os
import time

load_dotenv()

KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'eventos_navegacion')
RUTA_EVENTOS = "datos/eventos_simulados.csv"

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def main():
    print(f"Conectando a Kafka en {KAFKA_BOOTSTRAP}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BOOTSTRAP],
            value_serializer=json_serializer
        )
    except Exception as e:
        print(f"Error conectando a Kafka: {e}")
        return

    print("Leyendo eventos generados...")
    df = pd.read_csv(RUTA_EVENTOS)
    
    print(f"Enviando {len(df)} eventos al topic '{KAFKA_TOPIC}'...")
    
    for index, row in df.iterrows():
        # CAMBIO CLAVE: Estructura JSON con parent_asin y rating
        evento = {
            "user_id": str(row['user_id']),
            "parent_asin": str(row['parent_asin']),
            "rating": float(row['rating']),
            "timestamp": str(row['timestamp'])
        }
        
        producer.send(KAFKA_TOPIC, value=evento)
        
        if index % 10000 == 0 and index > 0:
            print(f"Enviados {index} eventos...")
            producer.flush() 
            
    producer.flush()
    print("¡Todos los eventos han sido enviados a Kafka exitosamente!")

if __name__ == "__main__":
    main()