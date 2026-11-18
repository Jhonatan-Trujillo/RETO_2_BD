import json
import time
import random
from kafka import KafkaProducer
from pyspark.sql import SparkSession

# 1. Leer datos desde S3 para tener un catálogo de eventos reales
print("Conectando a S3 para obtener datos...")
spark = SparkSession.builder \
    .appName("GeneradorEventos") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")
ruta_limpia = "s3a://accesorios-appliances/datos-limpios/"
df = spark.read.parquet(ruta_limpia)

# Tomamos 1000 registros aleatorios para simular el tráfico
datos = df.select("user_id", "parent_asin", "rating").limit(1000).collect()
spark.stop()

# 2. Configurar el productor de Kafka
# Nota: Usamos localhost:9092 porque este script se ejecutará en tu máquina (Windows)
# Si lo corres dentro de Docker, cambia a 'kafka:9092'
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("¡Productor iniciado! Enviando eventos a Kafka (Presiona Ctrl+C para detener)...")

# 3. Bucle infinito enviando eventos
try:
    while True:
        for row in datos:
            evento = {
                "user_id": row["user_id"],
                "parent_asin": row["parent_asin"],
                "rating": float(row["rating"]),
                "timestamp": str(int(time.time()))
            }
            
            # Enviar al topic
            producer.send('eventos_navegacion', evento)
            
            # Pequeña pausa para simular tiempo real (ej. 10 eventos por segundo)
            time.sleep(0.1) 
            
except KeyboardInterrupt:
    print("\nDeteniendo productor...")
finally:
    producer.close()