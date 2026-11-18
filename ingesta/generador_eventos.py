import json
import os
import boto3
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')

# RUTA EXACTA CONFIRMADA EN S3
S3_KEY = "accesorios/Appliances.jsonl" 
RUTA_GENERADA = "datos/eventos_simulados.csv"
LIMITE_REGISTROS = 600000

def generar_eventos():
    print(f"Conectando a AWS S3 (Bucket: {S3_BUCKET})...")
    
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    try:
        print(f"Solicitando archivo {S3_KEY}...")
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        body = response['Body']
    except Exception as e:
        print(f"Error conectando a S3: {e}")
        return

    print("Procesando datos y creando archivo de eventos. Espera un momento...")
    
    eventos_procesados = 0
    
    with open(RUTA_GENERADA, 'w', encoding='utf-8') as f_out:
        # CAMBIO CLAVE: Encabezados con parent_asin y rating
        f_out.write("user_id,parent_asin,rating,timestamp\n")
        
        for line in body.iter_lines():
            if not line:
                continue
            try:
                data = json.loads(line.decode('utf-8'))
                
                user_id = data.get("reviewerID") or data.get("user_id")
                parent_asin = data.get("asin") or data.get("parent_asin")
                timestamp_ms = data.get("unixReviewTime") or data.get("timestamp")
                rating = data.get("overall") or data.get("rating")
                
                if not all([user_id, parent_asin, timestamp_ms, rating]):
                    continue 
                
                # CAMBIO CLAVE: Escribimos parent_asin y rating
                f_out.write(f"{user_id},{parent_asin},{rating},{timestamp_ms}\n")
                eventos_procesados += 1
                
                if eventos_procesados % 50000 == 0:
                    print(f"Procesados {eventos_procesados} registros...")
                
                if eventos_procesados >= LIMITE_REGISTROS:
                    break
                    
            except Exception as e:
                continue 

    print(f"¡Éxito! Archivo generado en: {RUTA_GENERADA} con {eventos_procesados} registros.")

if __name__ == "__main__":
    generar_eventos()