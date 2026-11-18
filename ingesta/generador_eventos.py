import json
import os
import boto3
from dotenv import load_dotenv

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
AWS_REGION = os.getenv('AWS_REGION')
S3_BUCKET = os.getenv('S3_BUCKET')

S3_KEY = "accesorios/Appliances.jsonl" 
RUTA_GENERADA = "datos/eventos_simulados.csv"
LIMITE_REGISTROS = 600000
RUTA_PROGRESO = "datos/progreso.txt" # Archivo para guardar en qué línea se quedó

def generar_eventos():
    # 1. Verificar si el archivo ya existe y si tiene el tamaño esperado
    if os.path.exists(RUTA_GENERADA):
        with open(RUTA_GENERADA, 'r', encoding='utf-8') as f:
            lineas = sum(1 for _ in f)
        if lineas >= LIMITE_REGISTROS:
            print(f"✅ El archivo {RUTA_GENERADA} ya existe y tiene {lineas} registros. No es necesario volver a generarlo.")
            return

    print(f"Conectando a AWS S3 (Bucket: {S3_BUCKET})...")
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION
    )

    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=S3_KEY)
        body = response['Body']
    except Exception as e:
        print(f"Error conectando a S3: {e}")
        return

    print("Procesando datos y creando archivo de eventos...")
    
    # 2. Verificar si hay un progreso anterior para reanudar
    start_line = 0
    if os.path.exists(RUTA_PROGRESO):
        with open(RUTA_PROGRESO, 'r') as f:
            start_line = int(f.read().strip())
        print(f"Reanudando desde la línea {start_line}...")
        # Abrimos en modo 'a' (append) para no borrar lo que ya se hizo
        modo = 'a'
    else:
        # Abrimos en modo 'w' para escribir desde cero
        modo = 'w'
        start_line = 0

    with open(RUTA_GENERADA, modo, encoding='utf-8') as f_out:
        # Si es un archivo nuevo, escribimos el encabezado
        if modo == 'w':
            f_out.write("user_id,parent_asin,rating,timestamp\n")
        
        eventos_procesados = start_line
        
        for i, line in enumerate(body.iter_lines()):
            # Si estamos reanudando, saltamos las líneas que ya procesamos
            if i < start_line:
                continue
            
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
                
                f_out.write(f"{user_id},{parent_asin},{rating},{timestamp_ms}\n")
                eventos_procesados += 1
                
                # Cada 10,000 registros, guardamos el progreso y hacemos flush
                if eventos_procesados % 10000 == 0:
                    print(f"Procesados {eventos_procesados} registros...")
                    f_out.flush()
                    # Guardar el progreso
                    with open(RUTA_PROGRESO, 'w') as f_prog:
                        f_prog.write(str(eventos_procesados))
                
                if eventos_procesados >= LIMITE_REGISTROS:
                    break
                    
            except Exception as e:
                continue 

    # 3. Al finalizar, borramos el archivo de progreso porque ya terminó
    if os.path.exists(RUTA_PROGRESO):
        os.remove(RUTA_PROGRESO)
        
    print(f"¡Éxito! Archivo generado en: {RUTA_GENERADA} con {eventos_procesados} registros.")

if __name__ == "__main__":
    generar_eventos()