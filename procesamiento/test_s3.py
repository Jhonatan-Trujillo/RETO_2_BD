from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestS3_AmazonAppliances_Docker") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "30000") \
    .config("spark.hadoop.fs.s3a.connection.timeout", "200000") \
    .getOrCreate()

# Reducir logs para ver mejor los resultados
spark.sparkContext.setLogLevel("WARN")

print("¡Spark iniciado en Docker!")

# 2. Rutas en S3
ruta_s3_crudo = "s3a://accesorios-appliances/accesorios/Appliances.jsonl" 
ruta_s3_salida = "s3a://accesorios-appliances/prueba-spark/test_amazon.parquet"

print(f"Leyendo archivo desde S3: {ruta_s3_crudo}...")

try:
    # 3. Leer el JSONL
    df = spark.read.json(ruta_s3_crudo)
    
    print("Archivo leído correctamente. Tomando una muestra de 100 registros...")
    
    # 4. Tomar muestra (forzamos la ejecución)
    df_muestra = df.limit(100)
    conteo = df_muestra.count()
    print(f"¡ÉXITO! Se leyeron {conteo} registros de muestra.")
    
    print("Esquema del archivo:")
    df_muestra.printSchema()

    # 5. Escribir en S3
    print(f"Escribiendo datos de prueba en S3: {ruta_s3_salida}...")
    df_muestra.write.mode("overwrite").parquet(ruta_s3_salida)
    
    print("¡PROCESO COMPLETADO! Revisa tu bucket de S3.")

except Exception as e:
    print("\n❌ OCURRIÓ UN ERROR:")
    print(e)