from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("TestS3_AmazonAppliances") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.EnvironmentVariableCredentialsProvider") \
    .config("spark.hadoop.fs.s3a.threads.keepalivetime", "60000") \
    .getOrCreate()

print("¡Spark iniciado!")

# Ruta exacta de tu JSONL en S3
ruta_s3_crudo = "s3a://accesorios-appliances/accesorios/Appliances.jsonl" 

print(f"Leyendo archivo desde S3: {ruta_s3_crudo}...")
df = spark.read.json(ruta_s3_crudo)

# Tomamos solo 100 registros para que la prueba sea rápida
df_muestra = df.limit(100)
print(f"Se leyeron {df_muestra.count()} registros de muestra.")
df_muestra.printSchema()

# Ruta de salida para probar la escritura
ruta_s3_salida = "s3a://accesorios-appliances/prueba-spark/test_amazon.parquet"

print(f"Escribiendo datos en S3: {ruta_s3_salida}...")
df_muestra.write.mode("overwrite").parquet(ruta_s3_salida)

print("¡ÉXITO! Lectura y escritura en S3 funcionando correctamente.")